import edu.ucr.cs.bdlab.beast._
import edu.ucr.cs.bdlab.raptor.GeoTiffWriter
import edu.ucr.cs.bdlab.beast.io.tiff.TiffConstants
import org.apache.spark.SparkContext
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.types.{ArrayType, FloatType, IntegerType}
import java.net.URI
import java.nio.file.Paths

object NDVI_Single {
  def run(sc: SparkContext): Unit = {
    // Defaults from the provided Python script
    val defaultRed = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLM_doc/data/landsat8/LA/B4/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B4.TIF"
    val defaultNir = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLM_doc/data/landsat8/LA/B5/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B5.TIF"
    val defaultOut = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLM_doc/output/ndvi_single.v2.tif"

    // Read "args" from SparkConf/System properties if provided; otherwise use defaults
    val argsStrOpt =
      sc.getConf.getOption("rdpro.args")
        .orElse(sys.props.get("rdpro.args"))
        .orElse(sys.env.get("RDPRO_ARGS"))

    val cliArgs: Array[String] = argsStrOpt.map(_.split("\\s+").filter(_.nonEmpty)).getOrElse(Array.empty[String])

    val redInRaw = if (cliArgs.length >= 1) cliArgs(0) else defaultRed
    val nirInRaw = if (cliArgs.length >= 2) cliArgs(1) else defaultNir
    val outRaw   = if (cliArgs.length >= 3) cliArgs(2) else defaultOut

    // Normalize paths based on Spark mode and presence of URI scheme
    val redIn = normalizePath(redInRaw, sc)
    val nirIn = normalizePath(nirInRaw, sc)
    var outPath = normalizePath(outRaw, sc)

    // Prepare output path (treat as directory; create if missing; handle file-exists case)
    outPath = prepareOutputPath(outPath, sc)

    // Load rasters and upcast to Float for NDVI computation
    val redF: RasterRDD[Float] = loadAsFloat(sc, redIn)
    val nirF: RasterRDD[Float] = loadAsFloat(sc, nirIn)

    // Overlay requires aligned rasters (same metadata). It will fail if not aligned.
    val stacked: RasterRDD[Array[Float]] = redF.overlay(nirF)

    // NDVI = (NIR - RED) / (NIR + RED); write -9999.0f when denominator is zero
    val ndvi: RasterRDD[Float] = stacked.mapPixels((px: Array[Float]) => {
      val red = px(0)
      val nir = px(1)
      val denom = nir + red
      if (denom == 0.0f) -9999.0f else (nir - red) / denom
    })

    // Save as GeoTIFF with LZW compression
    ndvi.saveAsGeoTiff(outPath, GeoTiffWriter.Compression -> TiffConstants.COMPRESSION_LZW)

    println(s"NDVI written to: $outPath")
  }

  // ---- Helpers ----

  private def loadAsFloat(sc: SparkContext, path: String): RasterRDD[Float] = {
    val probe = sc.geoTiff(path)
    probe.first.pixelType match {
      case IntegerType =>
        val r: RasterRDD[Int] = sc.geoTiff[Int](path)
        r.mapPixels((v: Int) => v.toFloat)
      case FloatType =>
        val r: RasterRDD[Float] = sc.geoTiff[Float](path)
        r
      case ArrayType(_, _) =>
        throw new RuntimeException("Multi-band GeoTIFF detected. This operation expects single-band rasters (Red and NIR).")
      case other =>
        throw new RuntimeException(s"Unsupported pixel type: $other")
    }
  }

  private def hasScheme(p: String): Boolean = {
    try { val u = new URI(p); u.getScheme != null } catch { case _: Throwable => false }
  }

  private def looksLikeLocalPath(p: String): Boolean = {
    val winDrive = "^[a-zA-Z]:[\\\\/].*".r
    p.startsWith("/") || winDrive.pattern.matcher(p).matches()
  }

  private def normalizePath(p: String, sc: SparkContext): String = {
    val isLocal = Option(sc.master).getOrElse("").toLowerCase.startsWith("local")
    if (p == null || p.isEmpty) p
    else if (hasScheme(p)) p
    else if (isLocal && looksLikeLocalPath(p)) {
      Paths.get(p).toAbsolutePath.normalize().toUri.toString
    } else {
      // No scheme and not local mode -> let Hadoop resolve against fs.defaultFS
      p
    }
  }

  private def prepareOutputPath(out: String, sc: SparkContext): String = {
    val conf = sc.hadoopConfiguration
    val p = new Path(out)
    val fs = p.getFileSystem(conf)
    if (fs.exists(p) && fs.isFile(p)) {
      val alt = out + ".tif"
      val p2 = new Path(alt)
      val fs2 = p2.getFileSystem(conf)
      val parent = p2.getParent
      if (parent != null && !fs2.exists(parent)) fs2.mkdirs(parent)
      alt
    } else {
      // Treat output as a directory; create if missing
      if (!fs.exists(p)) fs.mkdirs(p)
      out
    }
  }
}

// NOTES
// - RDPro APIs used:
//   geoTiff, mapPixels, overlay, saveAsGeoTiff
// - Unsupported operations and why:
//   - Reading NoData values from input bands and setting NoData on output: Not exposed in provided DOC. The code implements the denom==0 guard but cannot replicate GDAL NoData masking or set an output NoData tag.
//   - Explicit alignment checks/warping: DOC provides overlay for aligned rasters and reshape/reproject APIs, but no metadata field accessors were shown to implement a pre-check. overlay will fail if inputs are not aligned. If alignment is needed, a prior reshape/reproject step should be added using available APIs.
// - Assumptions:
//   - Inputs are single-band GeoTIFFs. If multi-band is detected, the code fails fast.
//   - Both inputs are upcast to Float via mapPixels to ensure consistent numeric type for NDVI.
//   - Output path is treated as a directory per instructions; it's created if missing. If a file exists at the given path, ".tif" is appended to avoid overwrite.
//   - Path normalization follows Spark master detection: absolute local paths are converted to file:/// URIs only in local mode; otherwise scheme-less paths are left unchanged to resolve via Hadoop fs.defaultFS.

val _r = NDVI_Single.run(sc)
println("__DONE__ object=NDVI_Single")
System.exit(0)
