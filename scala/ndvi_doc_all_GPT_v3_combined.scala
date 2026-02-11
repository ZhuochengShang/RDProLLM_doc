import edu.ucr.cs.bdlab.beast._
import edu.ucr.cs.bdlab.raptor.GeoTiffWriter
import edu.ucr.cs.bdlab.beast.io.tiff.TiffConstants
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{ArrayType, FloatType, IntegerType}
import org.apache.hadoop.fs.{FileSystem, Path => HPath}
import java.net.URI
import java.nio.file.{Files, Paths}

object ComputeNDVI {
  def run(sc: SparkContext): Unit = {
    // 1) Read input/output paths from SparkConf with safe defaults
    val defaultRed = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLM_doc/data/landsat8/LA/B4/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B4.TIF"
    val defaultNir = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLM_doc/data/landsat8/LA/B5/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B5.TIF"
    val defaultOut = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLM_doc/output/ndvi_single.v3.tif"

    val redInRaw = sc.getConf.get("ndvi.redPath", defaultRed)
    val nirInRaw = sc.getConf.get("ndvi.nirPath", defaultNir)
    val outRaw   = sc.getConf.get("ndvi.output", defaultOut)

    // 2) Normalize paths per environment rules
    val redIn = normalizePath(redInRaw, sc)
    val nirIn = normalizePath(nirInRaw, sc)
    var outPath = normalizePath(outRaw, sc)

    // 3) Probe pixel types to select correct loaders and conversions
    val probeRed = sc.geoTiff(redIn)
    val probeNir = sc.geoTiff(nirIn)
    val redPT = probeRed.first.pixelType
    val nirPT = probeNir.first.pixelType

    // Ensure single-band numeric rasters only
    redPT match {
      case ArrayType(_, _) =>
        throw new IllegalArgumentException("Red input is multi-band; this NDVI operation expects single-band rasters")
      case _ => // ok
    }
    nirPT match {
      case ArrayType(_, _) =>
        throw new IllegalArgumentException("NIR input is multi-band; this NDVI operation expects single-band rasters")
      case _ => // ok
    }

    // Load as native types then convert to Float when needed to compute NDVI
    // We ensure both are RasterRDD[Float] before overlay
    val redFloat = redPT match {
      case IntegerType =>
        val r = sc.geoTiff[Int](redIn)
        r.mapPixels((v: Int) => v.toFloat)
      case FloatType =>
        sc.geoTiff[Float](redIn)
      case other =>
        throw new IllegalArgumentException(s"Unsupported red pixel type: $other")
    }

    val nirFloat = nirPT match {
      case IntegerType =>
        val r = sc.geoTiff[Int](nirIn)
        r.mapPixels((v: Int) => v.toFloat)
      case FloatType =>
        sc.geoTiff[Float](nirIn)
      case other =>
        throw new IllegalArgumentException(s"Unsupported NIR pixel type: $other")
    }

    // 4) Overlay aligned rasters and compute NDVI per pixel
    // overlay enforces alignment; it will fail if CRS/extent/resolution/layout mismatch
    val stacked: RasterRDD[Array[Float]] = redFloat.overlay(nirFloat)
    val ndvi: RasterRDD[Float] = stacked.mapPixels((ab: Array[Float]) => {
      val r: Float = ab(0)
      val n: Float = ab(1)
      val denom: Float = n + r
      if (denom == 0.0f) -9999.0f else (n - r) / denom
    })

    // 5) Prepare output path:
    // - If a file already exists at the target output path, append ".tif" to avoid overwrite
    // - Ensure parent directory exists
    outPath = adjustOutputPath(sc, outPath)
    ensureParentExists(sc, outPath)

    // 6) Write GeoTIFF with LZW compression in compatibility mode (single file), like the Python script
    ndvi.saveAsGeoTiff(
      outPath,
      Seq(
        GeoTiffWriter.Compression -> TiffConstants.COMPRESSION_LZW,
        GeoTiffWriter.WriteMode -> "compatibility"
      )
    )
  }

  // -------- Helper utilities (paths and FS) --------

  private def isLocal(sc: SparkContext): Boolean =
    sc.master != null && sc.master.toLowerCase.startsWith("local")

  private def hasScheme(p: String): Boolean = {
    try {
      val u = new URI(p)
      u.getScheme != null
    } catch {
      case _: Throwable => false
    }
  }

  private def looksLikeLocalPath(p: String): Boolean = {
    p != null && (p.startsWith("/") || p.matches("^[A-Za-z]:\\\\.*"))
  }

  private def normalizePath(p: String, sc: SparkContext): String = {
    if (p == null || p.trim.isEmpty) p
    else if (hasScheme(p)) p
    else if (isLocal(sc) && looksLikeLocalPath(p)) {
      Paths.get(p).toAbsolutePath.normalize.toUri.toString
    } else {
      // Leave scheme-less path unchanged in non-local mode (resolved by fs.defaultFS)
      p
    }
  }

  private def adjustOutputPath(sc: SparkContext, out: String): String = {
    val hPath = new HPath(out)
    val fs = hPath.getFileSystem(sc.hadoopConfiguration)
    if (fs.exists(hPath) && fs.isFile(hPath)) out + ".tif" else out
  }

  private def ensureParentExists(sc: SparkContext, out: String): Unit = {
    val uri = new URI(out)
    val scheme = uri.getScheme
    if (scheme != null && scheme.equalsIgnoreCase("file")) {
      val p = Paths.get(uri)
      val parent = p.getParent
      if (parent != null && !Files.exists(parent)) {
        Files.createDirectories(parent)
      }
    } else {
      val hPath = new HPath(out)
      val parent = hPath.getParent
      if (parent != null) {
        val fs = hPath.getFileSystem(sc.hadoopConfiguration)
        if (!fs.exists(parent)) fs.mkdirs(parent)
      }
    }
  }
}

// NOTES
// - RDPro APIs used:
//   geoTiff, mapPixels, overlay, saveAsGeoTiff
// - Unsupported operations and why:
//   - Explicit NoData handling (read/write) is unsupported because DOC CHUNKS provide no APIs to read band NoData values
//     or set NoData on output; we only guard against zero denominator and emit -9999.0f by convention.
//   - Per-band mask propagation from source rasters is not implemented for the same reason.
// - Assumptions:
//   - Input rasters are single-band and align spatially; overlay will fail fast if metadata mismatch exists.
//   - We load integer inputs and convert to Float before NDVI to preserve semantics.
//   - Path normalization logic:
//     * Local mode is detected by sc.master startsWith "local" (case-insensitive).
//     * If a path has a URI scheme, it is used as-is.
//     * If no scheme and running local and it looks like a local path, convert to absolute file:/// URI.
//     * If not local and no scheme, leave unchanged to resolve against cluster fs.defaultFS.
//   - Output uses GeoTIFF LZW compression and "compatibility" mode to mirror the Python single-file write.
//   - If an output file already exists at target path, ".tif" is appended to avoid overwriting, per requirement.

val _r = ComputeNDVI.run(sc)
println("__DONE__ object=ComputeNDVI")
System.exit(0)
