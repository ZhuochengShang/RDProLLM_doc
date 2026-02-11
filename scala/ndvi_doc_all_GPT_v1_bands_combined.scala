import edu.ucr.cs.bdlab.beast._
import edu.ucr.cs.bdlab.raptor.GeoTiffWriter
import edu.ucr.cs.bdlab.beast.io.tiff.TiffConstants
import org.apache.spark.SparkContext
import org.apache.hadoop.fs.{FileSystem, Path => HPath}
import java.net.URI
import java.nio.file.{Paths => JPaths}

object ComputeNDVI {
  def run(sc: SparkContext): Unit = {
    // Defaults from the original Python script
    val defaultInput = "/Users/clockorangezoe/Desktop/PlanetAPI/example/20230527_180231_61_247b_3B_AnalyticMS.tif"
    val defaultOutput = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLM_doc/output/ndvi_band.v1.tif"

    // Allow overriding via SparkConf (if provided by the caller)
    val inArg = sc.getConf.getOption("rdpro.input").getOrElse(defaultInput)
    val outArg = sc.getConf.getOption("rdpro.output").getOrElse(defaultOutput)
    val redBand = sc.getConf.getInt("rdpro.redBand", 3) // 1-based index, default 3
    val nirBand = sc.getConf.getInt("rdpro.nirBand", 4) // 1-based index, default 4

    // Normalize paths according to execution mode and scheme
    val inputPath = normalizePath(inArg, sc)
    val rawOutputPath = normalizePath(outArg, sc)

    // Prepare output path as a directory; avoid overwriting existing file
    val (finalOutPathStr, finalOutPath) = prepareOutputDirectory(rawOutputPath, sc)

    // Load multi-band raster as Array[Int] (typical for Planet UInt16 products)
    val mbRaster = sc.geoTiff[Array[Int]](inputPath)

    // Compute NDVI per pixel: (NIR - RED) / (NIR + RED)
    val redIdx = redBand - 1
    val nirIdx = nirBand - 1
    val ndvi = mbRaster.mapPixels((px: Array[Int]) => {
      // Guard against malformed pixel arrays
      if (px == null || px.length <= math.max(redIdx, nirIdx)) {
        -9999.0f
      } else {
        val red = px(redIdx).toFloat
        val nir = px(nirIdx).toFloat
        val denom = nir + red
        if (denom == 0.0f) -9999.0f else (nir - red) / denom
      }
    })

    // Persist output GeoTIFF with LZW compression in distributed mode
    ndvi.saveAsGeoTiff(
      finalOutPathStr,
      Seq(
        GeoTiffWriter.Compression -> TiffConstants.COMPRESSION_LZW,
        GeoTiffWriter.WriteMode -> "distributed"
      )
    )

    println(s"NDVI written to: $finalOutPathStr")
  }

  private def isLocal(sc: SparkContext): Boolean =
    Option(sc.master).exists(_.toLowerCase.startsWith("local"))

  private def hasScheme(p: String): Boolean = {
    try {
      val u = new URI(p)
      u.getScheme != null
    } catch {
      case _: Throwable => false
    }
  }

  private def looksLikeLocalPath(p: String): Boolean = {
    val winDrive = "^[A-Za-z]:[\\\\/].*".r
    p.startsWith("/") || winDrive.pattern.matcher(p).matches()
  }

  // Normalize IO paths per requirements
  private def normalizePath(path: String, sc: SparkContext): String = {
    if (path == null || path.trim.isEmpty)
      throw new IllegalArgumentException("Input/output path is empty")
    if (hasScheme(path)) {
      path
    } else {
      if (isLocal(sc) && looksLikeLocalPath(path)) {
        JPaths.get(path).toAbsolutePath.normalize().toUri.toString
      } else {
        // Leave scheme-less path unchanged in cluster mode (resolved via fs.defaultFS)
        path
      }
    }
  }

  private def prepareOutputDirectory(outputPath: String, sc: SparkContext): (String, HPath) = {
    val outURI = new URI(outputPath)
    val fs = FileSystem.get(outURI, sc.hadoopConfiguration)
    var outPath = new HPath(outURI)

    if (fs.exists(outPath) && fs.isFile(outPath)) {
      // Avoid overwriting an existing file; append .tif
      val suffixed = outputPath + ".tif"
      val suffixedURI = new URI(suffixed)
      outPath = new HPath(suffixedURI)
    }
    // Ensure the target directory exists (writer will populate it in distributed mode)
    fs.mkdirs(outPath)
    (outPath.toString, outPath)
  }
}

// NOTES
// - RDPro APIs used:
//   geoTiff[T]
//   mapPixels
//   saveAsGeoTiff
// - Unsupported operations and why:
//   - Per-band NoData detection and propagation are not supported by the provided DOC. The Python reads band NoData
//     values from GDAL and masks them. RDPro DOC does not expose NoData metadata or a way to set output NoData, so this
//     translation only guards against a zero denominator and uses -9999.0f as a fill value.
//   - Setting output NoData value in GeoTIFF metadata is not documented; not applied here.
// - Assumptions:
//   - Planet AnalyticMS file is multi-band UInt16, hence loaded as RasterRDD[Array[Int]].
//   - Bands are 1-based in the original script; defaults preserved (RED=3, NIR=4).
//   - Output is written in distributed mode to a directory path. If a file exists at the target path, ".tif" is appended.
//   - Path normalization follows the stated rules: add file:/// for absolute local paths in local mode; otherwise leave
//     scheme-less paths untouched to resolve via fs.defaultFS.

val _r = ComputeNDVI.run(sc)
println("__DONE__ object=ComputeNDVI")
System.exit(0)
