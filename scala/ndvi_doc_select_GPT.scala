import org.apache.spark.SparkContext
import edu.ucr.cs.bdlab.beast._
import edu.ucr.cs.bdlab.raptor.GeoTiffWriter
import edu.ucr.cs.bdlab.beast.io.tiff.TiffConstants
import java.net.URI
import java.nio.file.Paths

object NDVI {
  def run(sc: SparkContext): Unit = run(sc, Array.empty[String])

  // Optional overload to accept CLI-like args: <red_path> <nir_path> <out_path>
  def run(sc: SparkContext, args: Array[String]): Unit = {
    // Defaults from the provided Python script (fixed output extension to .tif)
    val defaultRed = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/data/landsat8/LA/B4/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B4.TIF"
    val defaultNir = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/data/landsat8/LA/B5/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B5.TIF"
    val defaultOut = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/python/ndvi.tif"

    val (redInRaw, nirInRaw, outRaw) = args.length match {
      case l if l >= 3 => (args(0), args(1), args(2))
      case 0           => (defaultRed, defaultNir, defaultOut)
      case _ =>
        throw new IllegalArgumentException("Expected 3 arguments: <red_band_path> <nir_band_path> <out_path>")
    }

    val redIn = normalizePath(redInRaw, sc)
    val nirIn = normalizePath(nirInRaw, sc)
    val outPath = normalizePath(outRaw, sc)

    // Read both bands as Int (typical for Landsat SR). We'll convert to Float for NDVI math.
    val red: RasterRDD[Int] = sc.geoTiff[Int](redIn)
    val nir: RasterRDD[Int] = sc.geoTiff[Int](nirIn)

    // Ensure rasters are aligned: overlay only works if metadata match.
    val stacked: RasterRDD[Array[Int]] =
      try {
        red.overlay(nir)
      } catch {
        case e: IllegalArgumentException =>
          throw new RuntimeException("B4 and B5 grids do not match — reshape/warp one band first to align metadata", e)
      }

    // Compute NDVI = (NIR - Red) / (NIR + Red), with denom==0 -> -9999.0f
    // Note: Source NoData masking cannot be replicated due to missing NoData APIs in provided docs.
    val ndvi: RasterRDD[Float] = stacked.mapPixels((px: Array[Int]) => {
      val r: Float = px(0).toFloat
      val n: Float = px(1).toFloat
      val denom: Float = n + r
      if (denom == 0.0f) -9999.0f else (n - r) / denom
    })

    // Write single-file GeoTIFF with LZW compression (compatibility mode to mimic GDAL single file)
    ndvi.saveAsGeoTiff(
      outPath,
      GeoTiffWriter.Compression -> TiffConstants.COMPRESSION_LZW,
      GeoTiffWriter.WriteMode -> "compatibility"
    )

    println(s"NDVI written to: $outPath")
  }

  // --------------------------
  // Path normalization helpers
  // --------------------------
  private def normalizePath(p: String, sc: SparkContext): String = {
    if (hasScheme(p)) {
      p
    } else {
      val isLocal = sc.master != null && sc.master.toLowerCase.startsWith("local")
      val looksAbsoluteLocal =
        p.startsWith("/") || p.matches("^[a-zA-Z]:[\\\\/].*")
      if (isLocal && looksAbsoluteLocal) {
        // Convert to absolute file:// URI so Hadoop doesn't treat it as HDFS
        Paths.get(p).toAbsolutePath.normalize().toUri.toString
      } else {
        // Leave scheme-less paths unchanged to resolve against cluster fs.defaultFS
        p
      }
    }
  }

  private def hasScheme(p: String): Boolean = {
    try {
      val u = new URI(p)
      u.getScheme != null
    } catch {
      case _: Throwable => false
    }
  }
}

// NOTES
// - RDPro APIs used: sc.geoTiff, overlay, mapPixels, saveAsGeoTiff
// - Unsupported operations and why:
//   - Reading per-band NoData values: No API in provided docs to access band NoData from input rasters, so masking based on source NoData cannot be implemented exactly as in Python.
//   - Setting output GeoTIFF NoData: No documented API to set NoData on write. The code uses -9999.0f for denom==0 but cannot tag it as NoData in the output file.
//   - Proactive metadata equality check: No documented metadata getters/comparators; we rely on overlay to enforce matching metadata and wrap any mismatch error with a clear message.
// - Assumptions:
//   - Input bands are stored as integer samples (e.g., Landsat SR Int16); they are read as Int and converted to Float for NDVI.
//   - Output is written as a single GeoTIFF file using compatibility mode to mimic GDAL’s single-file behavior.
//   - Filesystem handling:
//     - If sc.master starts with "local" and the path is an absolute local path without a URI scheme, it is converted to a file:/// URI.
//     - If a scheme is present, it is used as-is.
//     - If no scheme and Spark is not local, the path is left unchanged to resolve against the cluster filesystem (fs.defaultFS).

val _r = NDVI.run(sc)
println("__DONE__")
System.exit(0)
