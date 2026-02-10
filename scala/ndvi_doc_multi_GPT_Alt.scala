import org.apache.spark.SparkContext
import edu.ucr.cs.bdlab.beast._
import edu.ucr.cs.bdlab.raptor.GeoTiffWriter
import edu.ucr.cs.bdlab.beast.io.tiff.TiffConstants
import java.net.URI
import java.nio.file.Paths

object NDVI {
  def run(sc: SparkContext): Unit = {
    // Defaults derived from the original Python
    val defaultB4 = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/data/landsat8/LA/B4/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B4.TIF"
    val defaultB5 = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/data/landsat8/LA/B5/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B5.TIF"
    val defaultOut = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/output/ndvi.tif"

    // Normalize I/O paths according to Spark deployment mode and URI schemes
    val b4Path = normalizePath(sc, defaultB4)
    val b5Path = normalizePath(sc, defaultB5)
    val outPath = normalizePath(sc, defaultOut)

    // Load rasters as Float
    val red: RasterRDD[Float] = sc.geoTiff[Float](b4Path)
    val nir: RasterRDD[Float] = sc.geoTiff[Float](b5Path)

    // Stack bands (red first, then NIR); overlay requires matching CRS/resolution/tile size
    val stacked: RasterRDD[Array[Float]] = red.overlay(nir)

    // Compute NDVI with denominator protection; output sentinel -9999.0f on denom == 0
    val ndvi: RasterRDD[Float] = stacked.mapPixels((px: Array[Float]) => {
      val r: Float = px(0)
      val n: Float = px(1)
      val denom: Float = n + r
      if (denom == 0.0f) -9999.0f
      else (n - r) / denom
    })

    // Write output GeoTIFF (directory path). Use LZW compression; compatibility mode writes a single GeoTIFF.
    ndvi.saveAsGeoTiff(
      outPath,
      Seq(
        GeoTiffWriter.Compression -> TiffConstants.COMPRESSION_LZW,
        GeoTiffWriter.WriteMode -> "compatibility"
      )
    )
  }

  private def normalizePath(sc: SparkContext, rawPath: String): String = {
    def hasScheme(p: String): Boolean = {
      try {
        val u = new URI(p)
        u.getScheme != null
      } catch {
        case _: Throwable => false
      }
    }
    def isWindowsAbs(p: String): Boolean = p.matches("^[a-zA-Z]:\\\\.*")
    def isUnixAbs(p: String): Boolean = p.startsWith("/")
    val isLocal = Option(sc.master).exists(_.toLowerCase.startsWith("local"))

    if (hasScheme(rawPath)) {
      rawPath
    } else if (isLocal && (isUnixAbs(rawPath) || isWindowsAbs(rawPath))) {
      Paths.get(rawPath).toAbsolutePath.normalize().toUri.toString
    } else {
      rawPath
    }
  }
}

// NOTES:
// - RDPro APIs used: sc.geoTiff[T], overlay, mapPixels, saveAsGeoTiff
// - Unsupported operations and why:
//   * Explicit grid alignment check (GeoTransform/CRS comparison) is not exposed in provided APIs.
//     overlay requires matching CRS/resolution/tile size; if they differ, it should fail. No direct
//     pre-check API in given docs, so this code relies on overlay semantics.
//   * Reading input NoData values and masking accordingly: no API provided in the docs to read band
//     NoData or to set output NoData metadata. The code only guards against zero denominator and uses
//     a sentinel -9999.0f in pixel values; it does not set a NoData tag in the output dataset.
// - Assumptions:
//   * The first argument to overlay is band 0 (red), second is band 1 (NIR).
//   * Input rasters are aligned; otherwise overlay will error.
//   * Output path is treated as a directory (Spark semantics); "compatibility" mode writes a single
//     GeoTIFF inside that directory.
//   * Path normalization conforms to rules: preserve given scheme; in local mode, absolute local
//     paths without scheme are converted to file:/// URIs; in cluster mode, scheme-less paths are
//     left to resolve via cluster fs.defaultFS.

val _r = NDVI.run(sc)
println("__DONE__")
System.exit(0)
