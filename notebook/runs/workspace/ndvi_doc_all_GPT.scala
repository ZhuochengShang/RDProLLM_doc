import org.apache.spark.SparkContext
import edu.ucr.cs.bdlab.beast._
import edu.ucr.cs.bdlab.raptor.GeoTiffWriter
import edu.ucr.cs.bdlab.beast.io.tiff.TiffConstants
import java.net.URI
import java.nio.file.Paths

object ComputeNDVI {
  private def hasScheme(path: String): Boolean = {
    try {
      val s = new URI(path).getScheme
      s != null && s.nonEmpty
    } catch {
      case _: Throwable => false
    }
  }

  private def isLocalMaster(sc: SparkContext): Boolean = {
    val m = Option(sc.master).getOrElse("")
    m.toLowerCase.startsWith("local")
  }

  private def looksLikeLocalAbsolute(path: String): Boolean = {
    try {
      Paths.get(path).isAbsolute
    } catch {
      case _: Throwable => false
    }
  }

  private def normalizePath(sc: SparkContext, path: String): String = {
    if (hasScheme(path)) {
      path
    } else if (isLocalMaster(sc) && looksLikeLocalAbsolute(path)) {
      Paths.get(path).toAbsolutePath.normalize().toUri.toString
    } else {
      path
    }
  }

  def run(sc: SparkContext): Unit = {
    // Defaults (can be overridden by Spark conf keys: ndvi.red, ndvi.nir, ndvi.out)
    val defaultRed = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/data/landsat8/LA/B4/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B4.TIF"
    val defaultNir = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/data/landsat8/LA/B5/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B5.TIF"
    val defaultOut = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/python/ndvi.tif"

    val conf = sc.getConf
    val redInPathRaw = conf.get("ndvi.red", defaultRed)
    val nirInPathRaw = conf.get("ndvi.nir", defaultNir)
    val outPathRaw   = conf.get("ndvi.out", defaultOut)

    val redInPath = normalizePath(sc, redInPathRaw)
    val nirInPath = normalizePath(sc, nirInPathRaw)
    val outPath   = normalizePath(sc, outPathRaw)

    // Load as Int (common for Landsat SR); we'll cast to Float for math
    val redInt   = sc.geoTiff[Int](redInPath)
    val nirInt   = sc.geoTiff[Int](nirInPath)

    // Enforce alignment by attempting overlay; if metadata mismatches, overlay should fail
    val stacked =
      try {
        redInt.overlay(nirInt)
      } catch {
        case e: Throwable =>
          throw new RuntimeException("B4 and B5 grids do not match — warp one band first", e)
      }

    // NDVI = (NIR - RED) / (NIR + RED); handle denom==0 -> -9999.0f
    val ndvi = stacked.mapPixels((v: Array[Int]) => {
      val r: Float = v(0).toFloat
      val n: Float = v(1).toFloat
      val denom: Float = n + r
      if (denom == 0.0f) -9999.0f else (n - r) / denom
    })

    // Write GeoTIFF with LZW compression in compatibility mode (single file)
    ndvi.saveAsGeoTiff(outPath,
      GeoTiffWriter.Compression -> TiffConstants.COMPRESSION_LZW,
      GeoTiffWriter.WriteMode -> "compatibility"
    )

    println(s"NDVI written to: $outPath")
  }
}

NOTES
(a) RDPro APIs used:
- sc.geoTiff[Int]
- overlay
- mapPixels
- saveAsGeoTiff
- GeoTiffWriter.Compression
- GeoTiffWriter.WriteMode
- TiffConstants.COMPRESSION_LZW

(b) Unsupported operations and why:
- Reading source NoData values and propagating masks: The DOC does not expose APIs to access band NoData metadata or to mask pixels based on NoData, so this translation does not replicate the Python NoData masking. It only guards against division by zero (denominator equals zero).
- Setting NoData on output GeoTIFF: The DOC lists compression, write mode, bit compaction, and bits-per-sample, but not a way to set NoData in the output, so NoData is not set on the written file.
- Explicit alignment check (geotransform/projection comparison): The DOC does not provide direct accessors for geotransform/projection comparisons. Instead, we rely on overlay, which by documentation requires identical metadata and will fail otherwise. If it fails, we throw a clear runtime error mirroring the Python message.

(c) Assumptions about IO paths / bands / nodata / CRS / environment detection logic:
- Inputs are single-band rasters. They are read as Int and cast to Float during NDVI computation.
- If your inputs are Float GeoTIFFs, adjust sc.geoTiff type parameter accordingly.
- Output is saved as a single compatibility-mode GeoTIFF with LZW compression.
- Path normalization:
  - If path has a scheme (file:, hdfs:, s3a:, gs:, etc.), it is used as-is.
  - If Spark runs in local mode (sc.master starts with "local") and the path is an absolute local path, it is converted to a file:/// URI via java.nio.file.Paths.
  - If Spark is not local and the path has no scheme, it is left unchanged to resolve against the cluster filesystem (fs.defaultFS).
- Alignment: overlay enforces matching metadata; if rasters are not aligned, a RuntimeException is thrown advising to warp one band first.