import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import java.net.URI
import java.nio.file.{Files, Paths, Path => JPath}

import edu.ucr.cs.bdlab.beast._
import edu.ucr.cs.bdlab.beast.cg.SpatialDataTypes.RasterRDD
import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.beast.geolite.{ITile, RasterFeature, RasterMetadata}
import edu.ucr.cs.bdlab.beast.io.tiff.TiffConstants
import edu.ucr.cs.bdlab.raptor.{GeoTiffWriter, RasterOperationsFocal, RasterOperationsLocal, RasterOperationsGlobal}
import edu.ucr.cs.bdlab.raptor.RaptorMixin._
import edu.ucr.cs.bdlab.raptor.RaptorMixin.RasterReadMixinFunctions

object NDVIReproject4326 {
  // Constants adapted from the Python script
  private val DefaultRedPath = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLM_doc/data/landsat8/LA/B4/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B4.TIF"
  private val DefaultNirPath = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLM_doc/data/landsat8/LA/B5/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B5.TIF"
  private val DefaultOutPath = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLM_doc/output/ndvi_single_4326_rasterio.v1.tif"
  private val TargetSRID = 4326
  private val NodataOut = -9999.0f

  def run(sc: SparkContext): Unit = {
    // Determine local vs cluster mode
    val isLocal = {
      val m = sc.master
      m != null && m.toLowerCase.startsWith("local")
    }

    // Read configuration (acts as "CLI args" equivalent)
    val conf = sc.getConf
    val redRaw = conf.get("rdpro.input.red", DefaultRedPath)
    val nirRaw = conf.get("rdpro.input.nir", DefaultNirPath)
    val outRaw = conf.get("rdpro.output.path", DefaultOutPath)

    // Normalize paths according to the mandatory rules
    val redPath = normalizePath(redRaw, isLocal)
    val nirPath = normalizePath(nirRaw, isLocal)
    val outPathNormalized = normalizePath(outRaw, isLocal)
    val finalOutPath = ensureOutputDirectory(outPathNormalized, isLocal)

    // Load both bands as Int (Landsat SR is commonly Int16/Int32); we will convert to Float for NDVI
    val redInt: RasterRDD[Int] = sc.geoTiff[Int](redPath)
    val nirInt: RasterRDD[Int] = sc.geoTiff[Int](nirPath)

    // Stack bands (expects identical metadata; Landsat bands are typically aligned)
    val stacked: RasterRDD[Array[Int]] = redInt.overlay(nirInt)

    // Compute NDVI locally per pixel: (NIR - RED) / (NIR + RED), with denom==0 -> NODATA
    val ndviFloat: RasterRDD[Float] = stacked.mapPixels((px: Array[Int]) => {
      val r: Float = px(0).toFloat
      val n: Float = px(1).toFloat
      val denom: Float = n + r
      if (!java.lang.Float.isFinite(r) || !java.lang.Float.isFinite(n) || denom == 0.0f) NodataOut
      else (n - r) / denom
    })

    // Reproject result to EPSG:4326
    val ndvi4326: RasterRDD[Float] = ndviFloat.reproject(TargetSRID)

    // Save with LZW compression
    ndvi4326.saveAsGeoTiff(finalOutPath,
      Seq(
        GeoTiffWriter.Compression -> TiffConstants.COMPRESSION_LZW
      )
    )

    println(s"NDVI (EPSG:$TargetSRID) written to: $finalOutPath")
  }

  // ===================== Helpers: Path handling (Mandatory Rules) =====================

  private def hasURIScheme(p: String): Boolean = {
    try {
      val u = new URI(p)
      u.getScheme != null
    } catch {
      case _: Throwable => false
    }
  }

  private def looksLikeLocalAbsolute(p: String): Boolean = {
    // Unix/macOS absolute or Windows drive absolute
    p.startsWith("/") || p.matches("^[A-Za-z]:\\\\.*")
  }

  private def normalizePath(raw: String, isLocal: Boolean): String = {
    if (hasURIScheme(raw)) {
      raw
    } else {
      if (isLocal && looksLikeLocalAbsolute(raw)) {
        Paths.get(raw).toAbsolutePath.normalize().toUri.toString
      } else {
        // No scheme: leave as-is to resolve against cluster fs.defaultFS
        raw
      }
    }
  }

  // Treat output as a directory; create if missing. If a file already exists at that path, append ".tif"
  private def ensureOutputDirectory(normalizedOut: String, isLocal: Boolean): String = {
    val uri = new URI(normalizedOut)
    val scheme = uri.getScheme
    val isFileScheme = scheme != null && scheme.equalsIgnoreCase("file")
    // For local mode and no scheme, normalizePath should have already added file:///
    if (isFileScheme || (isLocal && scheme == null)) {
      val jpath: JPath = if (isFileScheme) Paths.get(uri) else Paths.get(normalizedOut).toAbsolutePath.normalize()
      if (Files.exists(jpath) && Files.isRegularFile(jpath)) {
        val alt = Paths.get(jpath.toString + ".tif")
        val parent = alt.getParent
        if (parent != null && !Files.exists(parent)) Files.createDirectories(parent)
        alt.toUri.toString
      } else {
        // Ensure directory exists
        if (!Files.exists(jpath)) {
          Files.createDirectories(jpath)
        } else if (Files.isRegularFile(jpath)) {
          // Already handled above, but double-check
          val alt = Paths.get(jpath.toString + ".tif")
          val parent = alt.getParent
          if (parent != null && !Files.exists(parent)) Files.createDirectories(parent)
          return alt.toUri.toString
        }
        jpath.toUri.toString
      }
    } else {
      // Non-local or other schemes: cannot safely check/create here; return as-is
      normalizedOut
    }
  }
}

// NOTES
// - RDPro APIs used:
//   geoTiff, overlay, mapPixels, reproject, saveAsGeoTiff
// - Unsupported operations and why:
//   - Exact nodata handling per input band (reading src nodata, propagating to output) is not exposed in the provided DOC; we used a computed NODATA sentinel (-9999.0f) in mapPixels but could not set an output nodata metadata flag.
//   - Explicit resampling method (bilinear) during reprojection is not configurable per DOC; we relied on reproject(srid) default behavior.
//   - Precise grid alignment via extracting/setting RasterMetadata (to emulate Python's "reproject NIR to RED grid") is not available in DOC; we instead computed NDVI on the original aligned grid (after overlay) and reprojected the single-band result to EPSG:4326.
// - Assumptions:
//   - Landsat 8 B4 and B5 are spatially aligned in their native CRS/resolution, allowing overlay without prior warp.
//   - Input pixel type is integral (e.g., Int16); we load as Int and cast to Float inside mapPixels.
//   - Path normalization:
//     * If sc.master starts with "local", absolute OS paths without a scheme are converted to file:/// URIs.
//     * If not local and no scheme is present, the path is passed unchanged to resolve against fs.defaultFS.
//   - Output path is treated as a directory. On local file:// paths, we create the directory if missing. If a regular file already exists at that path, we append ".tif" to avoid overwrite.

val _r = NDVIReproject4326.run(sc)
println("__DONE__ object=NDVIReproject4326")
System.exit(0)
