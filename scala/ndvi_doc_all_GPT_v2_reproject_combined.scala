import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import edu.ucr.cs.bdlab.beast._
import edu.ucr.cs.bdlab.beast.cg.SpatialDataTypes.RasterRDD
import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.beast.geolite.{ITile, RasterFeature, RasterMetadata}
import edu.ucr.cs.bdlab.beast.io.tiff.TiffConstants
import edu.ucr.cs.bdlab.raptor.{GeoTiffWriter, RasterOperationsFocal, RasterOperationsLocal, RasterOperationsGlobal}
import edu.ucr.cs.bdlab.raptor.RaptorMixin._
import edu.ucr.cs.bdlab.raptor.RaptorMixin.RasterReadMixinFunctions

import java.net.URI
import java.nio.file.{Files, Paths, Path => JPath}
import org.apache.hadoop.fs.{FileSystem, Path => HPath}

object NDVITo4326 {
  def run(sc: SparkContext): Unit = {
    // Defaults from the provided Python script
    val defaultRed = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLM_doc/data/landsat8/LA/B4/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B4.TIF"
    val defaultNir = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLM_doc/data/landsat8/LA/B5/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B5.TIF"
    val defaultOut = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLM_doc/output/ndvi_single_4326_rasterio.v2.tif"

    // Allow overriding paths via SparkConf for flexibility
    val redRaw = sc.getConf.get("rdpro.ndvi.red", defaultRed)
    val nirRaw = sc.getConf.get("rdpro.ndvi.nir", defaultNir)
    val outRaw = sc.getConf.get("rdpro.ndvi.out", defaultOut)

    val redPath = normalizePath(redRaw, sc)
    val nirPath = normalizePath(nirRaw, sc)
    val outPath0 = normalizePath(outRaw, sc)
    val outPath = adjustOutputPath(outPath0, sc)

    val targetSRID = 4326
    val nodataOut: Float = -9999.0f

    // Load as Int (to match common Landsat SR band types), then cast to Float before any resampling
    val redInt: RasterRDD[Int] = sc.geoTiff[Int](redPath)
    val nirInt: RasterRDD[Int] = sc.geoTiff[Int](nirPath)

    val redF: RasterRDD[Float] = redInt.mapPixels((v: Int) => v.toFloat)
    val nirF: RasterRDD[Float] = nirInt.mapPixels((v: Int) => v.toFloat)

    // Reproject both to EPSG:4326
    val red4326: RasterRDD[Float] = redF.reproject(targetSRID)
    val nir4326: RasterRDD[Float] = nirF.reproject(targetSRID)

    // Align NIR to RED's exact grid using reshapeAverage with RED's metadata as target
    // Obtain target metadata from one tile (all tiles share the same RasterMetadata)
    val redMeta: RasterMetadata = red4326.first.rasterMetadata
    val nirAligned: RasterRDD[Float] = RasterOperationsFocal.reshapeAverage(nir4326, (_: RasterMetadata) => redMeta)

    // Stack RED and NIR and compute NDVI with guard for division by zero and non-finite inputs
    val stacked: RasterRDD[Array[Float]] = red4326.overlay(nirAligned)
    val ndvi: RasterRDD[Float] = stacked.mapPixels((px: Array[Float]) => {
      val r = px(0)
      val n = px(1)
      val denom = n + r
      val finite = java.lang.Float.isFinite(r) && java.lang.Float.isFinite(n)
      if (!finite || denom == 0.0f) nodataOut
      else (n - r) / denom
    })

    // Prepare output as a directory (create if missing). If a file exists at the path, we appended ".tif" above.
    ensureOutputDirectory(outPath, sc)

    // Write GeoTIFF with LZW compression; "compatibility" mode for single-file output behavior
    ndvi.saveAsGeoTiff(
      outPath,
      Seq(
        GeoTiffWriter.Compression -> TiffConstants.COMPRESSION_LZW,
        GeoTiffWriter.WriteMode -> "compatibility"
      )
    )

    println(s"NDVI (EPSG:$targetSRID) written to: $outPath")
  }

  // ------------------ Helpers (path normalization and output handling) ------------------

  private def isLocal(sc: SparkContext): Boolean =
    sc.master != null && sc.master.toLowerCase.startsWith("local")

  private def isWindowsDrivePath(p: String): Boolean =
    p.matches("^[a-zA-Z]:\\\\.*")

  private def hasScheme(p: String): Boolean = {
    try {
      val u = new URI(p)
      val s = u.getScheme
      s != null && !isWindowsDrivePath(p)
    } catch {
      case _: Throwable => false
    }
  }

  private def normalizePath(rawPath: String, sc: SparkContext): String = {
    require(rawPath != null && rawPath.nonEmpty, "Empty path provided")
    if (hasScheme(rawPath)) {
      rawPath
    } else if (isLocal(sc)) {
      // Treat as local FS path; convert to absolute file:// URI
      val abs: JPath = Paths.get(rawPath).toAbsolutePath.normalize()
      abs.toUri.toString
    } else {
      // No scheme and not local -> leave unchanged to resolve via fs.defaultFS
      rawPath
    }
  }

  private def adjustOutputPath(outNormalized: String, sc: SparkContext): String = {
    // If a file already exists at the target path, append ".tif" to avoid overwrite (per requirement)
    val hconf = sc.hadoopConfiguration
    val outUri = new URI(outNormalized)
    val fs = FileSystem.get(outUri, hconf)
    val hPath = new HPath(outNormalized)
    if (fs.exists(hPath) && fs.getFileStatus(hPath).isFile) {
      outNormalized + ".tif"
    } else {
      outNormalized
    }
  }

  private def ensureOutputDirectory(pathStr: String, sc: SparkContext): Unit = {
    // Treat the output as a directory; create if missing
    val hconf = sc.hadoopConfiguration
    val outUri = new URI(pathStr)
    val fs = FileSystem.get(outUri, hconf)
    val hPath = new HPath(pathStr)
    if (!fs.exists(hPath)) {
      fs.mkdirs(hPath)
    }
  }
}

// NOTES
// - RDPro APIs used:
//   geoTiff, mapPixels, reproject, reshapeAverage, overlay, saveAsGeoTiff
// - Unsupported operations and why:
//   - Precise control of resampling kernel during reproject (Python used bilinear). RDPro docs do not expose a resampling option for reproject.
//   - Explicit nodata propagation from source rasters and setting output nodata metadata are not documented in RDPro write options; NDVI sets invalid results to -9999.0f in pixel values but we cannot tag nodata in the GeoTIFF profile.
//   - Exact control over GeoTIFF tiling is not available in the documented API.
// - Assumptions:
//   - Input Landsat bands are single-band GeoTIFFs with integer pixel types; they are loaded as Int and cast to Float before any resampling to preserve numeric fidelity.
//   - Overlay requires exact alignment; we obtain the reprojected RED raster's RasterMetadata via first.rasterMetadata and warp NIR to match using reshapeAverage.
//   - Path normalization follows the required rules: detect Spark local mode via sc.master; add file:/// to scheme-less local paths; leave scheme-less paths unchanged in cluster mode.
//   - Output path is treated as a directory and created if missing. If a file already exists at the target path, we append ".tif" to the provided path to avoid overwrite (as required).

val _r = NDVITo4326.run(sc)
println("__DONE__ object=NDVITo4326")
System.exit(0)
