import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.apache.hadoop.fs.{FileSystem, Path => HPath}
import java.net.URI
import java.nio.file.Paths

import edu.ucr.cs.bdlab.beast._
import edu.ucr.cs.bdlab.raptor.GeoTiffWriter
import edu.ucr.cs.bdlab.beast.io.tiff.TiffConstants
import edu.ucr.cs.bdlab.raptor.RasterOperationsFocal
import org.geotools.referencing.CRS

object NDVIToEPSG4326 {
  def run(sc: SparkContext): Unit = {
    // Defaults (can be overridden by SparkConf)
    val defaultRed = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLM_doc/data/landsat8/LA/B4/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B4.TIF"
    val defaultNir = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLM_doc/data/landsat8/LA/B5/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B5.TIF"
    val defaultOut = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLM_doc/output/ndvi_single_4326_rasterio.all.tif"

    val conf = sc.getConf
    val redPathRaw = conf.get("ndvi.red.path", defaultRed)
    val nirPathRaw = conf.get("ndvi.nir.path", defaultNir)
    val outPathRaw = conf.get("ndvi.out.path", defaultOut)

    val redPath = normalizePath(sc, redPathRaw)
    val nirPath = normalizePath(sc, nirPathRaw)
    var outPath = normalizePath(sc, outPathRaw)

    // Prepare filesystem and parent directory for output
    val hadoopConf = sc.hadoopConfiguration
    val fs = FileSystem.get(new URI(outPath), hadoopConf)
    val outHPath = new HPath(outPath)
    val parent = Option(outHPath.getParent).getOrElse(new HPath(new URI(outPath).resolve(".")))
    if (!fs.exists(parent)) {
      fs.mkdirs(parent)
    }
    if (fs.exists(outHPath) && fs.getFileStatus(outHPath).isFile) {
      outPath = outPath + ".tif"
    }

    // Load rasters (assume integer type for Landsat surface reflectance)
    val red = sc.geoTiff[Int](redPath)
    val nir = sc.geoTiff[Int](nirPath)

    // Validate pixel types based on available doc hints
    val redType = red.first.pixelType.toString
    val nirType = nir.first.pixelType.toString
    val allowedTypes = Set("IntegerType", "FloatType")
    if (!allowedTypes.contains(redType))
      throw new RuntimeException(s"Unsupported RED pixel type: $redType. Allowed: ${allowedTypes.mkString(", ")}")
    if (!allowedTypes.contains(nirType))
      throw new RuntimeException(s"Unsupported NIR pixel type: $nirType. Allowed: ${allowedTypes.mkString(", ")}")

    // Reproject both to EPSG:4326 using Average interpolation (closest available to bilinear in the doc)
    val targetCRS = CRS.decode("EPSG:4326")
    val red4326 = red.reproject(targetCRS, RasterOperationsFocal.InterpolationMethod.Average)
    val nir4326 = nir.reproject(targetCRS, RasterOperationsFocal.InterpolationMethod.Average)

    // Optional cache before overlay if data is large and re-used by multiple actions
    // red4326.persist(StorageLevel.MEMORY_AND_DISK)
    // nir4326.persist(StorageLevel.MEMORY_AND_DISK)

    // Overlay requires identical metadata (same grid); inputs are from same scene so they should match after reprojection
    val stacked = red4326.overlay(nir4326)

    // Compute NDVI: (NIR - RED) / (NIR + RED), emit -9999.0f when denominator == 0
    val NODATA_OUT = -9999.0f
    val ndvi = stacked.mapPixels((px: Array[Int]) => {
      val r = px(0).toFloat
      val n = px(1).toFloat
      val denom = n + r
      if (denom == 0.0f) NODATA_OUT else (n - r) / denom
    }: Float)

    // Write single GeoTIFF file with LZW compression in compatibility mode
    ndvi.saveAsGeoTiff(outPath,
      Seq(
        GeoTiffWriter.Compression -> TiffConstants.COMPRESSION_LZW,
        GeoTiffWriter.WriteMode -> "compatibility"
      )
    )
  }

  private def normalizePath(sc: SparkContext, pathStr: String): String = {
    if (pathStr == null || pathStr.isEmpty) return pathStr
    val hasScheme = try { new URI(pathStr).getScheme != null } catch { case _: Throwable => false }
    if (hasScheme) return pathStr

    val isLocal = Option(sc.master).exists(_.toLowerCase.startsWith("local"))
    val looksLocalAbs =
      pathStr.startsWith("/") ||
        pathStr.matches("^[A-Za-z]:\\\\.*") ||
        pathStr.matches("^\\\\\\\\.*")
    if (isLocal && looksLocalAbs) {
      Paths.get(pathStr).toAbsolutePath.normalize().toUri.toString
    } else {
      pathStr
    }
  }
}

// NOTES
// - RDPro APIs used:
//   - sc.geoTiff[T](path)
//   - RasterRDD.reproject(crs, RasterOperationsFocal.InterpolationMethod)
//   - RasterRDD.overlay(...)
//   - RasterRDD.mapPixels(f)
//   - RasterRDD.saveAsGeoTiff(path, options)
//   - ITile.pixelType (via raster.first.pixelType)
// - Unsupported operations and why:
//   - Exact bilinear resampling: DOC provides only Nearest Neighbor (default) and Average interpolation. Used Average as the closest available to bilinear.
//   - Explicit nodata handling/masking: DOC does not expose nodata metadata or masking APIs. Implemented only denominator==0 guard; cannot replicate rasterio-based nodata/NaN masking.
//   - Forcing alignment to an externally defined grid (transform/width/height): Only generic reproject/rescale/reshape are documented; no API to reproject one raster to another's exact grid except via identical metadata. We rely on identical source metadata for both bands; otherwise overlay will fail.
// - Assumptions about IO paths / bands / nodata / CRS / environment detection logic:
//   - Input Landsat bands are single-band GeoTIFFs with integer pixels (IntegerType).
//   - Both bands come from the same scene and share identical metadata, ensuring identical grids after reproject(4326) with the same interpolation.
//   - Output written as a single GeoTIFF (compatibility mode) with LZW compression.
//   - Path normalization honors URI schemes. In local Spark mode, absolute local paths are converted to file:/// URIs to avoid Hadoop misinterpreting them as HDFS. In cluster mode, scheme-less paths are left unchanged to resolve against fs.defaultFS.
//   - Output parent directory is created if missing. If a file already exists at the target output path, “.tif” is appended to avoid overwrite.

val _r = NDVIToEPSG4326.run(sc)
println("__DONE__ object=NDVIToEPSG4326")
System.exit(0)
