import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{ArrayType, FloatType, IntegerType}
import edu.ucr.cs.bdlab.beast._
import edu.ucr.cs.bdlab.beast.cg.SpatialDataTypes.RasterRDD
import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.beast.geolite.{ITile, RasterFeature, RasterMetadata}
import edu.ucr.cs.bdlab.beast.io.tiff.TiffConstants
import edu.ucr.cs.bdlab.raptor.{GeoTiffWriter, RasterOperationsFocal, RasterOperationsLocal, RasterOperationsGlobal}
import edu.ucr.cs.bdlab.raptor.RaptorMixin._
import edu.ucr.cs.bdlab.raptor.RaptorMixin.RasterReadMixinFunctions
import org.apache.hadoop.fs.{FileSystem, Path => HPath}

object NDVI4326 {
  def run(sc: SparkContext): Unit = {
    // Defaults from the Python script
    val defaultB4 = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLM_doc/data/landsat8/LA/B4/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B4.TIF"
    val defaultB5 = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLM_doc/data/landsat8/LA/B5/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B5.TIF"
    val defaultOut = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLM_doc/output/ndvi_single_4326_rasterio.v3.tif"

    // Allow overriding via SparkConf
    val b4Raw = sc.getConf.getOption("rdpro.ndvi.b4").getOrElse(defaultB4)
    val b5Raw = sc.getConf.getOption("rdpro.ndvi.nir").getOrElse(defaultB5)
    val outRaw = sc.getConf.getOption("rdpro.ndvi.out").getOrElse(defaultOut)

    // Normalize paths according to environment rules
    val b4Path = normalizePath(sc, b4Raw)
    val b5Path = normalizePath(sc, b5Raw)
    var outPath = normalizePath(sc, outRaw)

    // Prepare filesystem and ensure parent directory exists; if a file already exists at the target, append ".tif"
    val hadoopConf = sc.hadoopConfiguration
    val outURI = new java.net.URI(outPath)
    val fs = FileSystem.get(outURI, hadoopConf)
    val outHPath = new HPath(outPath)
    val parent = outHPath.getParent
    if (parent != null && !fs.exists(parent)) {
      fs.mkdirs(parent)
    }
    if (fs.exists(outHPath) && fs.isFile(outHPath)) {
      outPath = outPath + ".tif"
    }

    // Helper: load a GeoTIFF and convert to Float pixels
    def loadAsFloat(path: String): RasterRDD[Float] = {
      val probe = sc.geoTiff(path)
      probe.first.pixelType match {
        case IntegerType =>
          sc.geoTiff[Int](path).mapPixels((v: Int) => v.toFloat)
        case FloatType =>
          sc.geoTiff[Float](path)
        case ArrayType(IntegerType, _) =>
          throw new IllegalArgumentException("Unsupported pixel type: multi-band integer array. Provide single-band inputs.")
        case ArrayType(FloatType, _) =>
          throw new IllegalArgumentException("Unsupported pixel type: multi-band float array. Provide single-band inputs.")
        case other =>
          throw new IllegalArgumentException(s"Unsupported pixel type: $other")
      }
    }

    // Load inputs
    val red0: RasterRDD[Float] = loadAsFloat(b4Path)
    val nir0: RasterRDD[Float] = loadAsFloat(b5Path)

    // Reproject RED to EPSG:4326 to define target grid
    val red4326: RasterRDD[Float] = red0.reproject(4326).cache()
    // Extract target global metadata from RED after reprojection
    val targetMetadata: RasterMetadata = red4326.first.rasterMetadata

    // Reproject NIR to EPSG:4326 then reshape to exactly match RED's target grid
    val nir4326: RasterRDD[Float] = nir0.reproject(4326)
    val nirAligned: RasterRDD[Float] =
      RasterOperationsFocal.reshapeNN(nir4326, (_: RasterMetadata) => targetMetadata)

    // Basic validation
    assert(red4326.first.rasterMetadata.srid == 4326, "RED reproject failed: SRID != 4326")
    assert(nirAligned.first.rasterMetadata.srid == 4326, "NIR align/reproject failed: SRID != 4326")

    // Ensure exact alignment for overlay; will throw if mismatched
    val stacked: RasterRDD[Array[Float]] = red4326.overlay(nirAligned)

    // Compute NDVI with masking for non-finite and zero denominator; use -9999.0f as NODATA
    val NODATA_OUT = -9999.0f
    val ndvi: RasterRDD[Float] = stacked.mapPixels((v: Array[Float]) => {
      val r: Float = v(0)
      val n: Float = v(1)
      val rFinite = java.lang.Float.isFinite(r)
      val nFinite = java.lang.Float.isFinite(n)
      if (!rFinite || !nFinite) {
        NODATA_OUT
      } else {
        val denom: Float = n + r
        if (denom == 0.0f) NODATA_OUT else (n - r) / denom
      }
    })

    // Write single-file GeoTIFF with LZW compression (compatibility mode)
    ndvi.saveAsGeoTiff(
      outPath,
      Seq(
        GeoTiffWriter.Compression -> TiffConstants.COMPRESSION_LZW,
        GeoTiffWriter.WriteMode -> "compatibility"
      )
    )

    println(s"NDVI (EPSG:4326) written to: $outPath")
  }

  // ----------------- Helpers -----------------
  private def hasScheme(p: String): Boolean = {
    try {
      val u = new java.net.URI(p)
      u.getScheme != null && u.getScheme.nonEmpty
    } catch {
      case _: Throwable => false
    }
  }
  private def looksLikeLocalAbsolute(p: String): Boolean = {
    val isWindows = p.matches("^[a-zA-Z]:\\\\.*")
    val isUnix = p.startsWith("/")
    isWindows || isUnix
  }
  private def toFileURI(p: String): String = {
    java.nio.file.Paths.get(p).toAbsolutePath.normalize().toUri.toString
  }
  private def normalizePath(sc: SparkContext, rawPath: String): String = {
    if (rawPath == null) throw new IllegalArgumentException("Path is null")
    val isLocal = Option(sc.master).getOrElse("").toLowerCase.startsWith("local")
    if (hasScheme(rawPath)) rawPath
    else if (isLocal && looksLikeLocalAbsolute(rawPath)) toFileURI(rawPath)
    else rawPath
  }
}

// NOTES
// - RDPro APIs used:
//   sc.geoTiff[T], reproject, RasterOperationsFocal.reshapeNN, overlay, mapPixels, saveAsGeoTiff
// - Unsupported operations and why:
//   - Reading source nodata values and writing GDAL NODATA tag are not exposed in the provided DOC; implemented masking via non-finite checks and zero denominator only, and emitted -9999.0f values without setting a nodata tag.
//   - Exact control over resampling method (bilinear/nearest) during reproject is not available in DOC; used default reproject behavior and NN for reshape to match RED grid.
// - Assumptions:
//   - Inputs are single-band rasters; multi-band array pixel types are rejected with a clear error.
//   - Both inputs can be safely promoted to Float for NDVI computation.
//   - Path normalization:
//     * If sc.master starts with "local" and a scheme-less absolute local path is given, it is converted to file:/// URI.
//     * If not local and no scheme is present, the path is left unchanged to resolve against fs.defaultFS.
//   - Output handling:
//     * Parent directory is created if missing.
//     * If a file already exists at the target output path, ".tif" is appended to avoid overwrite.
//     * GeoTIFF is written with LZW compression and "compatibility" mode to produce a single file.

val _r = NDVI4326.run(sc)
println("__DONE__ object=NDVI4326")
System.exit(0)
