import org.apache.spark.SparkContext
import edu.ucr.cs.bdlab.beast._
import edu.ucr.cs.bdlab.raptor.GeoTiffWriter
import edu.ucr.cs.bdlab.beast.io.tiff.TiffConstants
import java.net.URI
import java.nio.file.Paths

object ComputeNDVI {
  private def hasScheme(p: String): Boolean = {
    try {
      val u = new URI(p)
      u.getScheme != null
    } catch {
      case _: Throwable => false
    }
  }

  private def normalizePath(sc: SparkContext, path: String): String = {
    if (path == null || path.isEmpty) path
    else if (hasScheme(path)) path
    else {
      val isLocal = sc.master != null && sc.master.toLowerCase.startsWith("local")
      if (isLocal) {
        val p = Paths.get(path)
        if (p.isAbsolute) p.toUri.toString else path
      } else {
        // Leave scheme-less paths to resolve via fs.defaultFS on the cluster
        path
      }
    }
  }

  def run(sc: SparkContext): Unit = {
    // Defaults derived from the original Python
    val defaultRedPath =
      "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/data/landsat8/LA/B4/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B4.TIF"
    val defaultNirPath =
      "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/data/landsat8/LA/B5/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B5.TIF"
    val defaultOutPath =
      "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/python/ndvi.tif"

    // Read paths from SparkConf if provided; otherwise use defaults
    val redPathRaw = sc.getConf.get("ndvi.red.path", defaultRedPath)
    val nirPathRaw = sc.getConf.get("ndvi.nir.path", defaultNirPath)
    val outPathRaw = sc.getConf.get("ndvi.out.path", defaultOutPath)

    val redPath = normalizePath(sc, redPathRaw)
    val nirPath = normalizePath(sc, nirPathRaw)
    val outPath = normalizePath(sc, outPathRaw)

    // Load rasters (assumed Int type for Landsat Surface Reflectance)
    val red: RasterRDD[Int] = sc.geoTiff[Int](redPath)
    val nir: RasterRDD[Int] = sc.geoTiff[Int](nirPath)

    // Alignment check similar to the Python script: ensure identical metadata
    val redMetaOpt = red.map(tile => tile.rasterMetadata).take(1).headOption
    val nirMetaOpt = nir.map(tile => tile.rasterMetadata).take(1).headOption

    if (redMetaOpt.isEmpty || nirMetaOpt.isEmpty) {
      throw new RuntimeException("Failed to open input files or inputs are empty")
    }
    if (!redMetaOpt.get.equals(nirMetaOpt.get)) {
      throw new RuntimeException("B4 and B5 grids do not match — warp one band first")
    }

    // Overlay to stack Red and NIR into a single raster with two bands per pixel
    val stacked: RasterRDD[Array[Int]] = red.overlay(nir)

    // NDVI calculation with NoData handling (-9999 for Landsat SR)
    val nodataInt: Int = -9999
    val nodataFloat: Float = -9999.0f

    val ndvi: RasterRDD[Float] = stacked.mapPixels((px: Array[Int]) => {
      val r: Float = px(0).toFloat
      val n: Float = px(1).toFloat
      val denom: Float = n + r
      val masked: Boolean = (px(0) == nodataInt) || (px(1) == nodataInt) || (denom == 0.0f)
      if (masked) nodataFloat else (n - r) / denom
    }: Float)

    // Write output GeoTIFF with LZW compression in compatibility mode (single file)
    ndvi.saveAsGeoTiff(
      outPath,
      GeoTiffWriter.Compression -> TiffConstants.COMPRESSION_LZW,
      GeoTiffWriter.WriteMode -> "compatibility"
    )
    println(s"NDVI written to: $outPath")
  }
}

// NOTES
// (a) RDPro APIs used:
// - sc.geoTiff[T]
// - overlay
// - mapPixels
// - saveAsGeoTiff

// (b) Unsupported operations and why:
// - Reading per-band NoData value from source rasters: Not exposed in the provided RDPro docs; assumed Landsat SR NoData = -9999.
// - Setting NoData metadata on the output GeoTIFF: No API in the provided docs to set band NoData; the nodata numeric value is embedded in the pixel values only.
// - Exact metadata equality checks beyond a sample: The docs do not expose a global metadata retrieval method; a small driver-side check is performed on the first tile to emulate the Python alignment check. If rasters are misaligned elsewhere, overlay will not be valid; the code fails fast based on the first-tile check similar to the Python intent.

// (c) Assumptions:
// - Input Landsat bands are stored as Int pixels; NDVI computed in Float.
// - NoData value for both inputs is -9999 (typical for Landsat SR).
// - Overlay preserves the order of inputs; i.e., Array(RED, NIR).
// - Output written as a single GeoTIFF using WriteMode "compatibility".
// - Filesystem/path normalization:
//   - If sc.master starts with "local" and a path is absolute without a scheme, it is converted to a file:/// URI.
//   - If sc.master is not local and a path has no scheme, it is left as-is to resolve via cluster fs.defaultFS.
//   - Paths with an existing URI scheme are used as-is.

val _r = ComputeNDVI.run(sc)
println("__DONE__")
System.exit(0)
