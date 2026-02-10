import edu.ucr.cs.bdlab.beast._
import edu.ucr.cs.bdlab.raptor.GeoTiffWriter
import edu.ucr.cs.bdlab.beast.io.tiff.TiffConstants
import org.apache.spark.SparkContext
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

  private def normalizePath(sc: SparkContext, p: String): String = {
    if (p == null) return p
    if (hasScheme(p)) {
      p
    } else {
      val isLocal = Option(sc.master).getOrElse("").toLowerCase.startsWith("local")
      if (isLocal) {
        try {
          Paths.get(p).toAbsolutePath.normalize().toUri.toString
        } catch {
          case _: Throwable => p
        }
      } else {
        // Leave scheme-less paths to resolve against cluster FS (e.g., fs.defaultFS)
        p
      }
    }
  }

  def run(sc: SparkContext): Unit = {
    // Safe defaults (from the provided Python); can be overridden by system properties
    val defaultRed = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/data/landsat8/LA/B4/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B4.TIF"
    val defaultNir = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/data/landsat8/LA/B5/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B5.TIF"
    val defaultOut = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/python/ndvi.tif"

    val redPathRaw = sys.props.getOrElse("ndvi.red", defaultRed)
    val nirPathRaw = sys.props.getOrElse("ndvi.nir", defaultNir)
    val outPathRaw = sys.props.getOrElse("ndvi.out", defaultOut)

    val redPath = normalizePath(sc, redPathRaw)
    val nirPath = normalizePath(sc, nirPathRaw)
    val outPath = normalizePath(sc, outPathRaw)

    if (!(outPath.toLowerCase.endsWith(".tif") || outPath.toLowerCase.endsWith(".tiff"))) {
      System.err.println(s"Warning: output path does not end with .tif/.tiff ($outPath). RDPro will still write a GeoTIFF.")
    }

    // Load input rasters. We assume integer pixels (typical for SR products) and convert to Float for NDVI math.
    val redInt = sc.geoTiff[Int](redPath)
    val nirInt = sc.geoTiff[Int](nirPath)

    // Alignment check: both rasters must have identical metadata to overlay
    val redMeta = redInt.first().rasterMetadata
    val nirMeta = nirInt.first().rasterMetadata
    if (redMeta != nirMeta) {
      throw new RuntimeException("B4 and B5 grids do not match — warp one band first (metadata differ)")
    }

    // Convert to Float for ratio math
    val redF = redInt.mapPixels((v: Int) => v.toFloat)
    val nirF = nirInt.mapPixels((v: Int) => v.toFloat)

    // Stack the two rasters [red, nir] and compute NDVI = (nir - red) / (nir + red)
    val stacked = redF.overlay(nirF) // RasterRDD[Array[Float]]
    val ndvi = stacked.mapPixels((arr: Array[Float]) => {
      val red = arr(0)
      val nir = arr(1)
      val denom = nir + red
      if (denom == 0.0f) -9999.0f else (nir - red) / denom
    })

    // Write as a single GeoTIFF with LZW compression (compatibility mode = one file)
    ndvi.saveAsGeoTiff(outPath,
      Seq(
        GeoTiffWriter.Compression -> TiffConstants.COMPRESSION_LZW,
        GeoTiffWriter.WriteMode -> "compatibility"
      )
    )

    println(s"NDVI written to: $outPath")
  }
}

// NOTES
// (a) RDPro APIs used:
// - sc.geoTiff[T](path)
// - mapPixels
// - overlay
// - saveAsGeoTiff

// (b) Unsupported operations and why:
// - Reading NoData from input bands: DOC CHUNKS do not expose APIs to get band-level NoData values, so masking based on input NoData could not be implemented. Current code only guards against denominator=0.
// - Setting NoData value on output GeoTIFF: DOC CHUNKS do not provide an option to set GDAL NoData tag during saveAsGeoTiff. The code writes -9999.0f into pixels but cannot set the NoData metadata/tag.
// - Explicit warp/reprojection to align rasters: Not required here because we fail fast if metadata differ. DOC CHUNKS provide reshape/reproject examples but not a simple “align to another raster’s metadata” helper; thus we require pre-aligned inputs.

// (c) Assumptions about IO paths / bands / nodata / CRS / environment detection logic:
// - Inputs are single-band GeoTIFFs with integer pixel type; we convert to Float for NDVI.
// - The two inputs are already aligned (identical RasterMetadata); otherwise we throw a clear error before processing.
// - The output is written as a single, compatibility-mode GeoTIFF with LZW compression.
// - Filesystem/path normalization follows: if sc.master starts with "local" and the path has no scheme, it is converted to an absolute file:/// URI; otherwise scheme-less paths are left to the cluster FS (fs.defaultFS).

val _r = ComputeNDVI.run(sc)
println("__DONE__")
System.exit(0)
