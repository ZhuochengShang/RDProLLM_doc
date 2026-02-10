import edu.ucr.cs.bdlab.beast._
import edu.ucr.cs.bdlab.raptor.RasterOperationsLocal
import org.apache.spark.SparkContext

import java.net.URI
import java.nio.file.{Paths, Path => JPath}

object ndvi {

  private def hasScheme(p: String): Boolean = {
    try {
      val u = new URI(p)
      u.getScheme != null
    } catch {
      case _: Exception => false
    }
  }

  // FILESYSTEM & PATH NORMALIZATION (MANDATORY)
  private def normalizePath(rawPath: String, sc: SparkContext): String = {
    if (rawPath == null || rawPath.trim.isEmpty) {
      throw new IllegalArgumentException("Path must be non-empty")
    }
    val isLocal: Boolean = sc.master != null && sc.master.toLowerCase.startsWith("local")
    if (hasScheme(rawPath)) {
      rawPath
    } else {
      // No scheme
      val jpath: JPath = Paths.get(rawPath)
      val looksLocalAbsolute: Boolean = {
        try {
          jpath.isAbsolute
        } catch {
          case _: Exception => false
        }
      }
      if (isLocal && looksLocalAbsolute) {
        jpath.toAbsolutePath.normalize().toUri.toString
      } else {
        // Leave as-is to resolve against cluster fs.defaultFS (when not local) or relative local path
        rawPath
      }
    }
  }

  def run(sc: SparkContext): RasterRDD[Float] = {
    // Inputs/outputs: allow overrides via system properties with safe defaults
    val defaultB4 =
      "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/data/landsat8/LA/B4/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B4.TIF"
    val defaultB5 =
      "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/data/landsat8/LA/B5/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B5.TIF"
    val defaultOut =
      "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/workspace/ndvi.tif"

    val b4PathRaw: String = sys.props.getOrElse("rdpro.ndvi.b4", defaultB4)
    val b5PathRaw: String = sys.props.getOrElse("rdpro.ndvi.b5", defaultB5)
    val outPathRaw: String = sys.props.getOrElse("rdpro.ndvi.out", defaultOut)

    val b4Path: String = normalizePath(b4PathRaw, sc)
    val b5Path: String = normalizePath(b5PathRaw, sc)
    val outPath: String = normalizePath(outPathRaw, sc)

    // Load Red (B4) and NIR (B5) as integer rasters
    val red: RasterRDD[Int] = sc.geoTiff[Int](b4Path)
    val nir: RasterRDD[Int] = sc.geoTiff[Int](b5Path)

    // Basic alignment check on key metadata fields (reshape not available in provided docs)
    val tRed = red.first()
    val tNir = nir.first()
    val mRed = tRed.rasterMetadata
    val mNir = tNir.rasterMetadata
    val sameMeta: Boolean =
      mRed.x1 == mNir.x1 &&
      mRed.y1 == mNir.y1 &&
      mRed.x2 == mNir.x2 &&
      mRed.y2 == mNir.y2 &&
      mRed.tileWidth == mNir.tileWidth &&
      mRed.tileHeight == mNir.tileHeight &&
      mRed.srid == mNir.srid
    if (!sameMeta) {
      throw new RuntimeException("Input rasters do not have identical metadata (extent/resolution/tiling/CRS). Please align them before NDVI (reshape not available in provided docs).")
    }

    // Stack the two rasters in the order: [Red, NIR]
    val stacked: RasterRDD[Array[Int]] = RasterOperationsLocal.overlay(red, nir)

    // NDVI = (NIR - RED) / (NIR + RED); if denom==0 => -9999.0f
    // Note: NoData masking cannot be implemented with provided APIs (see NOTES)
    val ndvi: RasterRDD[Float] = stacked.mapPixels((px: Array[Int]) => {
      val r: Float = px(0).toFloat
      val n: Float = px(1).toFloat
      val denom: Float = n + r
      if (denom == 0.0f) -9999.0f else (n - r) / denom
    })

    // Write output GeoTIFF (single-file compatibility mode if needed)
    // Compression options are available in docs, but not strictly required to produce valid output.
    ndvi.saveAsGeoTiff(outPath)

    // Force execution + print a sanity sample
    val t = ndvi.first
    val v = t.getPixelValue(t.x1, t.y1)
    println(s"[ndvi] sample(top-left)=$v partitions=${ndvi.getNumPartitions} out=$outPath")

    ndvi
  }
}

// NOTES
// (a) RDPro APIs used:
// - SparkContext.geoTiff
// - RasterOperationsLocal.overlay
// - RasterRDD.mapPixels
// - RasterRDD.saveAsGeoTiff
// - ITile.getPixelValue
// - ITile.rasterMetadata (field access: x1, y1, x2, y2, tileWidth, tileHeight, srid)

// (b) Unsupported operations and why:
// - Reading NoData values from source rasters: No API is shown in provided DOC CHUNKS or examples to access per-band NoData, so masking by NoData was not implemented. Only denom==0 is handled; invalid outputs are set to -9999.0f but not annotated as NoData in output.
// - Setting NoData metadata on output GeoTIFF: No documented API to set NoData in saveAsGeoTiff options.
// - Exact geotransform (affine) equality check: g2m is mentioned in RasterMetadata but no accessor equality/serialization API is shown; basic metadata fields are compared instead. If deeper alignment is required, the code throws an error asking for prior alignment.

// (c) Assumptions about IO paths / bands / nodata / CRS / environment detection logic:
// - Path normalization follows the required rules: if sc.master starts with "local" and the path has no scheme but is an absolute local path, it's converted to file:/// URI; otherwise paths are left as-is to resolve against cluster filesystem (fs.defaultFS).
// - Defaults for input B4/B5 and output paths are local absolute paths from the Python snippet; they can be overridden via system properties rdpro.ndvi.b4, rdpro.ndvi.b5, rdpro.ndvi.out.
// - Bands are single-band GeoTIFFs with integer pixel type; arithmetic is performed in Float.
// - NoData handling is not available with provided APIs; NDVI denominator zero is mapped to -9999.0f.
// - Write mode is default as per library; compression is not explicitly set to avoid relying on undocumented constants.

val _r = ndvi.run(sc)
println("__DONE__")
System.exit(0)
