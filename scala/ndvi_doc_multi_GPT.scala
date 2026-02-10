import edu.ucr.cs.bdlab.beast._
import org.apache.spark.SparkContext

import java.net.URI
import java.nio.file.Paths

object NDVI {
  // Normalize a single path per the required rules
  private def normalizePath(sc: SparkContext, path: String): String = {
    if (path == null || path.trim.isEmpty) return path
    val trimmed = path.trim
    val hasScheme = try {
      val u = new URI(trimmed)
      u.getScheme != null
    } catch {
      case _: Exception => false
    }
    if (hasScheme) {
      trimmed
    } else {
      val isLocal = sc.master != null && sc.master.toLowerCase.startsWith("local")
      val looksLikeLocalAbsolute =
        trimmed.startsWith("/") || trimmed.matches("^[a-zA-Z]:[\\\\/].*")
      if (isLocal && looksLikeLocalAbsolute) {
        Paths.get(trimmed).toAbsolutePath.toUri.toString
      } else {
        trimmed
      }
    }
  }

  def run(sc: SparkContext): Unit = {
    // Defaults derived from the provided Python (absolute paths keep behavior deterministic in local mode)
    val defaultB4 =
      "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/data/landsat8/LA/B4/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B4.TIF"
    val defaultB5 =
      "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/data/landsat8/LA/B5/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B5.TIF"
    val defaultOut =
      "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/python/ndvi.tif"

    // Read args from SparkConf (safe override), otherwise use defaults
    // You can pass them via --conf rdpro.b4=..., --conf rdpro.b5=..., --conf rdpro.out=...
    val b4In = sc.getConf.get("rdpro.b4", defaultB4)
    val b5In = sc.getConf.get("rdpro.b5", defaultB5)
    val outPath = sc.getConf.get("rdpro.out", defaultOut)

    // Mandatory path normalization before any IO
    val b4Path = normalizePath(sc, b4In)
    val b5Path = normalizePath(sc, b5In)
    val outUri = normalizePath(sc, outPath)

    // Load input rasters (assume integer pixels as typical for Landsat SR; will be cast to Float for math)
    val red: RasterRDD[Int] = sc.geoTiff[Int](b4Path)
    val nir: RasterRDD[Int] = sc.geoTiff[Int](b5Path)

    // Validate alignment/metadata equality (fail fast if mismatch)
    val redMD = red.first.rasterMetadata
    val nirMD = nir.first.rasterMetadata
    val aligned =
      redMD.x1 == nirMD.x1 &&
      redMD.y1 == nirMD.y1 &&
      redMD.x2 == nirMD.x2 &&
      redMD.y2 == nirMD.y2 &&
      redMD.tileWidth == nirMD.tileWidth &&
      redMD.tileHeight == nirMD.tileHeight &&
      redMD.srid == nirMD.srid &&
      redMD.g2m == nirMD.g2m

    if (!aligned) {
      throw new RuntimeException("B4 and B5 grids do not match — warp one band first")
    }

    // Stack bands and compute NDVI = (NIR - Red) / (NIR + Red)
    val stacked: RasterRDD[Array[Int]] = red.overlay(nir)
    val ndvi: RasterRDD[Float] = stacked.mapPixels((px: Array[Int]) => {
      val r: Float = px(0).toFloat
      val n: Float = px(1).toFloat
      val denom: Float = n + r
      if (denom == 0.0f) -9999.0f else (n - r) / denom
    })

    // Write GeoTIFF with LZW compression; use single-file compatibility mode (closest to GDAL Create)
    ndvi.saveAsGeoTiff(
      outUri,
      GeoTiffWriter.Compression -> TiffConstants.COMPRESSION_LZW,
      GeoTiffWriter.WriteMode -> "compatibility"
    )

    println(s"NDVI written to: $outUri")
  }
}

// NOTES
// (a) RDPro APIs used:
// - sc.geoTiff
// - RasterRDD (alias)
// - overlay
// - mapPixels
// - saveAsGeoTiff

// (b) Unsupported operations and why:
// - Reading NoData metadata and masking it: No API is documented to access band NoData values or carry masks, so nodata-based masking from the Python script could not be replicated. Only the denominator-zero guard is implemented.
// - Setting output NoData tag: No documented API to set NoData for the written GeoTIFF, so the output contains -9999.0 values but without a NoData tag.
// - Reprojection/warping/resampling: Not available in the provided docs. The code fails fast with a clear error if rasters are not aligned (same metadata).

// (c) Assumptions about IO paths / bands / nodata / CRS / environment detection logic:
// - Inputs are single-band GeoTIFFs with integer pixels (typical of Landsat SR). Pixels are cast to Float for NDVI computation.
// - Path normalization follows the mandatory rules: if sc.master starts with "local" and a path is an absolute local path without a scheme, it is converted to file:///...; otherwise, scheme-less paths are left unchanged to resolve via cluster fs.defaultFS.
// - SaveAsGeoTiff uses "compatibility" mode to write a single file (closest to the GDAL Create behavior in the Python).
// - CLI parameters are read from SparkConf keys rdpro.b4, rdpro.b5, and rdpro.out with safe defaults derived from the Python. Since run(sc: SparkContext) is mandated, SparkConf is used in place of direct argv parsing.

val _r = NDVI.run(sc)
println("__DONE__")
System.exit(0)
