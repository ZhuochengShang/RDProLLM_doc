import org.apache.spark.SparkContext
import edu.ucr.cs.bdlab.beast._
import edu.ucr.cs.bdlab.raptor.GeoTiffWriter
import edu.ucr.cs.bdlab.beast.io.tiff.TiffConstants

object ComputeNDVI {
  def run(sc: SparkContext): Unit = {
    val conf = sc.getConf

    // Defaults from the original Python script
    val defaultRedPath = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/data/landsat8/LA/B4/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B4.TIF"
    val defaultNirPath = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/data/landsat8/LA/B5/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B5.TIF"
    val defaultOutNdvi = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/output/ndvi.tif"

    // Allow overriding via SparkConf (standard Spark API)
    val rawRedPath = conf.get("rdpro.input.red", defaultRedPath)
    val rawNirPath = conf.get("rdpro.input.nir", defaultNirPath)
    val rawOutPath = conf.get("rdpro.output.ndvi", defaultOutNdvi)

    // RDPro requires output as a directory; if a .tif path is provided, strip the extension
    val outDirRaw = stripTifExtension(rawOutPath)

    // Normalize paths per environment rules
    val redPath = normalizePath(sc, rawRedPath)
    val nirPath = normalizePath(sc, rawNirPath)
    val outDirPath = normalizePath(sc, outDirRaw)

    // Load rasters (assume Landsat SR as integer pixels)
    val red: RasterRDD[Int] = sc.geoTiff[Int](redPath)
    val nir: RasterRDD[Int] = sc.geoTiff[Int](nirPath)

    // Alignment check analogous to Python (fail fast if mismatched)
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

    // Stack NIR and RED, then compute NDVI per pixel
    val stacked: RasterRDD[Array[Int]] = nir.overlay(red)
    val ndvi: RasterRDD[Float] = stacked.mapPixels[Array[Int], Float]((px: Array[Int]) => {
      val n: Float = px(0).toFloat
      val r: Float = px(1).toFloat
      val denom: Float = n + r
      if (denom == 0.0f) -9999.0f else (n - r) / denom
    })

    // Write GeoTIFF with LZW compression. Output path must be a directory.
    ndvi.saveAsGeoTiff(outDirPath, GeoTiffWriter.Compression -> TiffConstants.COMPRESSION_LZW)
  }

  private def normalizePath(sc: SparkContext, path: String): String = {
    if (path == null || path.trim.isEmpty) return path
    val p = path.trim
    val hasScheme = try { val u = new java.net.URI(p); u.getScheme != null } catch { case _: Throwable => false }
    if (hasScheme) return p
    val isLocal = sc.master != null && sc.master.toLowerCase.startsWith("local")
    val looksLikeLocal =
      p.startsWith("/") ||
      p.matches("^[A-Za-z]:[/\\\\].*")
    if (isLocal && looksLikeLocal) {
      java.nio.file.Paths.get(p).toAbsolutePath.normalize().toUri.toString
    } else {
      // Leave scheme-less paths unchanged for cluster FS resolution (fs.defaultFS)
      p
    }
  }

  private def stripTifExtension(path: String): String = {
    if (path == null) return path
    val lower = path.toLowerCase
    if (lower.endsWith(".tif")) path.substring(0, path.length - 4)
    else if (lower.endsWith(".tiff")) path.substring(0, path.length - 5)
    else path
  }
}

/*
NOTES:
- RDPro APIs used:
  - sc.geoTiff
  - overlay
  - mapPixels
  - saveAsGeoTiff
  - RasterRDD and ITile (implicit via imports)

- Unsupported operations and why:
  - Reading per-band NoData values: No API is documented to fetch NoData from input rasters; NDVI masking by NoData is not implemented.
  - Setting NoData value on output: No documented writer option to set NoData; output will not carry a NoData tag even though -9999.0 is used as a fill value.
  - Reprojection/warping to align rasters: No API documented; code fails fast with a clear error if inputs are misaligned.

- Assumptions about IO paths / bands / nodata / CRS / environment detection logic:
  - Inputs are single-band integer GeoTIFFs (Landsat SR); they are loaded as Int and converted to Float during NDVI computation.
  - Output path must be a directory; if a .tif/.tiff file path is provided, the extension is stripped to form a directory name.
  - Path normalization:
    - If a scheme is present (file:, hdfs:, s3a:, gs:, http:, etc.), the path is used as-is.
    - If no scheme and Spark is in local mode (sc.master starts with "local") and the path looks like a local path (/ or C:\...), it is converted to an absolute file:/// URI.
    - If not local and no scheme, the path is left unchanged to resolve via fs.defaultFS.
  - Optional overrides for input/output paths are read from SparkConf keys:
    - rdpro.input.red, rdpro.input.nir, rdpro.output.ndvi
*/

val _r = ComputeNDVI.run(sc)
println("__DONE__")
System.exit(0)
