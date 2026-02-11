import org.apache.spark.SparkContext
import edu.ucr.cs.bdlab.beast._
import edu.ucr.cs.bdlab.raptor.GeoTiffWriter
import edu.ucr.cs.bdlab.beast.io.tiff.TiffConstants

import java.net.URI
import java.nio.file.{Paths => JPaths}
import org.apache.hadoop.fs.{FileSystem, Path => HPath}

object NDVIFromTwoBands {
  // Defaults derived from the original Python
  private val DefaultRedPath = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLM_doc/data/landsat8/LA/B4/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B4.TIF"
  private val DefaultNirPath = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLM_doc/data/landsat8/LA/B5/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B5.TIF"
  private val DefaultOutPath = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLM_doc/output/ndvi_single.all.tif"

  def run(sc: SparkContext): Unit = {
    // Read parameters from SparkConf or system properties, with safe defaults
    val redArg = sys.props.get("red").orElse(Option(sc.getConf.get("rdpro.ndvi.red", null))).getOrElse(DefaultRedPath)
    val nirArg = sys.props.get("nir").orElse(Option(sc.getConf.get("rdpro.ndvi.nir", null))).getOrElse(DefaultNirPath)
    val outArg = sys.props.get("out").orElse(Option(sc.getConf.get("rdpro.ndvi.out", null))).getOrElse(DefaultOutPath)

    val redPath = normalizePath(redArg, sc)
    val nirPath = normalizePath(nirArg, sc)
    val outBaseDir = ensureOutputDirectory(outArg, sc)

    // Load input rasters (assumed integer type for Landsat SR)
    val red: RasterRDD[Int] = sc.geoTiff[Int](redPath)
    val nir: RasterRDD[Int] = sc.geoTiff[Int](nirPath)

    // Validate metadata alignment (same grid, extent, CRS, and tile size)
    val redMeta = red.first.rasterMetadata
    val nirMeta = nir.first.rasterMetadata
    val aligned =
      redMeta.x1 == nirMeta.x1 &&
      redMeta.y1 == nirMeta.y1 &&
      redMeta.x2 == nirMeta.x2 &&
      redMeta.y2 == nirMeta.y2 &&
      redMeta.srid == nirMeta.srid &&
      redMeta.tileWidth == nirMeta.tileWidth &&
      redMeta.tileHeight == nirMeta.tileHeight
    if (!aligned) {
      throw new RuntimeException("B4 and B5 grids do not match — warp one band first")
    }

    // Stack the two rasters and compute NDVI = (NIR - RED) / (NIR + RED), -9999.0f for denom==0
    val stacked: RasterRDD[Array[Int]] = red.overlay(nir)

    val ndviFunc: Array[Int] => Float = (px: Array[Int]) => {
      val r = px(0).toFloat
      val n = px(1).toFloat
      val denom = n + r
      if (denom == 0.0f) -9999.0f else (n - r) / denom
    }
    val ndvi: RasterRDD[Float] = stacked.mapPixels(ndviFunc)

    // Save as GeoTIFF using distributed mode and LZW compression
    ndvi.saveAsGeoTiff(
      outBaseDir,
      Seq(
        GeoTiffWriter.Compression -> TiffConstants.COMPRESSION_LZW,
        GeoTiffWriter.WriteMode -> "distributed"
      )
    )

    println(s"NDVI written to directory: $outBaseDir")
  }

  // ---------------- Path utilities (mandatory normalization rules) ----------------

  private def isLocalMode(sc: SparkContext): Boolean =
    sc.master != null && sc.master.toLowerCase.startsWith("local")

  private def hasScheme(p: String): Boolean = {
    try {
      val u = new URI(p)
      u.getScheme != null
    } catch {
      case _: Throwable => false
    }
  }

  private def looksLikeLocalAbsolute(p: String): Boolean = {
    val win = p.matches("^[a-zA-Z]:\\\\.*")
    val unix = p.startsWith("/")
    win || unix
  }

  private def normalizePath(p: String, sc: SparkContext): String = {
    if (p == null) return p
    if (hasScheme(p)) p
    else if (isLocalMode(sc) && looksLikeLocalAbsolute(p)) {
      JPaths.get(p).toAbsolutePath.normalize.toUri.toString
    } else {
      // No scheme and not local mode: leave as-is to resolve via cluster FS (fs.defaultFS)
      p
    }
  }

  // Ensure output is treated as a directory; if a file exists at target, append ".tif"
  private def ensureOutputDirectory(outArg: String, sc: SparkContext): String = {
    val normalized = normalizePath(outArg, sc)
    val uri = new URI(normalized)
    val fs = FileSystem.get(uri, sc.hadoopConfiguration)

    def asPath(u: URI): HPath = new HPath(u)

    var outPath = asPath(uri)
    val exists = fs.exists(outPath)
    if (exists) {
      val status = fs.getFileStatus(outPath)
      if (status.isFile) {
        // Append ".tif" to avoid overwrite as per requirement
        val appended = normalized + ".tif"
        outPath = asPath(new URI(appended))
      }
    }

    // If the path exists and is still a file (e.g., appended also exists as file), keep appending suffixes
    var idx = 1
    while (fs.exists(outPath) && fs.getFileStatus(outPath).isFile) {
      val candidate = outPath.toString + s".$idx"
      outPath = asPath(new URI(candidate))
      idx += 1
    }

    if (!fs.exists(outPath)) {
      fs.mkdirs(outPath)
    } else if (fs.getFileStatus(outPath).isDirectory) {
      // already a directory; ok
    } else {
      // Final fallback: cannot create a directory here
      throw new RuntimeException(s"Unable to prepare output directory at $outPath")
    }

    outPath.toString
  }
}

// NOTES
// - RDPro APIs used:
//   sc.geoTiff[Int]
//   RasterRDD.overlay
//   RasterRDD.mapPixels
//   RasterRDD.saveAsGeoTiff
//
// - Unsupported operations and why:
//   • Reading band NoData values and masking them: The DOC does not provide an API to access or propagate per-band NoData through RasterRDD,
//     nor to set NoData metadata in GeoTIFF output. The implementation masks only denom==0 and sets -9999.0 as a value without tagging it as NoData.
//   • Auto-warp/reproject to align rasters: While RDPro provides reproject/reshape APIs, the Python checks strict alignment and errors out if mismatched.
//     Following that, this Scala version validates metadata equality and fails fast with a clear message if misaligned.
//
// - Assumptions:
//   • Inputs are single-band GeoTIFFs with integer pixel type (e.g., Landsat SR). They are read as RasterRDD[Int] and cast to Float during NDVI computation.
//   • Output written in distributed mode to a directory, with LZW compression. The provided output path is treated as a directory; if a file already exists
//     at the target path, the code appends ".tif" to avoid overwrite, then ensures the directory exists.
//   • Path normalization follows the specified rules: if Spark runs in local mode and the path has no scheme but looks like a local absolute path,
//     it is converted to file:///... URI; otherwise, scheme-less paths are left unchanged for cluster FS resolution.
//   • Parameters (red, nir, out) are obtained from system properties (red, nir, out) or SparkConf keys (rdpro.ndvi.red, rdpro.ndvi.nir, rdpro.ndvi.out),
//     with safe defaults matching the Python script.

val _r = NDVIFromTwoBands.run(sc)
println("__DONE__ object=NDVIFromTwoBands")
System.exit(0)
