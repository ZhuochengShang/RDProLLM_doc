import edu.ucr.cs.bdlab.beast._
import org.apache.spark.SparkContext
import org.apache.hadoop.fs.{FileSystem, Path}
import java.net.URI
import java.nio.file.Paths
import edu.ucr.cs.bdlab.raptor.GeoTiffWriter
import edu.ucr.cs.bdlab.beast.io.tiff.TiffConstants

object NDVISingle {
  def run(sc: SparkContext): Unit = {
    // Defaults from the Python script; can be overridden via system properties B4_PATH, B5_PATH, OUT_NDVI
    val defaultB4 = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLM_doc/data/landsat8/LA/B4/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B4.TIF"
    val defaultB5 = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLM_doc/data/landsat8/LA/B5/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B5.TIF"
    val defaultOut = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLM_doc/output/ndvi_single.v1.tif"

    val rawB4 = sys.props.getOrElse("B4_PATH", defaultB4)
    val rawB5 = sys.props.getOrElse("B5_PATH", defaultB5)
    val rawOut = sys.props.getOrElse("OUT_NDVI", defaultOut)

    val b4Path = normalizePath(rawB4, sc)
    val b5Path = normalizePath(rawB5, sc)
    val outPathStr0 = normalizePath(rawOut, sc)

    // Prepare output path (treat as directory), create if missing; if a file exists, append ".tif"
    val hConf = sc.hadoopConfiguration
    val outURI = new URI(outPathStr0)
    val fs = FileSystem.get(outURI, hConf)
    var outPath = new Path(outPathStr0)
    if (fs.exists(outPath) && fs.isFile(outPath)) {
      outPath = new Path(outPathStr0 + ".tif")
    }
    if (!fs.exists(outPath)) {
      fs.mkdirs(outPath)
    }

    // Load rasters (assume integer pixel type; RDPro promotes integer rasters to Int)
    val red: RasterRDD[Int] = sc.geoTiff[Int](b4Path)
    val nir: RasterRDD[Int] = sc.geoTiff[Int](b5Path)

    // Alignment check (CRS, extent, resolution, tile layout); fail fast if mismatched
    val mdRed = red.first().rasterMetadata
    val mdNir = nir.first().rasterMetadata
    if (mdRed != mdNir) {
      throw new RuntimeException("B4 and B5 grids do not match — warp one band first")
    }

    // Stack and compute NDVI; output Float, use -9999.0f when denom==0 (nodata fallback)
    val stacked: RasterRDD[Array[Int]] = red.overlay(nir)
    val ndvi: RasterRDD[Float] = stacked.mapPixels((px: Array[Int]) => {
      val r: Float = px(0).toFloat
      val n: Float = px(1).toFloat
      val denom: Float = n + r
      if (denom == 0.0f) -9999.0f else (n - r) / denom
    }: Float)

    // Write GeoTIFFs with LZW compression (distributed mode by default)
    ndvi.saveAsGeoTiff(outPath.toString, Seq(
      GeoTiffWriter.Compression -> TiffConstants.COMPRESSION_LZW
    ))
  }

  // -----------------------
  // Path normalization per rules
  // -----------------------
  private def normalizePath(path: String, sc: SparkContext): String = {
    val isLocal = Option(sc.master).map(_.toLowerCase.startsWith("local")).getOrElse(false)
    val hasScheme = {
      try {
        val u = new URI(path)
        u.getScheme != null
      } catch {
        case _: Throwable => false
      }
    }
    if (hasScheme) {
      path
    } else {
      if (isLocal && looksLikeAbsoluteLocal(path)) {
        val p = Paths.get(path).toAbsolutePath.normalize()
        p.toUri.toString // yields file:///...
      } else {
        // leave schemeless paths unchanged for cluster FS resolution (fs.defaultFS)
        path
      }
    }
  }

  private def looksLikeAbsoluteLocal(path: String): Boolean = {
    path.startsWith("/") || path.matches("^[a-zA-Z]:\\\\.*")
  }
}

// NOTES
// - RDPro APIs used:
//   // geoTiff, overlay, mapPixels, saveAsGeoTiff
// - Unsupported operations and why:
//   // Reading source-band NoData values and writing output NoData are not covered by the provided DOC; thus explicit NoData handling identical to GDAL is not implemented. The code only guards denom==0 and emits -9999.0f in that case.
//   // Advanced alignment/warping on mismatch is not auto-applied; if metadata differ, the code fails fast because reshape/reproject target metadata accessors are not provided in DOC to perform safe alignment here.
// - Assumptions:
//   // Input GeoTIFF pixel types are integer and loadable with sc.geoTiff[Int]; RDPro commonly promotes integer rasters to Int in memory.
//   // overlay preserves the order of inputs; red is index 0 and nir is index 1.
//   // RasterMetadata equality comparison (mdRed != mdNir) is sufficient to detect grid/CRS/tiling mismatch.
//   // Output path is treated as a directory for distributed writing. If a file exists at the provided path, ".tif" is appended to avoid overwrite, then the path is created as a directory.
//   // Path normalization follows the specified rules based on sc.master and URI scheme.

val _r = NDVISingle.run(sc)
println("__DONE__ object=NDVISingle")
System.exit(0)
