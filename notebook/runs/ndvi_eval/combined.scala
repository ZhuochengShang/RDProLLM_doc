import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import edu.ucr.cs.bdlab.beast._
import java.net.URI
import java.nio.file.{Files, Paths, Path}

object ComputeNdviRDPro {
  def main(args: Array[String]): Unit = {
    val defaultB4 = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/data/landsat8/LA/B4/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B4.TIF"
    val defaultB5 = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/data/landsat8/LA/B5/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B5.TIF"
    val defaultOut = "ndvi.tif"

    val b4Path = if (args.length >= 1) args(0) else defaultB4
    val b5Path = if (args.length >= 2) args(1) else defaultB5
    val outPath = if (args.length >= 3) args(2) else defaultOut

    val spark = SparkSession.builder()
      .appName("ComputeNdviRDPro")
      .getOrCreate()

    try {
      val sc = spark.sparkContext

      // Determine output path locality by URI scheme; create parent directory for local paths
      val outURI = new URI(outPath)
      val schemeOpt = Option(outURI.getScheme)
      val defaultFS = sc.hadoopConfiguration.get("fs.defaultFS", "file")
      val isLocalOutput = schemeOpt.isEmpty || schemeOpt.contains("file") || (schemeOpt.contains("hdfs") && defaultFS.startsWith("file"))
      if (isLocalOutput) {
        val p: Path = if (schemeOpt.isDefined) Paths.get(outURI) else Paths.get(outPath)
        val parent = p.getParent
        if (parent != null) Files.createDirectories(parent)
      }

      // Load input rasters as integer pixels (typical for Landsat surface reflectance)
      val red: RasterRDD[Int] = sc.geoTiff[Int](b4Path)
      val nir: RasterRDD[Int] = sc.geoTiff[Int](b5Path)

      // Stack the two rasters; this requires identical metadata (alignment, CRS, resolution, tiling)
      val stacked: RasterRDD[Array[Int]] =
        try {
          red.overlay(nir)
        } catch {
          case e: Throwable =>
            throw new RuntimeException("B4 and B5 grids do not match — warp one band first", e)
        }

      // Compute NDVI per pixel: (NIR - RED) / (NIR + RED)
      // Use -9999.0f when denominator is zero. Note: NoData masking is not available via documented APIs.
      val ndvi: RasterRDD[Float] = stacked.mapPixels((px: Array[Int]) => {
        val r: Float = px(0).toFloat
        val n: Float = px(1).toFloat
        val denom: Float = n + r
        if (denom == 0.0f) -9999.0f else (n - r) / denom
      })

      // Write output as a single GeoTIFF with LZW compression (compatibility mode)
      ndvi.saveAsGeoTiff(
        outPath,
        Seq(GeoTiffWriter.Compression -> TiffConstants.COMPRESSION_LZW,
        GeoTiffWriter.WriteMode -> "compatibility")
      )

      println(s"NDVI written to: $outPath")
    } finally {
      spark.stop()
    }
  }
}

// NOTES
// (a) RDPro APIs used:
// - sc.geoTiff[Int]
// - overlay
// - mapPixels
// - saveAsGeoTiff
// - GeoTiffWriter.Compression
// - GeoTiffWriter.WriteMode

// (b) Unsupported operations and why:
// - Reading per-band NoData values and creating masks accordingly: No API in provided docs to read band NoData from input rasters, so exact masking semantics from the Python (masking when red or nir equals their NoData) cannot be replicated. The code only masks denom == 0.
// - Setting NoData tag in output GeoTIFF: No documented API to set a NoData value in the output; the raster writes numeric -9999.0f where masked, but does not set the GeoTIFF NoData metadata.
// - Explicit alignment/CRS/transform checking: No documented API to fetch RasterMetadata on the driver for comparison. The code relies on overlay’s requirement and throws a clear error if overlay fails, matching the Python’s fail-fast behavior.

// (c) Assumptions about IO paths / bands / nodata / CRS / environment detection logic:
// - Inputs are single-band GeoTIFFs with integer pixels (typical for Landsat SR). If inputs are Float GeoTIFFs, adjust the type parameter to Float.
// - Overlay is called in the order (red, nir), so stacked pixel arrays are [red, nir].
// - Alignment: If rasters are not aligned (different metadata), overlay will fail; we catch and rethrow with a clear message: “B4 and B5 grids do not match — warp one band first.”
// - Output path handling: We infer locality from URI scheme; for local paths (no scheme or file:), we create parent directories. For distributed schemes (e.g., hdfs://), path is passed as-is to saveAsGeoTiff. WriteMode is set to “compatibility” to produce a single GeoTIFF similar to the Python output.

ComputeNdviRDPro.run(sc)
System.exit(0)
