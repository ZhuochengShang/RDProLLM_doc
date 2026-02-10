import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import edu.ucr.cs.bdlab.beast._
import edu.ucr.cs.bdlab.raptor.GeoTiffWriter
import edu.ucr.cs.bdlab.beast.io.tiff.TiffConstants

object NDVIPythonPort {
  def main(args: Array[String]): Unit = {
    // Defaults copied from python/ndvi.py
    val defaultRedPath = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/data/landsat8/LA/B4/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B4.TIF"
    val defaultNirPath = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/data/landsat8/LA/B5/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B5.TIF"
    val defaultOutPath = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/python/ndvi.py"

    val redPath = if (args.length >= 1) args(0) else defaultRedPath
    val nirPath = if (args.length >= 2) args(1) else defaultNirPath
    val outPath = if (args.length >= 3) args(2) else defaultOutPath

    val spark = SparkSession.builder().appName("NDVIPythonPort").getOrCreate()
    val sc: SparkContext = spark.sparkContext

    try {
      // Load input rasters with correct pixel type, then convert to Float
      val redUntyped = sc.geoTiff(redPath)
      val nirUntyped = sc.geoTiff(nirPath)
      val redPixelType = redUntyped.first.pixelType
      val nirPixelType = nirUntyped.first.pixelType

      val redFloat: RasterRDD[Float] = redPixelType match {
        case "FloatType"   => sc.geoTiff[Float](redPath)
        case "IntegerType" => sc.geoTiff[Int](redPath).mapPixels((v: Int) => v.toFloat)
        case other         => throw new RuntimeException(s"Unsupported pixel type for RED band: $other")
      }

      val nirFloat: RasterRDD[Float] = nirPixelType match {
        case "FloatType"   => sc.geoTiff[Float](nirPath)
        case "IntegerType" => sc.geoTiff[Int](nirPath).mapPixels((v: Int) => v.toFloat)
        case other         => throw new RuntimeException(s"Unsupported pixel type for NIR band: $other")
      }

      // Alignment check (equivalent to GDAL geotransform/projection check)
      val redMeta = redFloat.first.rasterMetadata
      val nirMeta = nirFloat.first.rasterMetadata
      if (!redMeta.equals(nirMeta)) {
        throw new RuntimeException("B4 and B5 grids do not match — warp one band first")
      }

      // NOTE: The Python script masks NoData values using GDAL GetNoDataValue.
      // RDPro docs used in this project do not expose a NoData accessor, so that mask cannot be replicated here.
      // We only guard against denominator == 0 as in the Python code.

      // Overlay (stack) the rasters in the order: [RED, NIR]
      val stacked: RasterRDD[Array[Float]] = redFloat.overlay(nirFloat)

      // NDVI = (NIR - RED) / (NIR + RED), set -9999.0f when denom == 0
      val ndvi: RasterRDD[Float] = stacked.mapPixels((px: Array[Float]) => {
        val r: Float = px(0)
        val n: Float = px(1)
        val denom: Float = n + r
        if (denom == 0.0f) -9999.0f else (n - r) / denom
      })

      ndvi.saveAsGeoTiff(
        outPath,
        GeoTiffWriter.Compression -> TiffConstants.COMPRESSION_LZW,
        GeoTiffWriter.WriteMode -> "compatibility"
      )

      println(s"NDVI written to: $outPath")
    } finally {
      spark.stop()
    }
  }
}
