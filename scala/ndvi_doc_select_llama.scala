import edu.ucr.cs.bdlab.raptor.SparkRaptor
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object NDVI {
  def main(args: Array[String]): Unit = {
    val sc = SparkRaptor.getContext()
    val redPath = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/data/landsat8/LA/B4/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B4.TIF"
    val nirPath = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/data/landsat8/LA/B5/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B5.TIF"
    val outNdviPath = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/python/ndvi.tif"

    // Read Red and NIR rasters
    val redRaster = sc.hdfFile(redPath, "B4")
    val nirRaster = sc.hdfFile(nirPath, "B5")

    // Calculate NDVI
    val ndviPixels = redRaster.mapPixels((red, nir) => {
      if (nir + red == 0 || (red == null && nir == null)) -9999.0
      else (nir - red) / (nir + red)
    })

    // Write NDVI raster
    ndviPixels.saveAsGeoTiff(outNdviPath, GeoTiffWriter.Compression -> TiffConstants.COMPRESSION_LZW)

    SparkRaptor.stop()
  }
}
//Please note that this code assumes you have the necessary dependencies and setup for RDPro on Spark.