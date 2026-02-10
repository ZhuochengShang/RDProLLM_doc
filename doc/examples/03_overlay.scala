import edu.ucr.cs.bdlab.beast._
import edu.ucr.cs.bdlab.beast.common.{BeastOptions}
import edu.ucr.cs.bdlab.raptor.RasterOperationsLocal
import org.apache.hadoop.fs.{Path, PathFilter}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


object overlay {

def run(sc: SparkContext): RasterRDD[Array[Int]] = {
    val pathOne =
      "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/data/landsat8/LA/B4/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B4.TIF"
    val pathTwo =
      "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/data/landsat8/LA/B5/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B5.TIF"

    val rasterOne: RasterRDD[Int] = sc.geoTiff[Int](pathOne)
    val rasterTwo: RasterRDD[Int] = sc.geoTiff[Int](pathTwo)
    
    // --- Alignment checks (no forall on RasterRDD) ---
    //val meta1 = rasterOne.first().rasterMetadata
    //val meta2 = rasterTwo.first().rasterMetadata
    //require(meta1 == meta2, "Cannot stack tiles with different metadata")

    val overLayRaster: RasterRDD[Array[Int]] = RasterOperationsLocal.overlay( rasterOne, rasterTwo)

    // Force execution + small driver-side sanity check
    val t = overLayRaster.first
    val v = t.getPixelValue(t.x1, t.y1)
    println(s"[dataloading] pixelType=${t.pixelType}, sample=$v, partitions=${overLayRaster.getNumPartitions}")

    overLayRaster
  }
}