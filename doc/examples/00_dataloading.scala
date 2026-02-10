import edu.ucr.cs.bdlab.beast._
import edu.ucr.cs.bdlab.beast.common.{BeastOptions, CLIOperation}
import org.apache.hadoop.fs.{Path, PathFilter}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object dataloading {

def run(sc: SparkContext): RasterRDD[Int] = {
    val path =
      "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/data/landsat8/LA/B4/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B4.TIF"

    val raster: RasterRDD[Int] = sc.geoTiff[Int](path)

    // Force execution + small driver-side sanity check
    val t = raster.first
    val v = t.getPixelValue(t.x1, t.y1)
    println(s"[dataloading] pixelType=${t.pixelType}, sample=$v, partitions=${raster.getNumPartitions}")

    raster
  }
}