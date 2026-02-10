import edu.ucr.cs.bdlab.beast._
import org.apache.spark.rdd.RDD

val B4_PATH = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/data/landsat8/LA/B4/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B4.TIF" // Red
val B5_PATH = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/data/landsat8/LA/B5/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B5.TIF" // NIR
val OUT_NDVI = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/output/ndvi" // Output directory

// Load datasets
val red: RasterRDD[Float] = sc.geoTiff(B4_PATH)
val nir: RasterRDD[Float] = sc.geoTiff(B5_PATH)

// Check grid alignment
if (red.metadata != nir.metadata) {
  throw new RuntimeException("B4 and B5 grids do not match — warp one band first")
}

// Handle NoData
val redNoData = red.flatten().filter(_._4.isNaN).map(_._4).distinct().collect().headOption
val nirNoData = nir.flatten().filter(_._4.isNaN).map(_._4).distinct().collect().headOption

val mask = red.flatten().map { case (x, y, metadata, value) =>
  val isRedNoData = redNoData.exists(_ == value)
  val isNirNoData = nirNoData.exists(_ == nir.get(x, y))
  (x, y, value, isRedNoData || isNirNoData)
}.filter(_._4).map(_._1).distinct()

// NDVI calculation
val ndvi = red.mapPixels { value =>
  val nirValue = nir.get(value._1, value._2)
  val denom = nirValue + value
  if (denom == 0 || mask.contains(value._1)) -9999.0f else (nirValue - value) / denom
}

// Create output GeoTIFF
ndvi.saveAsGeoTiff(OUT_NDVI)

println("NDVI written to: " + OUT_NDVI)