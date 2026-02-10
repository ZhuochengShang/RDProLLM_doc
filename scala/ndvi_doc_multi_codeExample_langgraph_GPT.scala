```scala
import edu.ucr.cs.bdlab.beast._
import org.apache.spark.rdd.RDD
import edu.ucr.cs.bdlab.beast.geolite.ITile
import edu.ucr.cs.bdlab.beast.geolite.RasterRDD

// Define file paths
val B4_PATH = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/data/landsat8/LA/B4/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B4.TIF"
val B5_PATH = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/data/landsat8/LA/B5/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B5.TIF"
val OUT_NDVI = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/output/ndvi"

// Load the raster data
val redRaster: RasterRDD[Float] = sc.geoTiff[Float](B4_PATH)
val nirRaster: RasterRDD[Float] = sc.geoTiff[Float](B5_PATH)

// Overlay the two rasters
val stacked: RasterRDD[Array[Float]] = redRaster.overlay(nirRaster)

// Filter out NoData values
val redNoDataValue = -9999.0f
val nirNoDataValue = -9999.0f
val filtered: RasterRDD[Array[Float]] = stacked.filterPixels { pixel =>
  val red = pixel(0)
  val nir = pixel(1)
  red != redNoDataValue && nir != nirNoDataValue
}

// Calculate NDVI
val ndvi: RasterRDD[Float] = filtered.mapPixels { pixel =>
  val red = pixel(0)
  val nir = pixel(1)
  val denom = nir + red
  if (denom == 0) -9999.0f else (nir - red) / denom
}

// Save the result as a GeoTIFF
ndvi.saveAsGeoTiff(OUT_NDVI)

println(s"NDVI written to: $OUT_NDVI")
```

System.exit(0)
