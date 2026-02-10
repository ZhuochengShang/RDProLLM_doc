```scala
import edu.ucr.cs.bdlab.beast._
import org.apache.spark.rdd.RDD
import edu.ucr.cs.bdlab.beast.geolite.ITile

// Define file paths
val B4_PATH = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/data/landsat8/LA/B4/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B4.TIF"
val B5_PATH = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/data/landsat8/LA/B5/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B5.TIF"
val OUT_NDVI = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/output/ndvi"

// Load the raster data
val redRaster: RasterRDD[Float] = sc.geoTiff(B4_PATH)
val nirRaster: RasterRDD[Float] = sc.geoTiff(B5_PATH)

// Overlay the two rasters to ensure they have the same metadata
val overlaidRasters: RasterRDD[Array[Float]] = redRaster.overlay(nirRaster)

// Calculate NDVI
val ndviRaster: RasterRDD[Float] = overlaidRasters.mapPixels { case Array(red, nir) =>
  val denom = nir + red
  if (denom == 0) -9999.0f else (nir - red) / denom
}

// Save the NDVI raster as a GeoTIFF
ndviRaster.saveAsGeoTiff(OUT_NDVI)

// Print confirmation
println(s"NDVI written to: $OUT_NDVI")
```

System.exit(0)
