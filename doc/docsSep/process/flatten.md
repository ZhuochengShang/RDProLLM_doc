### Flatten
The flatten method extracts all non-empty pixels from the raster into a non-raster RDD.
This can help with computing global statistics for the entire dataset, e.g., minimum, maximum, average, or histogram.
The following example computes the histogram of the global land cover.
```scala
@param raster the raster to extract its pixels
@tparam T the type of pixel values
@return an RDD that contains all pixel locations and values
def flatten[T](raster: RasterRDD[T]): RDD[(Int, Int, RasterMetadata, T)]
```

```scala
val raster: RasterRDD[Int] = sc.geoTiff[Int]("glc2000_v1_1.tif")
val histogram: Map[Int, Long] = raster.flatten.map(_._4).countByValue().toMap
println(histogram)
```
The flatten method returns an RDD of tuples where each tuple contains the following values:
 - *x*: The pixel x location on the raster grid, i.e., the index of the column
 - *y*: The pixel y location on the raster grid, i.e., the index of the row
 - *metadata*: The metadata of the raster that can be used to convert the (x, y) coordinates into geographical coordinates
 - *m*: The measure value of that pixel