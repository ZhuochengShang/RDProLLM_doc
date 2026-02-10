### FilterPixels
This operation can filter out (remove) some pixels based on a provided condition.
The following example will keep only the pixels with temperature greater than 300&deg;K.
```scala
@param inputRaster the input raster
@param filter the filter function that tells which pixel values to keep in the output
@tparam T the thpe of the pixels in the input
@return a new raster where only pixels that pass the test are retained
def filterPixels[T: ClassTag](inputRaster: RasterRDD[T], filter: T => Boolean): RasterRDD[T]
```

```scala
val temperatureK: RasterRDD[Float] =
  sc.hdfFile("MOD11A1.A2022173.h08v05.006.2022174092443.hdf", "LST_Day_1km")
temperatureK.filterPixels(_>300).saveAsGeoTiff("temperature_high")
```