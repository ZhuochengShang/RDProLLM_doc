### MapPixels
This operation applies a mathematical function to each pixel to produce an output raster with the same dimensions
of the input. For example, the following code converts a raster that contains temperature values in Kelvin
to Fahrenheit.
```scala
@param inputRaster the input raster RDD
@param f the function to apply on each input pixel value to produce the output pixel value
@tparam T the type of pixels in the input
@tparam U the type of pixels in the output
@return the resulting RDD
def mapPixels[T: ClassTag, U: ClassTag](inputRaster: RasterRDD[T], f: T => U): RasterRDD[U]
```

```scala
val temperatureK: RasterRDD[Float] =
  sc.hdfFile("MOD11A1.A2022173.h08v05.006.2022174092443.hdf", "LST_Day_1km")
val temperatureF: RasterRDD[Float] = temperatureK.mapPixels(k => (k-273.15f) * 9 / 5 + 32)
temperatureF.saveAsGeoTiff("temperature_f")
```
Note: The file can be downloaded from the
[LP DAAC archive website](https://e4ftl01.cr.usgs.gov/MOLT/MOD11A1.006/2022.06.22/MOD11A1.A2022173.h08v05.006.2022174092443.hdf)
but you will need to be logged in to download the file.