### Reshape
The reshape operation is the most general way to change the shape of the raster data.
It takes a new `RasterMetadata` and changes the input raster to match it.
This is an advanced method that should only be used when a specific requirement is needed
and efficiency if crucial.
The following example changes the coordinate reference system, the region of interest, the resolution,
and the tile size all in one call. This is expected to be more efficient and accurate than making
several calls, e.g., reproject, rescale, ... etc.
```scala
val raster: RasterRDD[Int] = sc.geoTiff[Int]("glc2000_v1_1.tif")
val reshaped = RasterOperationsFocal.reshapeNN(raster,
  RasterMetadata.create(-124, 42, -114, 32, 4326, 1000, 1000, 100, 100))
reshaped.saveAsGeoTiff("glc_ca")
```

There are two implementations of the reshape functions, `reshapeNN` and `reshapeAverage`, which are contrasted below.

- `reshapeNN`: This is the default method used with rescale and reproject. In this method, each pixel in the target
get its value from the nearest pixel in the source. In other words, the center of each target pixel is mapped to
the source raster and whatever value is there is used. This method works for any type of data, i.e., categorical or
numerical. It is also generally more efficient. However, if the input raster is being downsized, contains continuous
values, or has a lot of empty pixels, then the result might be of a poor quality.
- `reshapeAverage`: In this method, if multiple source pixels are grouped into a single target pixel, their average
value is computed. This method should only be used if the pixel data is numerical and continuous, e.g., temperature,
vegetation, or visible frequencies. Empty pixels will be ignored while computing the average which makes this method
helpful if the input raster contains a lot of empty patches.

The following code shows how to use the average interpolation method with rescale or reproject functions.
```scala
val temperature: RasterRDD[Float] =
  sc.hdfFile("MOD11A1.A2022173.h08v05.006.2022174092443.hdf", "LST_Day_1km")
val rescaled = temperature.rescale(360, 180, RasterOperationsFocal.InterpolationMethod.Average)
temperature.reproject(CRS.decode("EPSG:4326"), RasterOperationsFocal.InterpolationMethod.Average)
```