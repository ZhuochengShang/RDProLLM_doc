### Retile
The retile function does not change any of the values in the raster but change the way tiles are created.
This can be sometimes helpful to improve the performance by adjusting the size of the tiles.
For example, square and bigger tiles can be more efficient for reprojection and window functions.

```scala
val raster: RasterRDD[Int] = sc.geoTiff[Int]("glc2000_v1_1.tif")
val retiled = raster.retile(64, 64)
retiled.saveAsGeoTiff("glc_retiled")
```