### Reproject
This operation changes the coordinate reference system (CRS) of a raster layer to a different one.
It keeps the resolution and tile size the same as the input.
The following code sample converts an HDF file stored in Sinusoidal projection to EPSG:4326.
```scala
val temperature: RasterRDD[Float] = 
  sc.hdfFile("MOD11A1.A2022173.h08v05.006.2022174092443.hdf", "LST_Day_1km")
temperature.reproject(4326)
```