### Explode
The explode function separates each tile from other tiles in the raster.
The values stay the same but this function helps to write each tile in a separate file.
For example, if you prepare your data for a machine learning algorithm that expects
every 256&times;256 tile to be in a separate file, then the explode function will help.
```scala
val raster: RasterRDD[Int] = sc.geoTiff[Int]("glc2000_v1_1.tif")
val retiled = raster.retile(64, 64).explode
retiled.saveAsGeoTiff("glc_retiled", GeoTiffWriter.WriteMode -> "distributed")
```

Hint: It is always better to use the distributed writing mode with exploded rasters
since the number of generated files will usually be very large.