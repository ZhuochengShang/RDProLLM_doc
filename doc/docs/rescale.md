### Rescale
The rescale operation changes the resolution of the raster, i.e., number of pixels, without changing
the tile size or coordinate reference system. The following example reads an input raster
and generates a thumbnail by reducing its size. Notice that we use the compatibility mode so that
the thumbnail can be loaded on any GIS software.
```scala
val raster: RasterRDD[Int] = sc.geoTiff[Int]("glc2000_v1_1.tif")
val rescaled = raster.rescale(360, 180)
rescaled.saveAsGeoTiff("glc_small", GeoTiffWriter.WriteMode -> "compatibility")
```