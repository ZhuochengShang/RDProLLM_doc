### Overlay
THe overlay operation stacks multiple rasters on top of each other. This function only works if all the input
rasters have the same metadata, i.e., same resolution, CRS, and tile size. If the two inputs have mixed
metadata, they should be first converted using the reshape operation (see below) to make them compatible.
```scala
@param inputs the RDDs to overlay
@return a raster with the same metadata of the inputs where output pixels are the concatenation of input pixels.
def overlay[T: ClassTag, V](@varargs inputs: RDD[ITile[T]]*): RasterRDD[Array[V]]
```

```scala
val raster1: RasterRDD[Int] = sc.geoTiff[Int]("glc2000_v1_1.tif")
val raster2: RasterRDD[Int] = sc.geoTiff[Int]("vegetation")
val stacked: RasterRDD[Array[Int]] = raster1.overlay(raster2)
```