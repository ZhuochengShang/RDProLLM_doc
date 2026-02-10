## Raster data Creation
An alternative way of creating the first raster RDD is by populating it directly from pixel values similar to the example below.
```scala
val metadata = RasterMetadata.create(x1 = -50, y1 = 40, x2 = -60, y2 = 30, srid = 4326,
  rasterWidth = 10, rasterHeight = 10, tileWidth = 10, tileHeight = 10)
val pixels = sc.parallelize(Seq(
  (0, 0, 100),
  (3, 4, 200),
  (8, 9, 300)
))
val raster = sc.rasterizePixels(pixels, metadata)
```

The metadata describes the geographic meaning of the pixel values.
 - The values (x1, y1) represent the north-west corner of the raster dataset.
 - The values (x2, y2) represent the south-east corner of the raster dataset.
 - SRID represents the coordinate reference system. For example, 4326 represents ["EPSG:4325"](https://epsg.io/4326)
 - rasterWidth&times;rasterHeight is the resolution of the entire raster dataset in pixels
 - tileWidth&times;tileHeight is the resolution of each tile in the raster
Notice that in most cases y1 will be greater than y2.
The pixels RDD contains a set of (x, y, m) values where (x, y) is the pixel location and m is the pixel value.

Alternatively, you can use the method `rasterizePoints` which takes the geographic coordinates of each pixel
instead of its raster locations as shown below.
```scala
val metadata = RasterMetadata.create(x1 = -50, y1 = 40, x2 = -60, y2 = 30, srid = 4326,
  rasterWidth = 10, rasterHeight = 10, tileWidth = 10, tileHeight = 10)
val pixels = sc.parallelize(Seq(
  (-51.3, 30.4, 100),
  (-55.2, 34.5, 200),
  (-56.4, 39.2, 300)
))
val raster = sc.rasterizePoints(pixels, metadata)
```