## Creating Raster Datasets Programmatically

In addition to reading files, rasters can be constructed directly from pixel
values, similar to manually creating arrays in NumPy.

### Rasterizing grid coordinates

```scala
val metadata = RasterMetadata.create(
  x1 = -50, y1 = 40, x2 = -60, y2 = 30,
  srid = 4326,
  rasterWidth = 10, rasterHeight = 10,
  tileWidth = 10, tileHeight = 10
)

val pixels = sc.parallelize(Seq(
  (0, 0, 100),
  (3, 4, 200),
  (8, 9, 300)
))

val raster = sc.rasterizePixels(pixels, metadata)
```

---

### Rasterizing geographic coordinates

Instead of grid indices, pixel values can be defined using geographic
coordinates:

```scala
val pixels = sc.parallelize(Seq(
  (-51.3, 30.4, 100),
  (-55.2, 34.5, 200),
  (-56.4, 39.2, 300)
))

val raster = sc.rasterizePoints(pixels, metadata)
```

---