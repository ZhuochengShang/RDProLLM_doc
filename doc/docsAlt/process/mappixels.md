### MapPixels (element-wise transform)

```scala
val temperatureF: RasterRDD[Float] =
  temperatureK.mapPixels(k => (k - 273.15f) * 9 / 5 + 32)
```

This produces an output raster with the same shape and metadata.
