### FilterPixels (masking)

Equivalent to masking raster values based on a condition:

```scala
temperatureK
  .filterPixels(_ > 300)
  .saveAsGeoTiff("temperature_high")
```