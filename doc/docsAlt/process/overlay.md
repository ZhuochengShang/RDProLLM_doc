## Stacking Rasters (Multi-band Datasets)

In GDAL, multiple rasters can be combined into a **multi-band dataset**.  
RDPro provides the same functionality via `overlay`.

```scala
val stacked: RasterRDD[Array[Int]] =
  raster1.overlay(raster2)
```

All input rasters must have the same CRS, resolution, and tile size.
