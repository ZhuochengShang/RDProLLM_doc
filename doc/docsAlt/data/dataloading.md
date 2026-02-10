## Opening Raster Datasets (GeoTIFF / HDF)

In GDAL or rasterio, raster processing starts by **opening a dataset from disk**.
RDPro follows the same idea, except datasets are loaded as distributed
collections.

```scala
// Load a GeoTIFF
val raster: RasterRDD[Int] = sc.geoTiff("glc2000_v1_1.tif")

// Load an HDF subdataset (similar to GDAL subdatasets)
val temperatureK: RasterRDD[Float] =
  sc.hdfFile(
    "MOD11A1.A2022173.h08v05.006.2022174092443.hdf",
    "LST_Day_1km"
  )
```

The input can be a single file or a directory containing many files; in both
cases, the data are loaded into a single `RasterRDD`.

---

### Determining pixel data types

Similar to inspecting `dataset.dtypes` in rasterio, RDPro allows you to inspect
pixel types:

```scala
val raster = sc.geoTiff("glc2000_v1_1.tif")
println(raster.first.pixelType)
```

Example output:
```
IntegerType
```

Supported types:

| Pixel type                  | Loading statement            |
|-----------------------------|------------------------------|
| IntegerType                 | `sc.geoTiff[Int]`            |
| FloatType                   | `sc.geoTiff[Float]`          |
| ArrayType(IntegerType,true) | `sc.geoTiff[Array[Int]]`     |
| ArrayType(FloatType,true)   | `sc.geoTiff[Array[Float]]`   |

---