## Raster Data Model (GDAL-style concepts)

RDPro explicitly models the same spatial concepts used implicitly in GDAL and
rasterio.

## Concept Mapping (for GDAL / rasterio users)

| GDAL / rasterio concept | RDPro |
|------------------------|-------|
| `gdal.Open` / `rasterio.open` | `sc.geoTiff` |
| HDF subdataset selection | `sc.hdfFile(path, dataset)` |
| Raster math (NumPy-style) | `mapPixels` |
| Masking | `filterPixels` |
| Band stacking / multi-band | `overlay` |
| Warp / resample | `reproject`, `rescale`, `reshape` |
| GeoTIFF write | `saveAsGeoTiff` |


### Grid space (pixel coordinates)

A raster is represented as a **2D pixel grid**:

- Width **W** (columns)
- Height **H** (rows)
- Origin **(0, 0)** at the **top-left corner**

Pixel coordinates are represented as `(i, j)` where `i` is the column index and
`j` is the row index. These coordinates are independent of geography, just like
array indexing in NumPy or rasterio.

---

### Raster tiles (distributed blocks)

For distributed processing, the raster grid is divided into **tiles**:

- Each tile has dimensions `(tileWidth × tileHeight)`
- Edge tiles may be smaller
- Each tile has a unique **tile ID**

This is analogous to block/window processing in GDAL, but exposed explicitly to
enable parallel execution.

---

### World space (geographic coordinates)

World space represents a rectangular geographic region on the Earth’s surface
defined by four coordinates:

```
[x1, x2) × [y1, y2)
```

Coordinates are expressed in the raster’s CRS, exactly as in GDAL datasets.

---

### Grid ↔ World transformations

- **Grid → World (G2W)**: affine transform from pixel coordinates to geographic
  coordinates
- **World → Grid (W2G)**: inverse transform from geographic coordinates to pixel
  indices

These correspond directly to GDAL’s geotransform and inverse geotransform.

---

## RDPro Data Abstractions

### `ITile[T]` — atomic raster unit

An `ITile[T]` represents a single raster tile and is the smallest unit of
processing in RDPro.

```scala
@tparam T              Pixel value type
@param tileID          Tile identifier
@param rasterMetadata  Spatial metadata
@param rasterFeature   Optional auxiliary attributes (e.g., filename, time)
```

---

### `RasterRDD[T]`

`RasterRDD[T]` is an alias for `RDD[ITile[T]]` and represents a **distributed
raster dataset**, analogous to a GDAL or rasterio dataset, but partitioned across
Spark.

Each RasterRDD stores raster data as a collection of tiles, where each tile
contains pixel values and shared spatial metadata (CRS, resolution, extent).

Just like GDAL and rasterio require you to know a dataset’s data type
(e.g., uint8, float32), RDPro requires the pixel type to be specified
explicitly when creating or loading a raster.
The type parameter T corresponds to the pixel value type.

Supported pixel types:

| Pixel type                  | RasterRDD type              |
|-----------------------------|------------------------------|
| IntegerType                 | `RasterRDD[Int]`            |
| FloatType                   | `RasterRDD[Float]`          |
| ArrayType(IntegerType,true) | `RasterRDD[Array[Int]]`     |
| ArrayType(FloatType,true)   | `RasterRDD[Array[Float]]`   |
