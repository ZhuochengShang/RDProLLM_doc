## Background

Raster data represents geospatial information as multi-dimensional arrays (image-like grids), commonly used for satellite and Earth observation analysis.

RDPro (Raster Distributed Processor) is Beast's component for distributed raster processing on Apache Spark.

## Setup

- Follow [dev-setup.md](dev-setup.md) to configure Beast.
- In Scala, import Beast APIs:

```scala
import edu.ucr.cs.bdlab.beast._
```

## Raster Data Model

### Grid space (pixel coordinates)

- Raster size is `W x H` (width x height).
- Origin is top-left `(0, 0)`.
- Coordinates are `(col, row)` and are independent of geography.

### Raster tiles

- The grid is partitioned into `tileWidth x tileHeight` tiles.
- Edge tiles may be smaller.
- Each tile has a unique `tileID`.

### World space (geographic coordinates)

- World extent is represented as `[x1, x2) x [y1, y2)` in the dataset CRS.

### Grid to World (G2W)

- `G2W` is the affine transform from grid coordinates to world coordinates.

### World to Grid (W2G)

- `W2G` is the inverse affine transform from world coordinates to grid coordinates.

## RDPro Data Abstractions

### `ITile[T]`

- Atomic processing unit in RDPro.
- Contains `tileID`, `rasterMetadata`, and optional `rasterFeature`.

### `RasterMetadata`

- Contains extent (`x1, y1, x2, y2`), tile layout, CRS (`srid`), and transform.

### `RasterRDD[T]`

- Alias for distributed raster datasets: `RDD[ITile[T]]`.

## Global Preconditions

- `sc` is an initialized SparkContext.
- Paths exist and are readable.
- Type parameter `T` matches actual raster pixel type.

## Load

### `geoTiff[T]`
- Purpose: Load GeoTIFF raster data as a distributed `RasterRDD[T]`.
- Name: `geoTiff[T]`
- Input: `path: String`
- Output: `RasterRDD[T]`
- Example:
```scala
val landcover: RasterRDD[Int] = sc.geoTiff[Int]("glc2000_v1_1.tif")
```
```scala
import org.apache.spark.sql.types.{ArrayType, FloatType, IntegerType}

val path = "glc2000_v1_1.tif"
val probe = sc.geoTiff(path)

probe.first.pixelType match {
  case IntegerType =>
    val rasterInt: RasterRDD[Int] = sc.geoTiff[Int](path)
  case FloatType =>
    val rasterFloat: RasterRDD[Float] = sc.geoTiff[Float](path)
  case ArrayType(IntegerType, _) =>
    val rasterIntArr: RasterRDD[Array[Int]] = sc.geoTiff[Array[Int]](path)
  case ArrayType(FloatType, _) =>
    val rasterFloatArr: RasterRDD[Array[Float]] = sc.geoTiff[Array[Float]](path)
  case other =>
    throw new IllegalArgumentException(s"Unsupported pixel type: $other")
}
```
- Explicit expectations:
  - `path` points to a GeoTIFF file or folder of GeoTIFF files.
  - Type `T` matches source pixel type (`Int`, `Float`, `Array[Int]`, `Array[Float]`).
  - CRS, extent, transform, and tile metadata are preserved from source.

### `hdfFile[T]`
- Purpose: Load one HDF subdataset as a distributed `RasterRDD[T]`.
- Name: `hdfFile[T]`
- Input: `path: String, dataset: String`
- Output: `RasterRDD[T]`
- Example:
```scala
val tempK: RasterRDD[Float] =
  sc.hdfFile("MOD11A1.A2022173.h08v05.006.2022174092443.hdf", "LST_Day_1km")
```
- Explicit expectations:
  - `path` points to an HDF file.
  - `dataset` subdataset exists.
  - Returns values for that subdataset.

## Pixel ops

### `mapPixels`
- Purpose: Apply an element-wise transformation to pixel values.
- Name: `mapPixels`
- Input: `input: RasterRDD[T], f: T => U`
- Output: `RasterRDD[U]`
- Example:
```scala
val tempF: RasterRDD[Float] = tempK.mapPixels(k => (k - 273.15f) * 9 / 5 + 32)
```
- Explicit expectations:
  - `f` should be deterministic per pixel.
  - Raster geometry stays the same (CRS, extent, dimensions, tiling).
  - Only pixel values are transformed.

### `filterPixels`
- Purpose: Keep only pixels that satisfy a predicate.
- Name: `filterPixels`
- Input: `input: RasterRDD[T], p: T => Boolean`
- Output: `RasterRDD[T]`
- Example:
```scala
val hot: RasterRDD[Float] = tempK.filterPixels(_ > 300)
```
- Explicit expectations:
  - Raster geometry is unchanged.
  - Non-matching pixels are excluded or masked.

### `flatten`
- Purpose: Convert raster pixels to sample tuples for global statistics or joins.
- Name: `flatten`
- Input: `input: RasterRDD[T]`
- Output: `RDD[(Int, Int, RasterMetadata, T)]`
- Example:
```scala
val hist = landcover.flatten.map(_._4).countByValue().toMap
```
- Explicit expectations:
  - Each record includes `(col, row)`, metadata, and pixel value.

## Multi-raster

### `overlay`
- Purpose: Stack multiple aligned rasters into multi-band per-pixel arrays.
- Name: `overlay`
- Input: `RasterRDD[T], RasterRDD[T], ...`
- Output: `RasterRDD[Array[T]]`
- Example:
```scala
val stacked: RasterRDD[Array[Int]] = raster1.overlay(raster2)
```
- Explicit expectations:
  - All input rasters align in CRS, extent, resolution, and tile layout.
  - Output stores stacked per-pixel values.
  - Operation fails on metadata mismatch.

## Reshape

### `retile`
- Purpose: Change tile layout to optimize partitioning and downstream performance.
- Name: `retile`
- Input: `input: RasterRDD[T], tileWidth: Int, tileHeight: Int`
- Output: `RasterRDD[T]`
- Example:
```scala
val retiled = landcover.retile(64, 64)
```
- Explicit expectations:
  - `tileWidth > 0` and `tileHeight > 0`.
  - Pixel values are preserved.
  - Tile boundaries and layout are changed.

### `reproject`
- Purpose: Convert a raster to a new coordinate reference system (CRS).
- Name: `reproject`
- Input: `input: RasterRDD[T], srid: Int`
- Output: `RasterRDD[T]`
- Example:
```scala
val wgs84 = landcover.reproject(4326)
```
- Explicit expectations:
  - `srid` is a valid target CRS identifier.
  - Output CRS equals target `srid`.
  - Pixel values are resampled by implementation defaults.

### `rescale`
- Purpose: Change raster dimensions/resolution.
- Name: `rescale`
- Input: `input: RasterRDD[T], width: Int, height: Int`
- Output: `RasterRDD[T]`
- Example:
```scala
val small = landcover.rescale(360, 180)
```
- Explicit expectations:
  - `width > 0` and `height > 0`.
  - Output dimensions become `width x height`.
  - CRS and extent remain consistent with operation definition.

### `reshapeNN`
- Purpose: Perform general warp to match target metadata using nearest-neighbor interpolation.
- Name: `reshapeNN`
- Input: `input: RasterRDD[T], target: RasterMetadata`
- Output: `RasterRDD[T]`
- Example:
```scala
val target = RasterMetadata.create(-124, 42, -114, 32, 4326, 1000, 1000, 100, 100)
val warpedNN = RasterOperationsFocal.reshapeNN(landcover, target)
```
- Explicit expectations:
  - `target` is fully specified (extent, CRS, dimensions, tile size).
  - Output metadata matches target exactly.
  - Interpolation method is nearest-neighbor.

### `reshapeAverage`
- Purpose: Perform general warp to match target metadata using average interpolation.
- Name: `reshapeAverage`
- Input: `input: RasterRDD[T], target: RasterMetadata`
- Output: `RasterRDD[T]`
- Example:
```scala
val target = RasterMetadata.create(-124, 42, -114, 32, 4326, 1000, 1000, 100, 100)
val warpedAvg = RasterOperationsFocal.reshapeAverage(landcover, target)
```
- Explicit expectations:
  - `target` is fully specified (extent, CRS, dimensions, tile size).
  - Output metadata matches target exactly.
  - Interpolation method is average.

## Write

### `saveAsGeoTiff`
- Purpose: Persist raster results to GeoTIFF format.
- Name: `saveAsGeoTiff`
- Input: `input: RasterRDD[T], outputPath: String, options?: Seq[(Any, Any)]`
- Output: GeoTIFF files on disk
- Example:
```scala
small.saveAsGeoTiff("glc_small")
```
- Explicit expectations:
  - `outputPath` is writable.
  - Optional writer options are valid (compression, write mode, bits).
  - Output is written to disk.
  - Distributed mode writes multiple part files.
  - Compatibility mode writes a single GIS-compatible file.

#### `saveAsGeoTiff` advanced options

- Purpose: Control compression, write layout, and bit depth behavior.
- Name: `saveAsGeoTiff` options
- Input: writer options with `GeoTiffWriter` and `TiffConstants`
- Output: GeoTIFF with configured storage properties
- Example:
```scala
import edu.ucr.cs.bdlab.raptor.GeoTiffWriter
import edu.ucr.cs.bdlab.beast.io.tiff.TiffConstants

raster.saveAsGeoTiff("temperature_f",
  GeoTiffWriter.Compression -> TiffConstants.COMPRESSION_LZW)
raster.saveAsGeoTiff("temperature_f",
  GeoTiffWriter.Compression -> TiffConstants.COMPRESSION_DEFLATE)
raster.saveAsGeoTiff("temperature_f",
  GeoTiffWriter.Compression -> TiffConstants.COMPRESSION_NONE)

raster.saveAsGeoTiff("temperature_f", GeoTiffWriter.WriteMode -> "distributed")
raster.saveAsGeoTiff("temperature_f", GeoTiffWriter.WriteMode -> "compatibility")

raster.saveAsGeoTiff(
  "temperature_f",
  Seq(
    GeoTiffWriter.Compression -> TiffConstants.COMPRESSION_LZW,
    GeoTiffWriter.WriteMode -> "compatibility"
  )
)

raster.saveAsGeoTiff("temperature_f", GeoTiffWriter.CompactBits -> true)
raster.saveAsGeoTiff("temperature_f", GeoTiffWriter.BitsPerSample -> "8,8,8")
```
- Explicit expectations:
  - Advanced options require imports:
    - `import edu.ucr.cs.bdlab.raptor.GeoTiffWriter`
    - `import edu.ucr.cs.bdlab.beast.io.tiff.TiffConstants`
  - `CompactBits` is valid for integer pixel data.
  - `BitsPerSample` values are used as provided; ensure they are correct.
  - Use `"distributed"` mode when expecting many output files (for example after `explode`).

## Minimal Validation Checks

Use these checks in examples/tests:

```scala
assert(raster.first.rasterMetadata.srid == 4326)
assert(scaled.first.rasterMetadata.rasterWidth == 360)
assert(scaled.first.rasterMetadata.rasterHeight == 180)
```

## Failure Modes to Document

- Pixel type mismatch between `T` and source data.
- Missing HDF subdataset name.
- Overlay with misaligned metadata.
- Invalid target CRS or invalid target dimensions.
- Unwritable output path.

## Quick Mapping

| GDAL/rasterio | RDPro |
|---|---|
| open dataset | `geoTiff`, `hdfFile` |
| raster math | `mapPixels` |
| masking | `filterPixels` |
| stack bands | `overlay` |
| warp/reproject | `reproject`, `reshapeNN`, `reshapeAverage` |
| resample | `rescale` |
| write output | `saveAsGeoTiff` |
