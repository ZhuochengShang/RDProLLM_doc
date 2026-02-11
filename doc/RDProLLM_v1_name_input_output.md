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

## Types

- `RasterRDD[T]`: distributed raster dataset (`RDD[ITile[T]]`)
- `RasterMetadata`: extent, CRS, transform, tile layout
- The possibilities for `T` are:
| Type                        | Loading statement          |
|-----------------------------|----------------------------|
| IntegerType                 | `sc.geoTiff[Int]`          |
| FloatType                   | `sc.geoTiff[Float]`        |
| ArrayType(IntegerType,true) | `sc.geoTiff[Array[Int]]`   |
| ArrayType(FloatType, true)  | `sc.geoTiff[Array[Float]]` |

## Load

### `geoTiff[T]`
- Purpose: Load GeoTIFF raster data as a distributed `RasterRDD[T]`.
- Input: `path: String`
- Output: `RasterRDD[T]`
- Note: The type parameter (for example `[Int]`) must match the actual pixel type in the file. If unsure, inspect `raster.first.pixelType`.

### `hdfFile[T]`
- Purpose: Load one HDF subdataset as a distributed `RasterRDD[T]`.
- Input: `path: String, dataset: String`
- Output: `RasterRDD[T]`
- Note: `dataset` must match an existing subdataset name in the HDF file.

## Pixel ops

### `mapPixels`
- Purpose: Apply an element-wise transformation to pixel values.
- Input: `RasterRDD[T], f: T => U`
- Output: `RasterRDD[U]`
- Note: Raster geometry (extent, CRS, dimensions, tiling) remains unchanged.

### `filterPixels`
- Purpose: Keep only pixels that satisfy a predicate.
- Input: `RasterRDD[T], p: T => Boolean`
- Output: `RasterRDD[T]`
- Note: Use this for masking or threshold-based filtering.

### `flatten`
- Purpose: Convert raster pixels to sample tuples for global statistics or joins.
- Input: `RasterRDD[T]`
- Output: `RDD[(Int, Int, RasterMetadata, T)]`
- Note: Output size can be large because it materializes pixel-level rows.

## Multi-raster

### `overlay`
- Purpose: Stack multiple aligned rasters into multi-band per-pixel arrays.
- Input: `RasterRDD[T], RasterRDD[T], ...`
- Output: `RasterRDD[Array[T]]`
- Note: All inputs must have the same metadata (CRS, extent, resolution, tile size). If not, align first with reshape/reproject/rescale.

## Reshape

### `retile`
- Purpose: Change tile layout to optimize partitioning and downstream performance.
- Input: `RasterRDD[T], tileWidth: Int, tileHeight: Int`
- Output: `RasterRDD[T]`
- Note: Pixel values are preserved; only tile boundaries change.

### `reproject`
- Purpose: Convert a raster to a new coordinate reference system (CRS).
- Input: `RasterRDD[T], srid: Int`
- Output: `RasterRDD[T]`
- Note: Resampling is applied during reprojection.

### `rescale`
- Purpose: Change raster dimensions/resolution.
- Input: `RasterRDD[T], width: Int, height: Int`
- Output: `RasterRDD[T]`
- Note: Keep CRS while adjusting pixel grid size.

### `reshapeNN`
- Purpose: Perform general warp to match target metadata using nearest-neighbor interpolation.
- Input: `RasterRDD[T], target: RasterMetadata`
- Output: `RasterRDD[T]`
- Note: Works well for categorical data.

### `reshapeAverage`
- Purpose: Perform general warp to match target metadata using average interpolation.
- Input: `RasterRDD[T], target: RasterMetadata`
- Output: `RasterRDD[T]`
- Note: Use for continuous numeric data.

## Write

### `saveAsGeoTiff`
- Purpose: Persist raster results to GeoTIFF format.
- Input: `RasterRDD[T], outputPath: String`
- Output: GeoTIFF files on disk
- Note: For advanced GeoTIFF writing options (for example compression, write mode, bit compaction), additional imports are required:
```scala
import edu.ucr.cs.bdlab.raptor.GeoTiffWriter
import edu.ucr.cs.bdlab.beast.io.tiff.TiffConstants
```
- Note: Common advanced options:
  - Compression: `GeoTiffWriter.Compression -> TiffConstants.COMPRESSION_LZW | COMPRESSION_DEFLATE | COMPRESSION_NONE`
  - Write mode: `GeoTiffWriter.WriteMode -> "distributed" | "compatibility"`
  - Bit compaction: `GeoTiffWriter.CompactBits -> true` (integer pixels only)
  - Bits per sample: `GeoTiffWriter.BitsPerSample -> "8,8,8"`
- Note: To pass multiple options in one call, use `Seq(...)` (for example `saveAsGeoTiff(path, Seq(opt1, opt2))`).

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
