## Background

Raster data represents geospatial information as multi-dimensional arrays (image-like grids), commonly used for satellite and Earth observation analysis.

RDPro (Raster Distributed Processor) is Beast's component for distributed raster processing on Apache Spark.

## Setup

- Follow [dev-setup.md](dev-setup.md) to configure Beast.
- In Scala, import Beast APIs:

```scala
import edu.ucr.cs.bdlab.beast._
```

## Required Imports (Scala)

```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import edu.ucr.cs.bdlab.beast._
import edu.ucr.cs.bdlab.beast.cg.SpatialDataTypes.RasterRDD
import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.beast.geolite.{ITile, RasterFeature, RasterMetadata}
import edu.ucr.cs.bdlab.beast.io.tiff.TiffConstants
import edu.ucr.cs.bdlab.raptor.{GeoTiffWriter, RasterOperationsFocal, RasterOperationsLocal, RasterOperationsGlobal}
import edu.ucr.cs.bdlab.raptor.RaptorMixin._
import edu.ucr.cs.bdlab.raptor.RaptorMixin.RasterReadMixinFunctions
```

### Why these imports

- `SparkSession`, `SparkConf`: Spark app/session setup.
- `edu.ucr.cs.bdlab.beast._`: core RDPro/Beast raster APIs.
- `RasterRDD`: explicit raster RDD type alias import.
- `BeastOptions`: options container for reader/writer and operation configs.
- `ITile`, `RasterFeature`, `RasterMetadata`: explicit raster core data structures.
- `TiffConstants`: GeoTIFF compression constants.
- `GeoTiffWriter`: write options (`Compression`, `WriteMode`, `CompactBits`, `BitsPerSample`).
- `RasterOperationsFocal`: advanced reshape/warp operations.
- `RasterOperationsLocal`: pixel/tile-local operations.
- `RasterOperationsGlobal`: whole-raster/global operations.
- `RaptorMixin._`: extension methods for raster operations.
- `RasterReadMixinFunctions`: explicit read mixin import for raster loading helpers.

## Operation Families

- `RasterOperationsLocal`: local operations that act on each pixel/tile independently (for example `mapPixels`, `filterPixels`).
- `RasterOperationsFocal`: focal/reshape/reprojection operations that depend on spatial neighborhood or target grid (for example `reshapeNN`, `reshapeAverage`, `reproject`, `rescale`).
- `RasterOperationsGlobal`: global operations/aggregations computed over the full raster.

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

### `hdfFile[T]`
- Purpose: Load one HDF subdataset as a distributed `RasterRDD[T]`.
- Input: `path: String, dataset: String`
- Output: `RasterRDD[T]`
- Note: `dataset` must match an existing subdataset name in the HDF file.

```scala
val tempK: RasterRDD[Float] =
  sc.hdfFile("MOD11A1.A2022173.h08v05.006.2022174092443.hdf", "LST_Day_1km")
```

## Pixel ops

### `mapPixels`
- Purpose: Apply an element-wise transformation to pixel values.
- Input: `RasterRDD[T], f: T => U`
- Output: `RasterRDD[U]`
- Note: Raster geometry (extent, CRS, dimensions, tiling) remains unchanged.

```scala
val tempF: RasterRDD[Float] = tempK.mapPixels(k => (k - 273.15f) * 9 / 5 + 32)
```

### `filterPixels`
- Purpose: Keep only pixels that satisfy a predicate.
- Input: `RasterRDD[T], p: T => Boolean`
- Output: `RasterRDD[T]`
- Note: Use this for masking or threshold-based filtering.

```scala
val hot: RasterRDD[Float] = tempK.filterPixels(_ > 300)
```

### `flatten`
- Purpose: Convert raster pixels to sample tuples for global statistics or joins.
- Input: `RasterRDD[T]`
- Output: `RDD[(Int, Int, RasterMetadata, T)]`
- Note: Output size can be large because it materializes pixel-level rows.

```scala
val hist = landcover.flatten.map(_._4).countByValue().toMap
```

## Multi-raster

### `overlay`
- Purpose: Stack multiple aligned rasters into multi-band per-pixel arrays.
- Input: `RasterRDD[T], RasterRDD[T], ...`
- Output: `RasterRDD[Array[T]]`
- Note: All inputs must have the same metadata (CRS, extent, resolution, tile size). If not, align first with reshape/reproject/rescale.

```scala
val stacked: RasterRDD[Array[Int]] = raster1.overlay(raster2)
```

## Reshape

### `retile`
- Purpose: Change tile layout to optimize partitioning and downstream performance.
- Input: `RasterRDD[T], tileWidth, tileHeight`
- Output: `RasterRDD[T]`
- Note: Pixel values are preserved; only tile boundaries change.

```scala
val retiled = landcover.retile(64, 64)
```

### `reproject`
- Purpose: Convert a raster to a new coordinate reference system (CRS).
- Input: `RasterRDD[T], srid`
- Output: `RasterRDD[T]`
- Note: Resampling is applied during reprojection.

```scala
val wgs84 = landcover.reproject(4326)
```

### `rescale`
- Purpose: Change raster dimensions/resolution.
- Input: `RasterRDD[T], width, height`
- Output: `RasterRDD[T]`
- Note: Keep CRS while adjusting pixel grid size.

```scala
val small = landcover.rescale(360, 180)
```

### `reshapeNN`
- Purpose: Perform general warp to match target metadata using nearest-neighbor interpolation.
- Input: `RasterRDD[T], target: RasterMetadata`
- Output: `RasterRDD[T]`
- Note: Works well for categorical data.

```scala
val targetMetadata = RasterMetadata.create(-124, 42, -114, 32, 4326, 1000, 1000, 100, 100)
val warped = RasterOperationsFocal.reshapeNN(landcover, _=>targetMetadata)
```

### `reshapeAverage`
- Purpose: Perform general warp to match target metadata using average interpolation.
- Input: `RasterRDD[T], target: RasterMetadata`
- Output: `RasterRDD[T]`
- Note: Use for continuous numeric data.

```scala
val targetMetadata = RasterMetadata.create(-124, 42, -114, 32, 4326, 1000, 1000, 100, 100)
val warpedAvg = RasterOperationsFocal.reshapeAverage(landcover, _=>targetMetadata)
```

## Write

### `saveAsGeoTiff`
- Purpose: Persist raster results to GeoTIFF format.
- Input: `RasterRDD[T], outputPath`
- Output: GeoTIFF on disk
- Note: For advanced GeoTIFF writing options (for example compression, write mode, bit compaction), additional imports are required:
```scala
import edu.ucr.cs.bdlab.raptor.GeoTiffWriter
import edu.ucr.cs.bdlab.beast.io.tiff.TiffConstants
```

```scala
small.saveAsGeoTiff("glc_small")
```

#### Advanced write options

- Purpose: Control output compression, layout, and bit representation.

```scala
raster.saveAsGeoTiff("temperature_f",
  GeoTiffWriter.Compression -> TiffConstants.COMPRESSION_LZW)
raster.saveAsGeoTiff("temperature_f",
  GeoTiffWriter.Compression -> TiffConstants.COMPRESSION_DEFLATE)
raster.saveAsGeoTiff("temperature_f",
  GeoTiffWriter.Compression -> TiffConstants.COMPRESSION_NONE)
```

```scala
raster.saveAsGeoTiff("temperature_f", GeoTiffWriter.WriteMode -> "distributed")
raster.saveAsGeoTiff("temperature_f", GeoTiffWriter.WriteMode -> "compatibility")
```

```scala
raster.saveAsGeoTiff(
  "temperature_f",
  Seq(
    GeoTiffWriter.Compression -> TiffConstants.COMPRESSION_LZW,
    GeoTiffWriter.WriteMode -> "compatibility"
  )
)
```

```scala
raster.saveAsGeoTiff("temperature_f", GeoTiffWriter.CompactBits -> true)
raster.saveAsGeoTiff("temperature_f", GeoTiffWriter.BitsPerSample -> "8,8,8")
```

- Note: `CompactBits` works with integer pixels only.
- Note: Use `"distributed"` mode when writing many files (for example after `explode`) for better throughput.
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
