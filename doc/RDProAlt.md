# Raster Processing with RDPro (GDAL / rasterio–style)

Raster data is one of the most common formats for geospatial analysis.  
In libraries such as **GDAL** and **rasterio**, raster datasets are treated as
**multidimensional grids (images)** with spatial metadata such as coordinate
reference systems (CRS), resolution, and geographic extent.

**RDPro (Raster Distributed Processor)** follows the same conceptual model as
GDAL and rasterio, but executes raster operations **in parallel on Apache Spark**.
If you are familiar with GDAL or rasterio, you can think of RDPro as providing
the same raster operations—opening datasets, raster math, reprojection,
resampling, stacking, and writing outputs—at distributed scale.

This page describes the basic raster processing functionality provided by RDPro
in Beast.

---

## Setup

Before using RDPro, follow the [setup page](dev-setup.md) to configure your
project for Beast.

In Scala, import Beast features:

```scala
import edu.ucr.cs.bdlab.beast._
```

---

## Raster Data Model (GDAL-style concepts)

RDPro explicitly models the same spatial concepts used implicitly in GDAL and
rasterio.

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

### `RasterMetadata` — dataset-level metadata

`RasterMetadata` holds the same information as a GDAL dataset’s spatial metadata:

```scala
@param x1, y1, x2, y2   Geographic extent
@param tileWidth        Tile width (pixels)
@param tileHeight       Tile height (pixels)
@param srid             CRS (e.g., EPSG:4326)
@param g2m              Grid-to-world affine transform
```

---

### `RasterRDD[T]`

`RasterRDD[T]` is an alias for `RDD[ITile[T]]` and represents a **distributed
raster dataset**, analogous to a GDAL or rasterio dataset, but partitioned across
Spark.

All examples below assume an initialized `SparkContext` named `sc`.

---

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

## Pixel-wise Raster Operations

Pixel-level operations correspond to **element-wise raster math** in NumPy or
rasterio.

### MapPixels (element-wise transform)

```scala
val temperatureF: RasterRDD[Float] =
  temperatureK.mapPixels(k => (k - 273.15f) * 9 / 5 + 32)
```

This produces an output raster with the same shape and metadata.

---

### FilterPixels (masking)

Equivalent to masking raster values based on a condition:

```scala
temperatureK
  .filterPixels(_ > 300)
  .saveAsGeoTiff("temperature_high")
```

---

### Flatten (raster → samples)

`flatten` extracts all non-empty pixels into a non-raster RDD, similar to
converting a raster into point samples for global statistics.

```scala
val histogram: Map[Int, Long] =
  raster.flatten.map(_._4).countByValue().toMap
```

Each output record contains:
- Pixel `(x, y)` indices
- Raster metadata
- Pixel value

---

## Stacking Rasters (Multi-band Datasets)

In GDAL, multiple rasters can be combined into a **multi-band dataset**.  
RDPro provides the same functionality via `overlay`.

```scala
val stacked: RasterRDD[Array[Int]] =
  raster1.overlay(raster2)
```

All input rasters must have the same CRS, resolution, and tile size.

---

## Reshaping Rasters (GDAL Warp–style Operations)

### Retile (change block layout)

```scala
val retiled = raster.retile(64, 64)
retiled.saveAsGeoTiff("glc_retiled")
```

---

### Explode (tile-wise outputs)

Useful when each tile should be written as a separate file (e.g., ML pipelines):

```scala
raster
  .retile(64, 64)
  .explode
  .saveAsGeoTiff("glc_retiled", GeoTiffWriter.WriteMode -> "distributed")
```

---

### Reproject (change CRS)

Equivalent to `gdal.Warp` with a new CRS:

```scala
val temperature: RasterRDD[Float] =
  sc.hdfFile("MOD11A1.A2022173.h08v05.006.2022174092443.hdf", "LST_Day_1km")

temperature.reproject(4326)
```

---

### Rescale (change resolution)

Equivalent to GDAL resampling:

```scala
val rescaled = raster.rescale(360, 180)
rescaled.saveAsGeoTiff("glc_small", GeoTiffWriter.WriteMode -> "compatibility")
```

---

### Reshape (general warp)

The most general operation for changing CRS, extent, resolution, and tile size
in one step:

```scala
val reshaped =
  RasterOperationsFocal.reshapeNN(
    raster,
    RasterMetadata.create(-124, 42, -114, 32, 4326, 1000, 1000, 100, 100)
  )

reshaped.saveAsGeoTiff("glc_ca")
```

Supported interpolation methods:
- Nearest Neighbor (`reshapeNN`)
- Average (`reshapeAverage`)

---

## Raster–Vector Analysis (Raptor)

Raptor enables joint raster–vector processing, similar to zonal statistics in
GDAL.

```scala
val raster: RasterRDD[Int] = sc.geoTiff[Int]("glc2000_v1_1.tif")
val trees = raster.filterPixels(lc => lc >= 1 && lc <= 10)
val countries = sc.shapefile("ne_10m_admin_0_countries.zip")

val result =
  RaptorJoin.raptorJoinFeature(trees, countries, Seq())
    .map(x => x.feature.getAs[String]("NAME"))
    .countByValue()
    .toMap
println(result)
```

---

## Writing Raster Outputs (GeoTIFF)

Writing rasters follows the same model as rasterio write profiles.

```scala
val temperatureK: RasterRDD[Float] =
  sc.hdfFile("MOD11A1.A2022173.h08v05.006.2022174092443.hdf", "LST_Day_1km")
val temperatureF: RasterRDD[Float] =
  temperatureK.mapPixels(k => (k-273.15f) * 9 / 5 + 32)
temperatureF.saveAsGeoTiff("temperature_f")
```

### Advanced GeoTIFF Options

For advanced options (compression, write mode, bit compaction), add imports:

```scala
import edu.ucr.cs.bdlab.raptor.GeoTiffWriter
import edu.ucr.cs.bdlab.beast.io.tiff.TiffConstants
```

#### Compression

```scala
raster.saveAsGeoTiff("temperature_f",
  GeoTiffWriter.Compression -> TiffConstants.COMPRESSION_LZW)

raster.saveAsGeoTiff("temperature_f",
  GeoTiffWriter.Compression -> TiffConstants.COMPRESSION_DEFLATE)

raster.saveAsGeoTiff("temperature_f",
  GeoTiffWriter.Compression -> TiffConstants.COMPRESSION_NONE)
```

#### Write mode

Distributed mode writes one file per Spark partition (fast, Beast-friendly).  
Compatibility mode writes a single GeoTIFF compatible with traditional GIS tools.

```scala
raster.saveAsGeoTiff("temperature_f",
  GeoTiffWriter.WriteMode -> "distributed")

raster.saveAsGeoTiff("temperature_f",
  GeoTiffWriter.WriteMode -> "compatibility")
```

Hint: Use `Seq` to pass multiple options.

```scala
raster.saveAsGeoTiff("temperature_f",
  Seq(
    GeoTiffWriter.Compression -> TiffConstants.COMPRESSION_LZW,
    GeoTiffWriter.WriteMode -> "compatibility"
  )
)
```

#### Bit compaction

```scala
raster.saveAsGeoTiff("temperature_f", GeoTiffWriter.CompactBits -> true)
```

#### BitsPerSample

```scala
raster.saveAsGeoTiff("temperature_f", GeoTiffWriter.BitsPerSample -> "8,8,8")
```

---

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

---

## Takeaway

**RDPro implements the same raster operations as GDAL and rasterio**, but executes
them **at scale on Apache Spark**. This README intentionally uses GDAL-style
terminology so that both users and LLMs can immediately recognize the workflow.
