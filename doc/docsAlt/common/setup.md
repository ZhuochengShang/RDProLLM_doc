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

## Setup

Before using RDPro, follow the [setup page](dev-setup.md) to configure your
project for Beast.

In Scala, import Beast features:

```scala
import edu.ucr.cs.bdlab.beast._
```