## Raster Data and Data Model Definitions
### Grid space
A grid space is defined as a two dimensional grid that consists of W columns and H rows with the
origin (0, 0) at the top-left corner.
Coordinates: Represented as (i,j) where i is the column and j is the row.
Origin: Always (0, 0) at the top-left corner.
Independence: The grid position does not depend on geography; it only cares about the image dimensions (W x H).

### Raster tile
The grid is broken down into tiles where each tile consists of tw columns and th rows, with the exception of the tiles at the last column
and/or the last row.
Each tile is associated with tile ID.

### World space
he world space represents a rectangular space on the Earth’s surface defined by four geographical coordinates (𝑥1,𝑦1) and (𝑥2,𝑦2) that define the space [𝑥1, 𝑥2 [×[𝑦1,𝑦2 [.

### Grid to World
G2W is a 2D affine transformation that transforms a point from the grid space to the
world space. 

### World to Grid
W2D is a 2D affine transformation that transforms a point from the world space to the grid space.

## RDPro Data Abstraction
ITile[T]: The atomic unit of processing in RDPro
```scala
@tparam T the type of measurement values in tiles
@param tileID the ID of this tile in the raster metadata
@param rasterMetadata the metadata of the underlying raster
@param rasterFeature (optional) any additional features that are associated with the raster layer, e.g., file name and time
```

RasterRDD[T]: An alias for RDD[ITile[T]]. It represents a distributed collection of raster tiles.
