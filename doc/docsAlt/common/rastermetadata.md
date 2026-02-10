### `RasterMetadata` — dataset-level metadata

`RasterMetadata` holds the same information as a GDAL dataset’s spatial metadata:

```scala
@param x1, y1, x2, y2   Geographic extent
@param tileWidth        Tile width (pixels)
@param tileHeight       Tile height (pixels)
@param srid             CRS (e.g., EPSG:4326)
@param g2m              Grid-to-world affine transform
```