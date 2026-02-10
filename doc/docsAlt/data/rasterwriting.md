## Writing Raster Outputs (GeoTIFF)

Writing rasters follows the same model as rasterio write profiles.

Hint: In RDPro, raster output follows Spark’s distributed write semantics. The output path must always be a directory (folder), not a single file path. Internally, Spark creates temporary and part files under this directory before producing the final raster output.

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