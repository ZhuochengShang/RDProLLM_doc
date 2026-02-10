## Raster writing
Any raster RDD can be written as raster files, e.g., GeoTIFF.
The following example reads an HDF file, applies some processing, and write the output as GeoTIFF.

Hint: In RDPro, raster output follows Spark’s distributed write semantics. The output path must always be a directory (folder), not a single file path. Internally, Spark creates temporary and part files under this directory before producing the final raster output.

```scala
val temperatureK: RasterRDD[Float] =
  sc.hdfFile("MOD11A1.A2022173.h08v05.006.2022174092443.hdf", "LST_Day_1km")
val temperatureF: RasterRDD[Float] = temperatureK.mapPixels(k => (k-273.15f) * 9 / 5 + 32)
temperatureF.saveAsGeoTiff("temperature_f")
```

Below are some additional options you can use when writing files.

For advanced GeoTIFF writing options (e.g., compression, write mode, bit compaction),
additional imports are required:
```scala
import edu.ucr.cs.bdlab.raptor.GeoTiffWriter
import edu.ucr.cs.bdlab.beast.io.tiff.TiffConstants
```

- Compression: You can choose between three types of compression {LZW, Deflate, and None} as follows.
```scala
raster.saveAsGeoTiff("temperature_f", GeoTiffWriter.Compression -> TiffConstants.COMPRESSION_LZW)
raster.saveAsGeoTiff("temperature_f", GeoTiffWriter.Compression -> TiffConstants.COMPRESSION_DEFLATE)
raster.saveAsGeoTiff("temperature_f", GeoTiffWriter.Compression -> TiffConstants.COMPRESSION_NONE)
```
- Write mode: A raster RDD can be written in distributed or compatibility mode.
In distributed mode, each RDD partition is written to a separate file.
These files can be read back and processes by Beast but traditional GIS software might not be able to process it.
If you use compatibility mode, the entire raster RDD is written as a single file that is compatible with
traditional GIS software.
```scala
raster.saveAsGeoTiff("temperature_f", GeoTiffWriter.WriteMode -> "distributed")
raster.saveAsGeoTiff("temperature_f", GeoTiffWriter.WriteMode -> "compatibility")
```
Hint: Use Seq to pass multiple options.
```scala
raster.saveAsGeoTiff("temperature_f", Seq(GeoTiffWriter.Compression -> TiffConstants.COMPRESSION_LZW GeoTiffWriter.WriteMode -> "compatibility"))
```

Hint: If you expect a large number of files to be written, e.g., after applying the explode method,
the distributed writing mode is recommended for more efficiency.
- Bit compaction: In GeoTIFF, values can be bit-compacted to use as few bits as possible.
To do that, Beast will need to first scan all the data to determine the maximum value
and use it to calculate the minimal bit representation of values. The following example uses this feature.
```scala
raster.saveAsGeoTiff("temperature_f", GeoTiffWriter.CompactBits -> true)
```
This feature works only with integer pixel values. Float values are always represented in 32-bits.
- BitsPerSample: If you want to use the bit compaction feature without the overhead
of scanning the entire dataset to find the maximum, you can directly set the
number of bits per sample (band) in the raster data. Note that Beast will use these values
without checking their validity so you must be sure that the values are correct.
If unsure, let Beast find out or disable the bit compaction feature.
In the following example, we assume that we write a file with three bands, RGB,
each taking 8 bits.
```scala
raster.saveAsGeoTiff("temperature_f", GeoTiffWriter.BitsPerSample -> "8,8,8")
```