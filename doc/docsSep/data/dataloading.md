## Raster data Loading
The first step for processing any dataset in Spark is loading it as an RDD.
RDPro currently supports both GeoTIFF and HDF as input files.
The following is an example of loading raster data in Beast.
Notice that the input can be either a single file or a directory with many files.
In both cases, the input will be loaded in a single RDD.

```scala
// Load GeoTiff file
val raster: RDD[ITile[Int]] = sc.geoTiff("glc2000_v1_1.tif")
// Or using the RasterRDD alias
val raster: RasterRDD[Int] = sc.geoTiff("glc2000_v1_1.tif")
// Load HDF file
val temperatureK: RasterRDD[Float] = sc.hdfFile("MOD11A1.A2022173.h08v05.006.2022174092443.hdf", "LST_Day_1km")
```

*Note*: You can download this file at [this link](https://forobs.jrc.ec.europa.eu/products/glc2000/products.php).

The type parameter `[Int]` indicates that each pixel in the GeoTIFF file contains a single integer value.
This must match the actual contents of the file to work correctly. If you are not sure what type the file contains,
the following code example could help you figure it out.

```scala
val raster = sc.geoTiff("glc2000_v1_1.tif")
println(raster.first.pixelType)
```

In this case, it will print
```
IntegerType
```
The possibilities are:

| Type                        | Loading statement          |
|-----------------------------|----------------------------|
| IntegerType                 | `sc.geoTiff[Int]`          |
| FloatType                   | `sc.geoTiff[Float]`        |
| ArrayType(IntegerType,true) | `sc.geoTiff[Array[Int]]`   |
| ArrayType(FloatType, true)  | `sc.geoTiff[Array[Float]]` |
