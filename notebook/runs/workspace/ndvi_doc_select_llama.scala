Here is a Scala translation of the Python script using only the RDPro APIs as described in the documentation:

```scala
import edu.ucr.cs.bdlab.raptor.RDPro
import edu.ucr.cs.bdlab.beast.io.spark.SparkRDD
import edu.ucr.cs.bdlab.beast.core.Raster

object NDVI {
  def main(args: Array[String]): Unit = {
    // Load Red and NIR raster data from files
    val redRaster = RDPro.rasterFromGeoTIFF("/path/to/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B4.TIF")
    val nirRaster = RDPro.rasterFromGeoTIFF("/path/to/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B5.TIF")

    // Calculate NDVI
    val ndviPixels = redRaster.mapPixels { (red, _) =>
      val nir = nirRaster.readPixel(_, _)
      if (nir == 0) -9999.0 else ((nir - red) / (nir + red))
    }

    // Write NDVI raster to GeoTIFF
    ndviPixels.saveAsGeoTiff("/path/to/ndvi.tif", writeMode = "distributed")
  }
}
```

Note: You will need to replace the file paths with actual file locations on your system. Also, this code assumes that you have RDPro and Spark installed and configured correctly.