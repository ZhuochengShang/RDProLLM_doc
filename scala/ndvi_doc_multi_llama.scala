// Here is the Scala code that translates the given Python script into Scala, targeting RDPro on Spark:

// ```scala
import edu.ucr.cs.bdlab.beast._

object NDVI {
  def run(sc: SparkContext): Unit = {
    val b4_path = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/data/landsat8/LA/B4/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B4.TIF"
    val b5_path = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/data/landsat8/LA/B5/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B5.TIF"
    val out_ndvi = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/python/ndvi.scala"

    // Open datasets
    val b4 = sc.hdfFile(b4_path, "B4")
    val b5 = sc.hdfFile(b5_path, "B5")

    assert(b4.isDefined && b5.isDefined, "Failed to open input files")

    // Read arrays
    val red = b4.get.mapPixels { pixel => pixel.floatValue }
    val nir = b5.get.mapPixels { pixel => pixel.floatValue }

    // Check grid alignment
    if (b4.get.metadata.gridToModelTransform != b5.get.metadata.gridToModelTransform ||
        b4.get.metadata.projection != b5.get.metadata.projection) {
      throw new RuntimeException("B4 and B5 grids do not match — warp one band first")
    }

    // Handle NoData
    val red_nodata = b4.get.band(1).noDataValue
    val nir_nodata = b5.get.band(1).noDataValue

    val mask = red == red_nodata || nir == nir_nodata

    // NDVI calculation
    val denom = nir + red
    val ndvi = if (denom == 0 || mask) -9999.0 else (nir - red) / denom

    // Create output GeoTIFF
    b4.get.overlay(b5).mapPixels { pixel =>
      if (pixel == -9999.0f) -9999.0f else ndvi(pixel)
    }.saveAsGeoTiff(out_ndvi, GeoTiffWriter.Compression -> TiffConstants.COMPRESSION_LZW)
  }
}


// Please note that this code uses the `beast_` package and requires Spark and RDPro to be installed.

val _r = NDVI.run(sc)
println("__DONE__")
System.exit(0)
