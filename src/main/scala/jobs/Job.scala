package jobs

import edu.ucr.cs.bdlab.beast._
import edu.ucr.cs.bdlab.raptor.GeoTiffWriter
import edu.ucr.cs.bdlab.beast.io.tiff.TiffConstants

object Job {
  val B4_PATH: String = "/content/B4/LC09_L2SP_040036_20250803_20250804_02_T1_SR_B4.TIF"
  val B5_PATH: String = "/content/B5/LC09_L2SP_040036_20250803_20250804_02_T1_SR_B5.TIF"
  val OUT_NDVI: String = "/content/ndvi.tif"

  def run(sc: org.apache.spark.SparkContext): Unit = {
    val red: RasterRDD[Float] = sc.geoTiff[Float](B4_PATH)
    val nir: RasterRDD[Float] = sc.geoTiff[Float](B5_PATH)

    val stacked: RasterRDD[Array[Float]] = red.overlay(nir)
    val ndvi: RasterRDD[Float] = stacked.mapPixels((px: Array[Float]) => {
      val r: Float = px(0)
      val n: Float = px(1)
      val denom: Float = n + r
      if (denom == 0f) -9999.0f else (n - r) / denom
    })

    ndvi.saveAsGeoTiff(
      OUT_NDVI,
      GeoTiffWriter.Compression -> TiffConstants.COMPRESSION_LZW,
      GeoTiffWriter.WriteMode -> "compatibility"
    )
  }
}

/*
NOTES
(a) RDPro APIs used: geoTiff, overlay, mapPixels, saveAsGeoTiff
(b) Unsupported operations and why: NoData read/propagation (GetNoDataValue) and explicit grid alignment checks are not in doc chunks; GeoTIFF TILED option is not in doc chunks
(c) Assumptions about IO paths / bands / nodata: B4/B5 paths are valid GeoTIFFs with matching metadata; single band per file; pixels are Float; nodata handled only by denom==0 sentinel; output written as compatibility GeoTIFF at OUT_NDVI
*/
