import edu.ucr.cs.bdlab.beast._
import edu.ucr.cs.bdlab.raptor.GeoTiffWriter
import edu.ucr.cs.bdlab.beast.io.tiff.TiffConstants
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{ArrayType, FloatType, IntegerType}

import java.net.URI
import java.nio.file.Paths
import org.apache.hadoop.fs.{FileSystem, Path}

object NDVIFromAnalyticMS {
  // Indices are 0-based for RDPro Arrays (Planet AnalyticMS: Red=3, NIR=4 in GDAL 1-based)
  private val RedBandIndex0: Int = 2
  private val NirBandIndex0: Int = 3
  private val NdviNoData: Float = -9999.0f

  def run(sc: SparkContext): Unit = {
    // Safe defaults (from the provided Python)
    val defaultIn = "/Users/clockorangezoe/Desktop/PlanetAPI/example/20230527_180231_61_247b_3B_AnalyticMS.tif"
    val defaultOut = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLM_doc/output/ndvi_band.v3.tif"

    // Try to read two args from SparkConf (optional), else use defaults
    // Expected: spark-submit ... --conf rdpro.args="<inputPath>|<outputPath>"
    val argStr = sc.getConf.get("rdpro.args", s"$defaultIn|$defaultOut")
    val parts = argStr.split("\\|", -1).map(_.trim)
    val inPathRaw = parts.headOption.getOrElse(defaultIn)
    val outPathRaw = parts.lift(1).getOrElse(defaultOut)

    // Normalize I/O paths per environment rules
    val inputPath = normalizePath(sc, inPathRaw)
    val desiredOutPath = normalizePath(sc, outPathRaw)

    // Resolve final output path and ensure parent directory exists
    val (finalOutPathStr, fs) = prepareOutputPath(sc, desiredOutPath)

    // Probe pixel type to branch into the correct RDPro loader
    val probe = sc.geoTiff(inputPath)
    val pixelType = probe.first.pixelType

    pixelType match {
      case ArrayType(IntegerType, _) =>
        val mb: RasterRDD[Array[Int]] = sc.geoTiff[Array[Int]](inputPath)
        val ndvi: RasterRDD[Float] = mb.mapPixels((bands: Array[Int]) => {
          if (bands == null || bands.length <= NirBandIndex0 || bands.length <= RedBandIndex0)
            throw new RuntimeException(s"Raster does not contain required bands: need at least index $NirBandIndex0 (0-based). Found length=${if (bands==null) 0 else bands.length}")
          val red: Float = bands(RedBandIndex0).toFloat
          val nir: Float = bands(NirBandIndex0).toFloat
          val denom: Float = nir + red
          if (denom == 0.0f) NdviNoData else (nir - red) / denom
        })
        // Write single-file GeoTIFF (compatibility) with LZW compression
        ndvi.saveAsGeoTiff(finalOutPathStr,
          Seq(
            GeoTiffWriter.Compression -> TiffConstants.COMPRESSION_LZW,
            GeoTiffWriter.WriteMode -> "compatibility"
          )
        )

      case ArrayType(FloatType, _) =>
        val mb: RasterRDD[Array[Float]] = sc.geoTiff[Array[Float]](inputPath)
        val ndvi: RasterRDD[Float] = mb.mapPixels((bands: Array[Float]) => {
          if (bands == null || bands.length <= NirBandIndex0 || bands.length <= RedBandIndex0)
            throw new RuntimeException(s"Raster does not contain required bands: need at least index $NirBandIndex0 (0-based). Found length=${if (bands==null) 0 else bands.length}")
          val red: Float = bands(RedBandIndex0)
          val nir: Float = bands(NirBandIndex0)
          if (java.lang.Float.isNaN(red) || java.lang.Float.isNaN(nir)) {
            NdviNoData
          } else {
            val denom: Float = nir + red
            if (denom == 0.0f) NdviNoData else (nir - red) / denom
          }
        })
        // Write single-file GeoTIFF (compatibility) with LZW compression
        ndvi.saveAsGeoTiff(finalOutPathStr,
          Seq(
            GeoTiffWriter.Compression -> TiffConstants.COMPRESSION_LZW,
            GeoTiffWriter.WriteMode -> "compatibility"
          )
        )

      case IntegerType | FloatType =>
        throw new IllegalArgumentException(
          s"Expected a multi-band raster (Array pixel type) to compute NDVI. Found scalar pixel type: $pixelType"
        )

      case other =>
        throw new IllegalArgumentException(s"Unsupported pixel type for this operation: $other")
    }

    println(s"NDVI written to: $finalOutPathStr")
  }

  // ----------------- Helpers -----------------

  private def normalizePath(sc: SparkContext, path: String): String = {
    if (path == null || path.isEmpty) return path
    val isLocalMode: Boolean = Option(sc.master).exists(_.toLowerCase.startsWith("local"))
    val uri = try new URI(path) catch { case _: Throwable => null }
    val hasScheme = uri != null && uri.getScheme != null

    if (hasScheme) {
      path
    } else {
      if (isLocalMode && looksLikeLocalAbsolute(path)) {
        Paths.get(path).toAbsolutePath.toUri.toString
      } else {
        // Leave scheme-less path unchanged to resolve against cluster filesystem
        path
      }
    }
  }

  private def looksLikeLocalAbsolute(p: String): Boolean = {
    if (p == null) return false
    val s = p.trim
    s.startsWith("/") || s.matches("^[a-zA-Z]:\\\\.*")
  }

  private def prepareOutputPath(sc: SparkContext, desiredOutPath: String): (String, FileSystem) = {
    val outURI = new URI(desiredOutPath)
    val conf = sc.hadoopConfiguration
    val fs = FileSystem.get(outURI, conf)
    var outPath = new Path(outURI)

    // If a file already exists at the target path, append ".tif" to avoid overwrite
    if (fs.exists(outPath) && fs.isFile(outPath)) {
      val parent = Option(outPath.getParent).getOrElse(new Path(new URI(outURI.getScheme + ":///")))
      val name = outPath.getName
      outPath = new Path(parent, name + ".tif")
    }

    // Ensure parent directory exists
    val parent = Option(outPath.getParent).getOrElse(new Path(new URI(outURI.getScheme + ":///")))
    if (!fs.exists(parent)) {
      fs.mkdirs(parent)
    }

    (outPath.toString, fs)
  }
}

// NOTES
// - RDPro APIs used:
//   geoTiff[T], mapPixels, saveAsGeoTiff
// - Unsupported operations and why:
//   - Per-band NoData read/write: DOC does not expose band-level NoData retrieval or setting on output.
//     We approximate masking only for Float arrays by treating NaN as NoData and always guard against zero denominator.
//     Output NoData value (-9999.0) is used in pixel values but cannot be registered in GeoTIFF tags via DOC APIs.
//   - Explicit band metadata (names/count) is not exposed in DOC; we assume 0-based band indexing within Array[T] and
//     validate band count at execution time inside mapPixels.
// - Assumptions about IO paths / bands / nodata / CRS / environment detection logic:
//   - Input is a multi-band Planet AnalyticMS GeoTIFF where Red is band index 2 and NIR is index 3 (0-based).
//   - If denominator (NIR+Red) is zero, NDVI is set to -9999.0f.
//   - In local Spark mode, absolute paths without a scheme are promoted to file:/// URIs to avoid Hadoop misinterpretation.
//   - For non-local Spark or scheme-less inputs, paths are left unchanged to resolve via fs.defaultFS.
//   - Output uses GeoTiffWriter.WriteMode -> "compatibility" to produce a single GIS-compatible file,
//     with LZW compression. Parent directory is created if missing; if a file exists at target path,
//     ".tif" is appended to avoid overwrite.

val _r = NDVIFromAnalyticMS.run(sc)
println("__DONE__ object=NDVIFromAnalyticMS")
System.exit(0)
