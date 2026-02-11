import edu.ucr.cs.bdlab.beast._
import edu.ucr.cs.bdlab.raptor.GeoTiffWriter
import edu.ucr.cs.bdlab.beast.io.tiff.TiffConstants
import org.apache.spark.SparkContext
import org.apache.hadoop.fs.{FileSystem, Path => HPath}
import java.net.URI
import java.nio.file.{Paths => JPaths}

object ComputeNDVI {
  def run(sc: SparkContext): Unit = {
    // ---- Helper: path normalization per rules ----
    def hasScheme(p: String): Boolean = {
      try {
        val u = new URI(p)
        u.getScheme != null
      } catch {
        case _: Throwable => false
      }
    }
    def isWindowsAbs(p: String): Boolean = p.matches("^[a-zA-Z]:\\\\.*")
    def isUnixAbs(p: String): Boolean = p.startsWith("/")

    def normalizePath(sc: SparkContext, p: String): String = {
      if (p == null || p.trim.isEmpty) p
      else {
        val isLocal = Option(sc.master).getOrElse("").toLowerCase.startsWith("local")
        if (hasScheme(p)) p
        else if (isLocal && (isUnixAbs(p) || isWindowsAbs(p))) {
          JPaths.get(p).toAbsolutePath.normalize().toUri.toString // file:///
        } else {
          p // leave as-is to resolve against cluster FS (fs.defaultFS)
        }
      }
    }

    def ensureParentAndResolveOutput(sc: SparkContext, outRaw: String): String = {
      var out = outRaw
      val uri = new URI(out)
      val hconf = sc.hadoopConfiguration
      val fs: FileSystem =
        if (uri.getScheme == null) FileSystem.get(hconf) else FileSystem.get(uri, hconf)
      val p = new HPath(out)
      val parent = p.getParent
      if (parent != null) {
        fs.mkdirs(parent)
      }
      // If a file already exists exactly at the output path, append ".tif" to avoid overwrite
      if (fs.exists(p) && fs.isFile(p)) {
        out = out + ".tif"
      }
      out
    }

    // ---- Inputs (allow overrides via system properties or Spark conf) ----
    val defaultIn = "/Users/clockorangezoe/Desktop/PlanetAPI/example/20230527_180231_61_247b_3B_AnalyticMS.tif"
    val defaultOut = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLM_doc/output/ndvi_band.all.tif"
    def getOpt(keys: Seq[String], default: String): String = {
      keys.foldLeft(Option.empty[String]) { (acc, k) =>
        acc.orElse(Option(System.getProperty(k))).orElse(sc.getConf.getOption(s"spark.$k"))
      }.getOrElse(default)
    }
    val inputRaw = getOpt(Seq("input", "in"), defaultIn)
    val outputRaw = getOpt(Seq("output", "out"), defaultOut)
    val redBand: Int = getOpt(Seq("redBand", "red"), "3").toInt
    val nirBand: Int = getOpt(Seq("nirBand", "nir"), "4").toInt
    require(redBand >= 1 && nirBand >= 1, s"Band indices must be 1-based positive integers. Got red=$redBand nir=$nirBand")

    val inputPath = normalizePath(sc, inputRaw)
    val outputPath0 = normalizePath(sc, outputRaw)
    val outputPath = ensureParentAndResolveOutput(sc, outputPath0)

    // ---- Probe pixel type to decide how to read multi-band ----
    val probe = sc.geoTiff(inputPath)
    val pixelTypeStr = probe.first.pixelType.toString // e.g., "ArrayType(IntegerType,true)" or "ArrayType(FloatType,true)"

    val redIdx = redBand - 1
    val nirIdx = nirBand - 1
    val noDataOut: Float = -9999.0f

    // Compute NDVI for Array[Float]
    def ndviFromFloats(p: Array[Float]): Float = {
      if (p == null || p.length <= math.max(redIdx, nirIdx)) noDataOut
      else {
        val r = p(redIdx)
        val n = p(nirIdx)
        val denom = n + r
        if (java.lang.Float.isNaN(r) || java.lang.Float.isNaN(n) || denom == 0.0f) noDataOut
        else (n - r) / denom
      }
    }
    // Compute NDVI for Array[Int]
    def ndviFromInts(p: Array[Int]): Float = {
      if (p == null || p.length <= math.max(redIdx, nirIdx)) noDataOut
      else {
        val r = p(redIdx).toFloat
        val n = p(nirIdx).toFloat
        val denom = n + r
        if (denom == 0.0f) noDataOut else (n - r) / denom
      }
    }

    // ---- Load with correct type and compute NDVI ----
    if (pixelTypeStr.contains("ArrayType(FloatType")) {
      val raster: RasterRDD[Array[Float]] = sc.geoTiff[Array[Float]](inputPath)
      val ndvi: RasterRDD[Float] = raster.mapPixels((p: Array[Float]) => ndviFromFloats(p))
      ndvi.saveAsGeoTiff(outputPath,
        Seq(
          GeoTiffWriter.Compression -> TiffConstants.COMPRESSION_LZW,
          GeoTiffWriter.WriteMode -> "compatibility"
        ))
    } else if (pixelTypeStr.contains("ArrayType(IntegerType")) {
      val raster: RasterRDD[Array[Int]] = sc.geoTiff[Array[Int]](inputPath)
      val ndvi: RasterRDD[Float] = raster.mapPixels((p: Array[Int]) => ndviFromInts(p))
      ndvi.saveAsGeoTiff(outputPath,
        Seq(
          GeoTiffWriter.Compression -> TiffConstants.COMPRESSION_LZW,
          GeoTiffWriter.WriteMode -> "compatibility"
        ))
    } else {
      throw new RuntimeException(
        s"Unsupported pixel type for NDVI computation: $pixelTypeStr. Expected a multi-band raster (Array[Float] or Array[Int])."
      )
    }

    println(s"NDVI written to: $outputPath")
  }
}

// NOTES
// - RDPro APIs used:
//   sc.geoTiff, mapPixels, saveAsGeoTiff, RasterRDD alias
// - Unsupported operations and why:
//   - Per-band NoData reading/masking is not available in the provided DOC; NDVI masks only NaN and denom==0,
//     but cannot honor integer NoData values from source bands.
//   - Setting GeoTIFF NoData on output is not exposed in the provided writer options; thus the -9999.0 flag is written as data.
//   - Inspecting band names/metadata or exact band count is not available; out-of-range band indices yield NoData output per-pixel.
// - Assumptions:
//   - Multi-band GeoTIFF loads as Array[Int] or Array[Float]; band indices are 1-based (as in the Python) and are converted to 0-based.
//   - Output is written as a single-file GeoTIFF using WriteMode "compatibility"; parent directory is created if missing.
//   - Path normalization follows rules:
//     - If path has a scheme, use as-is.
//     - If no scheme and Spark is in local mode and the path looks like local absolute, convert to file:/// URI.
//     - If not local and no scheme, leave unchanged to resolve against cluster fs.defaultFS.
//   - If a file already exists exactly at the target output path, ".tif" is appended to avoid overwrite.

val _r = ComputeNDVI.run(sc)
println("__DONE__ object=ComputeNDVI")
System.exit(0)
