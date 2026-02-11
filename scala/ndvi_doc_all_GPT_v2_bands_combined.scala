import edu.ucr.cs.bdlab.beast._
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{ArrayType, FloatType, IntegerType}

import java.net.URI
import java.nio.file.{Files, Paths}
import scala.util.Try

import edu.ucr.cs.bdlab.raptor.GeoTiffWriter
import edu.ucr.cs.bdlab.beast.io.tiff.TiffConstants

object NDVIFromPlanet {
  def run(sc: SparkContext): Unit = {
    // Defaults from the Python script
    val defaultInput = "/Users/clockorangezoe/Desktop/PlanetAPI/example/20230527_180231_61_247b_3B_AnalyticMS.tif"
    val defaultOutput = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLM_doc/output/ndvi_band.v2.tif"

    // Allow overrides via SparkConf keys for convenience
    val inputRaw = sc.getConf.get("rdpro.input", defaultInput)
    val outputRaw = sc.getConf.get("rdpro.output", defaultOutput)

    // Path normalization per rules
    val inputPath = normalizePath(sc, inputRaw)
    val preparedOutputPath = prepareOutputPath(sc, outputRaw)

    // Planet AnalyticMS bands (1-based): Red=3, NIR=4
    val redBand1Based = 3
    val nirBand1Based = 4
    val redIdx = redBand1Based - 1
    val nirIdx = nirBand1Based - 1
    val maxIdx = math.max(redIdx, nirIdx)

    // Compute NDVI from Array[Float] pixels
    val ndviFromArrayFloat = (px: Array[Float]) => {
      if (px == null || px.length <= maxIdx) {
        -9999.0f
      } else {
        val red = px(redIdx)
        val nir = px(nirIdx)
        val denom = nir + red
        if (java.lang.Float.isNaN(red) || java.lang.Float.isNaN(nir) || denom == 0.0f) -9999.0f
        else (nir - red) / denom
      }
    }: Float

    // Probe pixel type to choose correct loader
    val probe = sc.geoTiff(inputPath)
    val pixelType = probe.first.pixelType

    pixelType match {
      case ArrayType(IntegerType, _) =>
        val rasterIntArr = sc.geoTiff[Array[Int]](inputPath)
        val rasterFloatArr = rasterIntArr.mapPixels((px: Array[Int]) => {
          val out = new Array[Float](px.length)
          var i = 0
          while (i < px.length) { out(i) = px(i).toFloat; i += 1 }
          out
        })
        val ndvi = rasterFloatArr.mapPixels(ndviFromArrayFloat)
        ndvi.saveAsGeoTiff(preparedOutputPath,
          Seq(GeoTiffWriter.Compression -> TiffConstants.COMPRESSION_LZW))

      case ArrayType(FloatType, _) =>
        val rasterFloatArr = sc.geoTiff[Array[Float]](inputPath)
        val ndvi = rasterFloatArr.mapPixels(ndviFromArrayFloat)
        ndvi.saveAsGeoTiff(preparedOutputPath,
          Seq(GeoTiffWriter.Compression -> TiffConstants.COMPRESSION_LZW))

      case ArrayType(_, _) =>
        throw new IllegalArgumentException("Unsupported multi-band pixel element type; expected integer or float array bands")

      case IntegerType | FloatType =>
        throw new IllegalArgumentException("Input must be a multi-band raster (expected Red and NIR bands). Got single-band raster.")

      case other =>
        throw new IllegalArgumentException(s"Unsupported pixel type: $other")
    }
  }

  // ----------------- Helpers: Filesystem & path normalization -----------------

  private def isLocal(sc: SparkContext): Boolean =
    Option(sc.master).exists(_.toLowerCase.startsWith("local"))

  private def hasScheme(p: String): Boolean =
    Try(new URI(p)).toOption.exists(_.getScheme != null)

  private def looksLikeLocalPath(p: String): Boolean = {
    val windowsDrive = "^[a-zA-Z]:[\\\\/].*".r
    p.startsWith("/") || windowsDrive.pattern.matcher(p).matches()
  }

  // Normalize any input/output path before calling RDPro IO
  private def normalizePath(sc: SparkContext, raw: String): String = {
    if (raw == null || raw.trim.isEmpty) raw
    else {
      if (hasScheme(raw)) {
        raw
      } else if (isLocal(sc) && looksLikeLocalPath(raw)) {
        Paths.get(raw).toAbsolutePath.normalize().toUri.toString
      } else {
        // In non-local mode, leave scheme-less paths unchanged to resolve against fs.defaultFS
        raw
      }
    }
  }

  // Output path handling per rules:
  // - Treat output as a directory; create it if missing (local only)
  // - If a file already exists at the target output path, append ".tif" to avoid overwrite (local only)
  private def prepareOutputPath(sc: SparkContext, rawOut: String): String = {
    val norm = normalizePath(sc, rawOut)
    val local = isLocal(sc)
    val uri = Try(new URI(norm)).getOrElse(null)
    val scheme = if (uri != null) uri.getScheme else null

    // Handle local filesystem only
    if (local && (scheme == null || scheme.equalsIgnoreCase("file"))) {
      val localPath =
        if (scheme == null) Paths.get(norm).toAbsolutePath.normalize()
        else Paths.get(uri)

      if (Files.exists(localPath) && Files.isRegularFile(localPath)) {
        // Append .tif to avoid overwrite of an existing file
        val appended = localPath.toString + ".tif"
        Paths.get(appended).toAbsolutePath.normalize().toUri.toString
      } else {
        // Treat as directory and create if missing
        Try(Files.createDirectories(localPath)).getOrElse(localPath)
        if (scheme == null) localPath.toUri.toString else norm
      }
    } else {
      // Cannot reliably check/create remote FS here; pass through
      norm
    }
  }
}

// NOTES
// - RDPro APIs used:
//   // geoTiff, mapPixels, saveAsGeoTiff
// - Unsupported operations and why:
//   // Reading per-band NoData values and propagating them: Not available in the provided DOC; NDVI masks only denom==0 and NaN values.
//   // Setting output NoData value in GeoTIFF: Not exposed in the provided write options; cannot set -9999 as NoData in metadata.
//   // Explicit band name/description inspection: Not available; we assume Planet convention Red=3, NIR=4.
// - Assumptions:
//   // Input is a multi-band GeoTIFF where bands are ordered so that Red is band 3 and NIR is band 4 (1-based).
//   // If band count is insufficient, pixels output -9999.0f for robustness without driver-side sampling.
//   // Path normalization: local mode detected via sc.master starts with "local"; scheme-less local absolute paths are converted to file:/// URIs.
//   // For non-local Spark, scheme-less paths are left unchanged to resolve via Hadoop fs.defaultFS.
//   // Output path is treated as a directory. In local mode, the directory is created if missing. If a regular file exists at that exact path, ".tif" is appended to avoid overwrite.

val _r = NDVIFromPlanet.run(sc)
println("__DONE__ object=NDVIFromPlanet")
System.exit(0)
