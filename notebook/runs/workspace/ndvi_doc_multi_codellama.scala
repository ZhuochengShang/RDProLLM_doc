[PYTHON]
import os
from pathlib import Path

import numpy as np
from PIL import Image

B4_PATH = "/path/to/landsat8/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B4.TIF"  # Red
B5_PATH = "/path/to/landsat8/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B5.TIF"  # NIR
OUT_NDVI = "/path/to/ndvi.tif"

# Open datasets
ds_red = gdal.Open(B4_PATH, gdal.GA_ReadOnly)
ds_nir = gdal.Open(B5_PATH, gdal.GA_ReadOnly)

assert ds_red and ds_nir, "Failed to open input files"

# Read arrays
red = np.array(Image.open(str(Path(B4_PATH).resolve().absolute())))
nir = np.array(Image.open(str(Path(B5_PATH).resolve().absolute())))

# Check grid alignment
if (
    ds_red.GetGeoTransform() != ds_nir.GetGeoTransform()
    or ds_red.GetProjection() != ds_nir.GetProjection()
):
    raise RuntimeError("B4 and B5 grids do not match — warp one band first")

# Handle NoData
red_nodata = ds_red.GetRasterBand(1).GetNoDataValue()
nir_nodata = ds_nir.GetRasterBand(1).GetNoDataValue()

mask = np.zeros(red.shape, dtype=bool)
if red_nodata is not None:
    mask |= (red == red_nodata)
if nir_nodata is not None:
    mask |= (nir == nir_nodata)

# NDVI calculation
denom = nir + red
ndvi = np.where(
    (denom == 0) | mask,
    -9999.0,
    (nir - red) / denom
).astype(np.float32)

# Create output GeoTIFF
driver = gdal.GetDriverByName("GTiff")
out_ds = driver.Create(
    OUT_NDVI,
    ds_red.RasterXSize,
    ds_red.RasterYSize,
    1,
    gdal.GDT_Float32,
    options=["TILED=YES", "COMPRESS=LZW"]
)

out_ds.SetGeoTransform(ds_red.GetGeoTransform())
out_ds.SetProjection(ds_red.GetProjection())

out_band = out_ds.GetRasterBand(1)
out_band.WriteArray(ndvi)
out_band.SetNoDataValue(-9999.0)
out_band.FlushCache()

# Cleanup
ds_red = None
ds_nir = None
out_ds = None

print("NDVI written to:", OUT_NDVI)
[/PYTHON]
[C++]
#include <iostream>
#include <opencv2/opencv.hpp>

int main() {
    // Paths to input images
    std::string b4Path = "/path/to/landsat8/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B4.TIF";  // Red
    std::string b5Path = "/path/to/landsat8/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B5.TIF";  // NIR
    std::string outNdviPath = "/path/to/ndvi.tif";

    // Open datasets
    cv::Mat dsRed = cv::imread(b4Path, cv::IMREAD_GRAYSCALE);
    cv::Mat dsNir = cv::imread(b5Path, cv::IMREAD_GRAYSCALE);

    // Check grid alignment
    if (dsRed.size() != dsNir.size() || dsRed.type() != dsNir.type()) {
        std::cout << "B4 and B5 grids do not match — warp one band first" << std::endl;
        return -1;
    }

    // Handle NoData
    double redNodata = 0.0;
    double nirNodata = 0.0;
    cv::Mat mask = cv::Mat::zeros(dsRed.size(), dsRed.type());
    if (redNodata != 0) {
        mask |= (dsRed == redNodata);
    }
    if (nirNodata != 0) {
        mask |= (dsNir == nirNodata);
    }

    // NDVI calculation
    cv::Mat denom = dsNir + dsRed;
    cv::Mat ndvi = cv::Mat::zeros(dsRed.size(), CV_32FC1);
    ndvi.setTo(-9999.0, mask);
    cv::MatIterator_<float> itNDVI = ndvi.begin<float>();
    for (cv::MatConstIterator_<float> itB4 = dsRed.begin<float>(), itEndB4 = dsRed.end<float>(); itB4 != itEndB4; ++itB4, ++itNDVI) {
        if (*itB4 == 0 || *itB4 == redNodata) {
            continue;
        }
        ndvi += (nirNodata - *itB4) / denom;
    }

    // Create output GeoTIFF
    cv::Mat outImage = cv::imwrite(outNdviPath, ndvi, {cv::IMWRITE_TILED, true, cv::IMWRITE_COMPRESSION_LZW});

    return 0;
}
[/C++]
[SCALA]
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.hadoop.fs._
import geotrellis.raster._
import geotrellis.raster.io.geotiff._
import geotrellis.proj4._
import geotrellis.vector._

object RDProOnSpark {
  def main(args: Array[String]): Unit = {
    val inputPathB4 = "/path/to/landsat8/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B4.TIF"  // Red
    val inputPathB5 = "/path/to/landsat8/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B5.TIF"  // NIR
    val outputPath = "/path/to/ndvi.tif"

    val conf = new SparkConf().setAppName("RDProOnSpark")
    val sc = new SparkContext(conf)

    val inputDsB4 = GeoTiffRasterSource(inputPathB4)
    val inputDsB5 = GeoTiffRasterSource(inputPathB5)

    // Check grid alignment
    if (inputDsB4.gridExtent != inputDsB5.gridExtent) {
      println("B4 and B5 grids do not match — warp one band first")
      return
    }

    // Handle NoData
    val redNodata = inputDsB4.metadata.bandType match {
      case TypeFloat => inputDsB4.metadata.nodataValue
      case _ => 0.0
    }
    val nirNodata = inputDsB5.metadata.bandType match {
      case TypeFloat => inputDsB5.metadata.nodataValue
      case _ => 0.0
    }
    val mask = sc.parallelize(inputDsB4.read().map { row =>
      (row._1, row._2)
    }, inputDsB4.rasterExtent.cols).filter { case (_, data) =>
      if (data.isFloat && !data.isNoDataTile) {
        false
      } else {
        true
      }
    }.map(_._2).collect()

    // NDVI calculation
    val denom = inputDsB5.read().add(inputDsB4.read()).multiply(mask)
    ndvi = sc.parallelize(denom.flatMap { row =>
      if (row.isFloat && !row.isNoDataTile) {
        val ndviRow = new Array[Double](row.length)
        var i = 0
        while (i < row.length) {
          if (mask(i)) {
            ndviRow(i) = (nirNodata - row(i)) / denom(i)
          } else {
            ndviRow(i) = Float.NaN
          }
          i += 1
        }
        ndviRow
      } else {
        null
      }
    }, inputDsB4.rasterExtent.cols).reduce(_ + _)

    // Create output GeoTIFF
    val crs = inputDsB4.crs
    val extent = inputDsB4.extent
    val layoutScheme = FloatingLayoutScheme(256)
    val writer = GeoTiffWriter(outputPath, compression = LZWCompression)
    writer.write(ndvi, crs, extent, layoutScheme)
  }
}
[/SCALA]
[GOLANG]
package main

import (
	"fmt"
	"github.com/mjibson/go-dsp/fft"
	"math"
)

func main() {
	// Paths to input images
	inputPathB4 := "/path/to/landsat8/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B4.TIF"  // Red
	inputPathB5 := "/path/to/landsat8/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B5.TIF"  // NIR
	outputPath := "/path/to/ndvi.tif"

	// Open datasets
	inputDsB4, err := geotrellis.NewRasterSource(inputPathB4)
	if err != nil {
		fmt.Println("Failed to open dataset B4")
	}
	inputDsB5, err := geotrellis.NewRasterSource(inputPathB5)
	if err != nil {
		fmt.Println("Failed to open dataset B5")
	}

	// Check grid alignment
	if !inputDsB4.GridExtent().Equals(inputDsB5.GridExtent()) {
		fmt.Println("B4 and B5 grids do not match — warp one band first")
		return
	}

	// Handle NoData
	redNodata := inputDsB4.Metadata().BandType() == geotrellis.TypeFloat && !inputDsB4.Metadata().NoDataValue().IsNaN()
	nirNodata := inputDsB5.Metadata().BandType() == geotrellis.TypeFloat && !inputDsB5.Metadata().NoDataValue().IsNaN()

	// NDVI calculation
	denom, err := inputDsB5.Read().Add(inputDsB4.Read()).Multiply(mask)
	if err != nil {
		fmt.Println("Failed to calculate denom")
	}
	ndvi := denom.FlatMap(func(row geotrellis.Raster, _ int) []float64 {
		if row.IsFloat() && !row.IsNoDataTile() {
			ndviRow := make([]float64, len(row))
			for i := 0; i < len(row); i++ {
				if mask[i] {
					ndviRow[i] = (nirNodata - row[i]) / denom[i]
				} else {
					ndviRow[i] = math.NaN()
				}
			}
			return ndviRow
		} else {
			return nil
		}
	}, inputDsB4.RasterExtent().Cols()).Reduce(func(i, j []float64) []float64 {
		return i
	})

	// Create output GeoTIFF
	crs := inputDsB4.Crs()
	extent := inputDsB4.Extent()
	layoutScheme := geotrellis.NewFloatingLayoutScheme(256)
	writer, err := geotrellis.NewGeoTiffWriter(outputPath, fft.LZWCompression{})
	if err != nil {
		fmt.Println("Failed to create writer")
	}
	err = writer.WriteFloat32(ndvi, crs, extent, layoutScheme)
	if err != nil {
		fmt.Println("Failed to write file")
	}
}
[/GOLANG]
[JAVA]
import java.awt.image.BufferedImage;
import java.io.File;
import javax.imageio.ImageIO;

public class Main {
  public static void main(String[] args) throws Exception {
    // Paths to input images
    String b4Path = "/path/to/landsat8/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B4.TIF";  // Red
    String b5Path = "/path/to/landsat8/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B5.TIF";  // NIR
    String outputPath = "/path/to/ndvi.tif";

    // Open datasets
    BufferedImage inputDsB4 = ImageIO.read(new File(b4Path));
    BufferedImage inputDsB5 = ImageIO.read(new File(b5Path));

    // Check grid alignment
    if (!inputDsB4.getWidth() == inputDsB5.getWidth() && !inputDsB4.getHeight() == inputDsB5.getHeight()) {
      System.out.println("B4 and B5 grids do not match — warp one band first");
      return;
    }

    // Handle NoData
    boolean redNodata = inputDsB4.getType() == BufferedImage.TYPE_FLOAT && !inputDsB4.isData();
    boolean nirNodata = inputDsB5.getType() == BufferedImage.TYPE_FLOAT && !inputDsB5.isData();

    // NDVI calculation
    BufferedImage denom = new BufferedImage(inputDsB4.getWidth(), inputDsB4.getHeight(), BufferedImage.TYPE_BYTE_GRAY);
    for (int y = 0; y < inputDsB4.getHeight(); y++) {
      for (int x = 0; x < inputDsB4.getWidth(); x++) {
        if (!redNodata && !nirNodata) {
          denom.setRGB(x, y, (byte) ((inputDsB5.getRGB(x, y)) + (inputDsB4.getRGB(x, y))));
        } else {
          denom.setRGB(x, y, (byte) 0);
        }
      }
    }
    ndvi = new BufferedImage(inputDsB4.getWidth(), inputDsB4.getHeight(), BufferedImage.TYPE_BYTE_GRAY);
    for (int y = 0; y < inputDsB4.getHeight(); y++) {
      for (int x = 0; x < inputDsB4.getWidth(); x++) {
        if (!redNodata && !nirNodata) {
          ndvi.setRGB(x, y, (byte) ((denom.getRGB(x, y)) / (inputDsB5.getRGB(x, y) - inputDsB4.getRGB(x, y))));
        } else {
          ndvi.setRGB(x, y, (byte) 0);
        }
      }
    }

    // Create output GeoTIFF
    File outputFile = new File(outputPath);
    ImageIO.write(ndvi, "GeoTIFF", outputFile);
  }
}
[/JAVA]
[PHP]
<?php
// Paths to input images
$b4Path = "/path/to/landsat8/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B4.TIF";  // Red
$b5Path = "/path/to/landsat8/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B5.TIF";  // NIR
$outputPath = "/path/to/ndvi.tif";

// Open datasets
$inputDsB4 = imagecreatefromstring(file_get_contents($b4Path));
$inputDsB5 = imagecreatefromstring(file_get_contents($b5Path));

// Check grid alignment
if ($inputDsB4->width != $inputDsB5->width || $inputDsB4->height != $inputDsB5->height) {
    echo "B4 and B5 grids do not match — warp one band first";
    return;
}

// Handle NoData
$redNodata = imagecolorat($inputDsB4, 0, 0) == -1 && !imageistruecolor($inputDsB4);
$nirNodata = imagecolorat($inputDsB5, 0, 0) == -1 && !imageistruecolor($inputDsB5);

// NDVI calculation
$denom = imagecreatetruecolor($inputDsB4->width, $inputDsB4->height);
for ($y = 0; $y < $inputDsB4->height; $y++) {
    for ($x = 0; $x < $inputDsB4->width; $x++) {
        if (!$redNodata && !$nirNodata) {
            imagesetpixel($denom, $x, $y, imagecolorat($inputDsB5, $x, $y) + imagecolorat($inputDsB4, $x, $y));
        } else {
            imagesetpixel($denom, $x, $y, -1);
        }
    }
}
$ndvi = imagecreatetruecolor($inputDsB4->width, $inputDsB4->height);
for ($y = 0; $y < $inputDsB4->height; $y++) {
    for ($x = 0; $x < $inputDsB4->width; $x++) {
        if (!$redNodata && !$nirNodata) {
            imagesetpixel($ndvi, $x, $y, imagecolorat($denom, $x, $y) / (imagecolorat($inputDsB5, $x, $y) - imagecolorat($inputDsB4, $x, $y)));
        } else {
            imagesetpixel($ndvi, $x, $y, -1);
        }
    }
}

// Create output GeoTIFF
$outputFile = fopen($outputPath, 'wb');
if ($outputFile === false) {
    echo "Failed to create output file";
    return;
}
imagepng($ndvi, $outputFile);
fclose($outputFile);
[/PHP]
[C++]
#include <iostream>
#include <opencv2/core.hpp>
#include <opencv2/imgcodecs.hpp>
#include <opencv2/highgui.hpp>

using namespace cv;
using namespace std;

int main() {
    // Paths to input images
    string b4Path = "/path/to/landsat8/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B4.TIF";  // Red
    string b5Path = "/path/to/landsat8/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B5.TIF";  // NIR
    string outputPath = "/path/to/ndvi.tif";

    // Open datasets
    Mat inputDsB4 = imread(b4Path, IMREAD_GRAYSCALE);
    Mat inputDsB5 = imread(b5Path, IMREAD_GRAYSCALE);

    // Check grid alignment
    if (inputDsB4.size != inputDsB5.size) {
        cout << "B4 and B5 grids do not match — warp one band first" << endl;
        return -1;
    }

    // Handle NoData
    bool redNodata = !inputDsB4.data || inputDsB4.at<uchar>(0, 0) == NO_DATA;
    bool nirNodata = !inputDsB5.data || inputDsB5.at<uchar>(0, 0) == NO_DATA;

    // NDVI calculation
    Mat denom(inputDsB4.size(), CV_8UC1);
    for (int y = 0; y < inputDsB4.rows; ++y) {
        for (int x = 0; x < inputDsB4.cols; ++x) {
            if (!redNodata && !nirNodata) {
                denom.at<uchar>(y, x) = saturate_cast<uchar>((inputDsB5.at<uchar>(y, x)) + (inputDsB4.at<uchar>(y, x)));
            } else {
                denom.at<uchar>(y, x) = NO_DATA;
            }
        }
    }
    ndvi = Mat(inputDsB4.size(), CV_8UC1);
    for (int y = 0; y < inputDsB4.rows; ++y) {
        for (int x = 0; x < inputDsB4.cols; ++x) {
            if (!redNodata && !nirNodata) {
                ndvi.at<uchar>(y, x) = saturate_cast<uchar>((denom.at<uchar>(y, x)) / (inputDsB5.at<uchar>(y, x) - inputDsB4.at<uchar>(y, x)));
            } else {
                ndvi.at<uchar>(y, x) = NO_DATA;
            }
        }
    }

    // Create output GeoTIFF
    imwrite(outputPath, ndvi);
    return 0;
}
[/C++]
[RUST]
use std::path::Path;
use std::fs::File;
use georust::*;

fn main() -> Result<(), GeoEngineerError> {
    // Paths to input images
    let b4_path = "/path/to/landsat8/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B4.TIF";  // Red
    let b5_path = "/path/to/landsat8/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B5.TIF";  // NIR
    let output_path = "/path/to/ndvi.tif";

    // Open datasets
    let input_ds_b4 = Raster::open(Path::new(b4_path))?;
    let input_ds_b5 = Raster::open(Path::new(b5_path))?;

    // Check grid alignment
    if input_ds_b4.metadata().grid_aligment() != input_ds_b5.metadata().grid_aligment() {
        return Err(GeoEngineerError::Generic("B4 and B5 grids do not match — warp one band first".to_string()));
    }

    // Handle NoData
    let red_nodata = input_ds_b4.metadata().data_type() == DataType::NoData;
    let nir_nodata = input_ds_b5.metadata().data_type() == DataType::NoData;

    // NDVI calculation
    let denom: Raster<u8> = Raster::new(input_ds_b4.metadata().grid_aligment(), 0u8)?;
    for (x, y, value) in input_ds_b5.iter() {
        if !red_nodata && !nir_nodata {
            denom.set(x, y, value + input_ds_b4.get(x, y))?;
        } else {
            denom.set(x, y, NO_DATA)?;
        }
    }
    ndvi = Raster::new(input_ds_b4.metadata().grid_aligment(), 0u8)?;
    for (x, y, value) in denom.iter() {
        if !red_nodata && !nir_nodata {
            ndvi.set(x, y, value / (input_ds_b5.get(x, y) - input_ds_b4.get(x, y)))?;
        } else {
            ndvi.set(x, y, NO_DATA)?;
        }
    }

    // Create output GeoTIFF
    let _ = ndvi.write_to_file(output_path)?;
    Ok(())
}
[/RUST]
[POWERSHELL]
# Paths to input images
$b4Path = "C:\path\to\landsat8\LC08_L2SP_040037_20250827_20250903_02_T1_SR_B4.TIF";  # Red
$b5Path = "C:\path\to\landsat8\LC08_L2SP_040037_20250827_20250903_02_T1_SR_B5.TIF";  # NIR
$outputPath = "C:\path\to\ndvi.tif";

# Open datasets
$inputDsB4 = Open-RasterImage -File $b4Path;
$inputDsB5 = Open-RasterImage -File $b5Path;

# Check grid alignment
if ($inputDsB4.GridAlignment -ne $inputDsB5.GridAlignment) {
    Write-Host "B4 and B5 grids do not match — warp one band first";
    return;
}

# Handle NoData
$redNodata = $inputDsB4.DataType -eq [ArcGIS.Core.Data.DataType]::NoData;
$nirNodata = $inputDsB5.DataType -eq [ArcGIS.Core.Data.DataType]::NoData;

# NDVI calculation
$denom = New-Object ArcGIS.Core.CIM.CIMDomain ([ArcGIS.Core.Geometry.Envelope]::New([ArcGIS.Core.Geometry.SpatialReference]::FromWkt("PROJCS[\"NAD83 / UTM zone 17N\",GEOGCS[\"NAD83\",DATUM[\"D_North_American_Datum_1927\",SPHEROID[\"Clarke 1866\",6378206.4,294.9786981345]],PRIMEM[\"Greenwich\",0],UNIT[\"Degree\",0.0174532925199433]],PROJECTION[\"Transverse_Mercator\"],PARAMETER[\"False_Easting\",500000],PARAMETER[\"False_Northing\",0],PARAMETER[\"Central_Meridian\",-67],PARAMETER[\"Scale_Factor\",0.9996],PARAMETER[\"Latitude_Of_Center\",0],UNIT[\"Foot_US\",0.3048006096012192]]))), $inputDsB4.GridAlignment);
for ($y = 0; $y -lt $inputDsB5.RasterYSize; $y++) {
    for ($x = 0; $x -lt $inputDsB5.RasterXSize; $x++) {
        if (!$redNodata -and !$nirNodata) {
            $denom.Value[$y][$x] = $inputDsB5.GetValue($x, $y) + $inputDsB4.GetValue($x, $y);
        } else {
            $denom.Value[$y][$x] = 0;
        }
    }
}
$ndvi = New-Object ArcGIS.Core.CIM.CIMDomain ([ArcGIS.Core.Geometry.Envelope]::New([ArcGIS.Core.Geometry.SpatialReference]::FromWkt("PROJCS[\"NAD83 / UTM zone 17N\",GEOGCS[\"NAD83\",DATUM[\"D_North_American_Datum_1927\",SPHEROID[\"Clarke 1866\",6378206.4,294.9786981345]],PRIMEM[\"Greenwich\",0],UNIT[\"Degree\",0.0174532925199433]],PROJECTION[\"Transverse_Mercator\"],PARAMETER[\"False_Easting\",500000],PARAMETER[\"False_Northing\",0],PARAMETER[\"Central_Meridian\",-67],PARAMETER[\"Scale_Factor\",0.9996],PARAMETER[\"Latitude_Of_Center\",0],UNIT[\"Foot_US\",0.3048006096012192]]))), $inputDsB4.GridAlignment);
for ($y = 0; $y -lt $inputDsB5.RasterYSize; $y++) {
    for ($x = 0; $x -lt $inputDsB5.RasterXSize; $x++) {
        if (!$redNodata -and !$nirNodata) {
            $ndvi.Value[$y][$x] = $denom.GetValue($x, $y) / ($inputDsB5.GetValue($x, $y) - $inputDsB4.GetValue($x, $y));
        } else {
            $ndvi.Value[$y][$x] = 0;
        }
    }
}

# Create output GeoTIFF
Save-RasterImage -Raster $ndvi -Path $outputPath;
[/POWERSHELL]