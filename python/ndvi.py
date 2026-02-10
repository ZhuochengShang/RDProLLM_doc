from osgeo import gdal
import numpy as np
from pathlib import Path

B4_PATH = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/data/landsat8/LA/B4/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B4.TIF"   # Red
B5_PATH = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/data/landsat8/LA/B5/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B5.TIF"   # NIR
OUT_NDVI = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/output/ndvi.tif"

# Open datasets
ds_red = gdal.Open(B4_PATH, gdal.GA_ReadOnly)
ds_nir = gdal.Open(B5_PATH, gdal.GA_ReadOnly)

assert ds_red and ds_nir, "Failed to open input files"

# Read arrays
red = ds_red.GetRasterBand(1).ReadAsArray().astype(np.float32)
nir = ds_nir.GetRasterBand(1).ReadAsArray().astype(np.float32)

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
