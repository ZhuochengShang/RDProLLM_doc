from osgeo import gdal
import numpy as np
from pathlib import Path

PATH = "/Users/clockorangezoe/Desktop/PlanetAPI/example/20230527_180231_61_247b_3B_AnalyticMS.tif"
OUT_NDVI = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLM_doc/output/ndvi_band.tif"

# Ensure output folder exists
Path(OUT_NDVI).parent.mkdir(parents=True, exist_ok=True)

def gdal_to_numpy_dtype(gdal_type):
    mapping = {
        gdal.GDT_Byte: np.uint8,
        gdal.GDT_UInt16: np.uint16,
        gdal.GDT_Int16: np.int16,
        gdal.GDT_UInt32: np.uint32,
        gdal.GDT_Int32: np.int32,
        gdal.GDT_Float32: np.float32,
        gdal.GDT_Float64: np.float64,
    }
    if gdal_type not in mapping:
        raise RuntimeError(f"Unsupported GDAL data type: {gdal_type}")
    return mapping[gdal_type]

ds = gdal.Open(PATH, gdal.GA_ReadOnly)
if ds is None:
    raise FileNotFoundError(f"GDAL could not open: {PATH}")

# --- OPTIONAL: inspect band count & metadata to confirm mapping ---
print("Raster size:", ds.RasterXSize, ds.RasterYSize)
print("Band count:", ds.RasterCount)
for i in range(1, ds.RasterCount + 1):
    b = ds.GetRasterBand(i)
    desc = b.GetDescription()
    # Some Planet files store names in metadata rather than description
    print(f"Band {i}: desc={desc!r}, dtype={gdal.GetDataTypeName(b.DataType)} nodata={b.GetNoDataValue()}")

# ---- Choose band indices (MOST LIKELY for Planet AnalyticMS: Red=3, NIR=4) ----
RED_BAND = 3
NIR_BAND = 4

red_band = ds.GetRasterBand(RED_BAND)
nir_band = ds.GetRasterBand(NIR_BAND)

# Read using native dtype first (type check), then convert to float32 for NDVI
red_dtype = gdal_to_numpy_dtype(red_band.DataType)
nir_dtype = gdal_to_numpy_dtype(nir_band.DataType)

red = red_band.ReadAsArray().astype(red_dtype)
nir = nir_band.ReadAsArray().astype(nir_dtype)

red = red.astype(np.float32)
nir = nir.astype(np.float32)

# Handle NoData (if present)
red_nodata = red_band.GetNoDataValue()
nir_nodata = nir_band.GetNoDataValue()

mask = np.zeros(red.shape, dtype=bool)
if red_nodata is not None:
    if np.isnan(red_nodata):
        mask |= np.isnan(red)
    else:
        mask |= (red == np.float32(red_nodata))
if nir_nodata is not None:
    if np.isnan(nir_nodata):
        mask |= np.isnan(nir)
    else:
        mask |= (nir == np.float32(nir_nodata))

# NDVI
denom = nir + red
ndvi = np.where(
    mask | (denom == 0),
    -9999.0,
    (nir - red) / denom
).astype(np.float32)

# Write output GeoTIFF
driver = gdal.GetDriverByName("GTiff")
out_ds = driver.Create(
    OUT_NDVI,
    ds.RasterXSize,
    ds.RasterYSize,
    1,
    gdal.GDT_Float32,
    options=["TILED=YES", "COMPRESS=LZW"]
)
if out_ds is None:
    raise RuntimeError("Failed to create output dataset (check output path/permissions).")

out_ds.SetGeoTransform(ds.GetGeoTransform())
out_ds.SetProjection(ds.GetProjection())

out_band = out_ds.GetRasterBand(1)
out_band.WriteArray(ndvi)
out_band.SetNoDataValue(-9999.0)
out_band.FlushCache()

# Cleanup
out_ds = None
ds = None

print("NDVI written to:", OUT_NDVI)
