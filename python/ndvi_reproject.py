from osgeo import gdal
import numpy as np
from pathlib import Path
import os
import sys

# Use GDAL exceptions explicitly (future-proof for GDAL 4 behavior changes)
gdal.UseExceptions()


def configure_proj():
    """
    Pick a PROJ data directory that actually contains proj.db.
    This avoids Homebrew-Python + Conda-PROJ mismatches.
    """
    candidates = [
        os.environ.get("PROJ_LIB"),
        "/opt/homebrew/share/proj",
        "/usr/local/share/proj",
        str(Path(sys.prefix) / "share" / "proj"),
        "/Users/clockorangezoe/miniconda3/envs/geo_llm_spark/share/proj",
    ]

    for c in candidates:
        if c and Path(c, "proj.db").exists():
            os.environ["PROJ_LIB"] = c
            gdal.SetConfigOption("PROJ_LIB", c)
            return c

    raise RuntimeError(
        "Could not find proj.db. Set PROJ_LIB to a valid PROJ data directory."
    )


PROJ_PATH = configure_proj()

B4_PATH = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLM_doc/data/landsat8/LA/B4/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B4.TIF"   # Red
B5_PATH = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLM_doc/data/landsat8/LA/B5/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B5.TIF"   # NIR
OUT_NDVI = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLM_doc/output/ndvi_single_4326.tif"

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


def reproject_to_4326(ds, resample_alg="bilinear"):
    warp_opts = gdal.WarpOptions(
        format="VRT",
        dstSRS="EPSG:4326",
        resampleAlg=resample_alg,
        dstNodata=-9999.0,
    )
    out = gdal.Warp("", ds, options=warp_opts)
    if out is None:
        raise RuntimeError("Failed to reproject dataset to EPSG:4326")
    return out


def bounds_from_geotransform(ds):
    gt = ds.GetGeoTransform()
    w = ds.RasterXSize
    h = ds.RasterYSize
    minx = gt[0]
    maxy = gt[3]
    maxx = minx + w * gt[1]
    miny = maxy + h * gt[5]
    return (minx, miny, maxx, maxy)


# Open source datasets
src_red = gdal.Open(B4_PATH, gdal.GA_ReadOnly)
src_nir = gdal.Open(B5_PATH, gdal.GA_ReadOnly)
assert src_red and src_nir, "Failed to open input files"

# Reproject RED to EPSG:4326
ds_red = reproject_to_4326(src_red, resample_alg="bilinear")

# Reproject NIR to EPSG:4326 and align to RED grid for pixel-wise overlay
minx, miny, maxx, maxy = bounds_from_geotransform(ds_red)
ds_nir = gdal.Warp(
    "",
    src_nir,
    options=gdal.WarpOptions(
        format="VRT",
        dstSRS="EPSG:4326",
        outputBounds=(minx, miny, maxx, maxy),
        width=ds_red.RasterXSize,
        height=ds_red.RasterYSize,
        resampleAlg="bilinear",
        dstNodata=-9999.0,
    ),
)
if ds_nir is None:
    raise RuntimeError("Failed to reproject/align NIR dataset to EPSG:4326")

red_band = ds_red.GetRasterBand(1)
nir_band = ds_nir.GetRasterBand(1)
assert red_band and nir_band, "Missing band 1 in one of the inputs"

# Read arrays using native dtype first, then convert to float32 for NDVI
red_dtype = gdal_to_numpy_dtype(red_band.DataType)
nir_dtype = gdal_to_numpy_dtype(nir_band.DataType)

red = red_band.ReadAsArray().astype(red_dtype).astype(np.float32)
nir = nir_band.ReadAsArray().astype(nir_dtype).astype(np.float32)

# Handle NoData
red_nodata = red_band.GetNoDataValue()
nir_nodata = nir_band.GetNoDataValue()

mask = np.zeros(red.shape, dtype=bool)
if red_nodata is not None:
    if np.isnan(red_nodata):
        mask |= np.isnan(red)
    else:
        mask |= (red == red_nodata)
if nir_nodata is not None:
    if np.isnan(nir_nodata):
        mask |= np.isnan(nir)
    else:
        mask |= (nir == nir_nodata)

# NDVI calculation
denom = nir + red
ndvi = np.where(
    (denom == 0) | mask,
    -9999.0,
    (nir - red) / denom,
).astype(np.float32)

# Create output GeoTIFF (in EPSG:4326)
driver = gdal.GetDriverByName("GTiff")
out_ds = driver.Create(
    OUT_NDVI,
    ds_red.RasterXSize,
    ds_red.RasterYSize,
    1,
    gdal.GDT_Float32,
    options=["TILED=YES", "COMPRESS=LZW"],
)

out_ds.SetGeoTransform(ds_red.GetGeoTransform())
out_ds.SetProjection(ds_red.GetProjection())

out_band = out_ds.GetRasterBand(1)
out_band.WriteArray(ndvi)
out_band.SetNoDataValue(-9999.0)
out_band.FlushCache()

# Cleanup
src_red = None
src_nir = None
ds_red = None
ds_nir = None
out_ds = None

print("PROJ_LIB:", PROJ_PATH)
print("NDVI (EPSG:4326) written to:", OUT_NDVI)
