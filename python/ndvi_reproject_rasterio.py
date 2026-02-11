import os
from pathlib import Path

import numpy as np
import rasterio
from rasterio.warp import calculate_default_transform, reproject, Resampling

# Homebrew defaults; override if needed
os.environ.setdefault("PROJ_LIB", "/opt/homebrew/share/proj")
os.environ.setdefault("GDAL_DATA", "/opt/homebrew/share/gdal")

B4_PATH = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLM_doc/data/landsat8/LA/B4/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B4.TIF"  # Red
B5_PATH = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLM_doc/data/landsat8/LA/B5/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B5.TIF"  # NIR
OUT_NDVI = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLM_doc/output/ndvi_single_4326_rasterio.tif"

TARGET_CRS = "EPSG:4326"
NODATA_OUT = -9999.0
ALLOWED_DTYPES = {"uint8", "uint16", "int16", "uint32", "int32", "float32", "float64"}

Path(OUT_NDVI).parent.mkdir(parents=True, exist_ok=True)


def reproject_band_to_4326(src_ds, band_index=1, resampling=Resampling.bilinear):
    transform, width, height = calculate_default_transform(
        src_ds.crs,
        TARGET_CRS,
        src_ds.width,
        src_ds.height,
        *src_ds.bounds,
    )

    out = np.empty((height, width), dtype=np.float32)
    out.fill(NODATA_OUT)

    reproject(
        source=rasterio.band(src_ds, band_index),
        destination=out,
        src_transform=src_ds.transform,
        src_crs=src_ds.crs,
        src_nodata=src_ds.nodata,
        dst_transform=transform,
        dst_crs=TARGET_CRS,
        dst_nodata=NODATA_OUT,
        resampling=resampling,
    )

    return out, transform, width, height


def reproject_band_to_grid(src_ds, dst_transform, dst_crs, dst_width, dst_height, band_index=1, resampling=Resampling.bilinear):
    out = np.empty((dst_height, dst_width), dtype=np.float32)
    out.fill(NODATA_OUT)

    reproject(
        source=rasterio.band(src_ds, band_index),
        destination=out,
        src_transform=src_ds.transform,
        src_crs=src_ds.crs,
        src_nodata=src_ds.nodata,
        dst_transform=dst_transform,
        dst_crs=dst_crs,
        dst_nodata=NODATA_OUT,
        resampling=resampling,
    )

    return out


def main():
    with rasterio.open(B4_PATH) as red_src, rasterio.open(B5_PATH) as nir_src:
        red_dtype = red_src.dtypes[0]
        nir_dtype = nir_src.dtypes[0]
        if red_dtype not in ALLOWED_DTYPES:
            raise RuntimeError(f"Unsupported RED dtype: {red_dtype}")
        if nir_dtype not in ALLOWED_DTYPES:
            raise RuntimeError(f"Unsupported NIR dtype: {nir_dtype}")

        # Reproject RED to EPSG:4326 (defines target grid)
        red, dst_transform, dst_width, dst_height = reproject_band_to_4326(red_src, band_index=1)

        # Reproject NIR to RED's target grid for pixel-wise overlay
        nir = reproject_band_to_grid(
            nir_src,
            dst_transform=dst_transform,
            dst_crs=TARGET_CRS,
            dst_width=dst_width,
            dst_height=dst_height,
            band_index=1,
        )

        # Build invalid mask
        red_nodata = red_src.nodata
        nir_nodata = nir_src.nodata

        mask = np.zeros(red.shape, dtype=bool)
        mask |= ~np.isfinite(red)
        mask |= ~np.isfinite(nir)

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

        denom = nir + red
        ndvi = np.where((denom == 0) | mask, NODATA_OUT, (nir - red) / denom).astype(np.float32)

        profile = red_src.profile.copy()
        profile.update(
            driver="GTiff",
            dtype="float32",
            count=1,
            compress="lzw",
            tiled=True,
            nodata=NODATA_OUT,
            crs=TARGET_CRS,
            transform=dst_transform,
            width=dst_width,
            height=dst_height,
        )

        with rasterio.open(OUT_NDVI, "w", **profile) as dst:
            dst.write(ndvi, 1)

    print("NDVI (EPSG:4326) written to:", OUT_NDVI)


if __name__ == "__main__":
    main()
