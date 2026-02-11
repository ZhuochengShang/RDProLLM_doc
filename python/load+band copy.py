from osgeo import gdal
from pathlib import Path
import json

def gdal_band_stats(in_tif: str, out_json: str, approx_ok: bool = True, force: bool = True):
    """
    Write basic dataset info + per-band stats to a JSON file.

    approx_ok=True uses overviews / approximate stats when possible (faster).
    force=True forces stats computation if not already available.
    """
    ds = gdal.Open(in_tif, gdal.GA_ReadOnly)
    if ds is None:
        raise FileNotFoundError(f"Failed to open: {in_tif}")

    info = {
        "path": in_tif,
        "size": [ds.RasterXSize, ds.RasterYSize],
        "bands": ds.RasterCount,
        "projection": ds.GetProjection(),
        "geotransform": ds.GetGeoTransform(),
        "driver": ds.GetDriver().ShortName if ds.GetDriver() else None,
        "band_stats": [],
    }

    for i in range(1, ds.RasterCount + 1):
        b = ds.GetRasterBand(i)
        nodata = b.GetNoDataValue()

        # If stats aren't present in metadata, force compute if requested.
        # GetStatistics(force, approx_ok)
        mn, mx, mean, std = b.GetStatistics(force, approx_ok)

        info["band_stats"].append({
            "band": i,
            "nodata": nodata,
            "min": mn,
            "max": mx,
            "mean": mean,
            "std": std,
            "dtype": gdal.GetDataTypeName(b.DataType),
            "block_size": b.GetBlockSize(),
        })

        # release band handle
        b = None

    # release dataset handle
    ds = None

    out_path = Path(out_json)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(info, indent=2), encoding="utf-8")
    return info

B4_PATH = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/data/landsat8/LA/B4/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B4.TIF"
gdal_band_stats(B4_PATH, "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/output/B4_stats.json")