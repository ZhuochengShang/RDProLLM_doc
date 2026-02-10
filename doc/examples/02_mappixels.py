from osgeo import gdal
import numpy as np

def run():
    path = (
        "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/"
        "data/landsat8/LA/B4/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B4.TIF"
    )

    ds = gdal.Open(path, gdal.GA_ReadOnly)
    if ds is None:
        raise FileNotFoundError(f"GDAL could not open: {path}")

    band = ds.GetRasterBand(1)

    # Pixel type (similar idea to RDPro pixelType)
    dtype_name = gdal.GetDataTypeName(band.DataType)

    # Equivalent to raster.mapPixels(pixel => pixel + 10)
    raster = band.ReadAsArray().astype(np.int32)
    map_pixel_raster = raster + 10

    # Force execution: read one pixel (top-left of raster)
    sample = int(map_pixel_raster[0, 0])

    width, height = ds.RasterXSize, ds.RasterYSize

    print(
        f"[dataloading] pixelType={dtype_name}, "
        f"sample={sample}, size={width}x{height}, bands={ds.RasterCount}"
    )

    # Match Scala behavior: Scala returns `raster`, not mapped raster
    return raster


if __name__ == "__main__":
    run()
