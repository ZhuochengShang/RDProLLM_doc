from osgeo import gdal

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

    # Force execution: read one pixel (top-left of raster)
    sample = band.ReadAsArray(0, 0, 1, 1)[0, 0]

    # Optional: raster size / "partitions"-ish info (GDAL doesn't partition like Spark)
    width, height = ds.RasterXSize, ds.RasterYSize

    print(
        f"[dataloading] pixelType={dtype_name}, sample={sample}, "
        f"size={width}x{height}, bands={ds.RasterCount}"
    )

    return ds


if __name__ == "__main__":
    run()
