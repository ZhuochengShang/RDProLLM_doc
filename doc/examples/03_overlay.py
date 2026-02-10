from osgeo import gdal
import numpy as np

def overlay_gdal(path_one: str, path_two: str, out_path: str) -> None:
    ds1 = gdal.Open(path_one, gdal.GA_ReadOnly)
    ds2 = gdal.Open(path_two, gdal.GA_ReadOnly)
    if ds1 is None:
        raise RuntimeError(f"Failed to open: {path_one}")
    if ds2 is None:
        raise RuntimeError(f"Failed to open: {path_two}")

    b1 = ds1.GetRasterBand(1)
    b2 = ds2.GetRasterBand(1)

    # --- Alignment checks (rough equivalent to "same metadata") ---
    if (ds1.RasterXSize, ds1.RasterYSize) != (ds2.RasterXSize, ds2.RasterYSize):
        raise ValueError("Cannot overlay rasters: different raster size")

    gt1, gt2 = ds1.GetGeoTransform(), ds2.GetGeoTransform()
    if gt1 != gt2:
        raise ValueError("Cannot overlay rasters: different geotransform")

    prj1, prj2 = ds1.GetProjectionRef(), ds2.GetProjectionRef()
    if prj1 != prj2:
        raise ValueError("Cannot overlay rasters: different projection/CRS")

    # Read as arrays (this is driver-side / in-memory, unlike Spark)
    arr1 = b1.ReadAsArray()
    arr2 = b2.ReadAsArray()
    if arr1 is None or arr2 is None:
        raise RuntimeError("Failed to read raster arrays")

    # Ensure integer type like your RasterRDD[Int]
    arr1 = np.asarray(arr1, dtype=np.int32)
    arr2 = np.asarray(arr2, dtype=np.int32)

    # --- Sanity check like Scala first() and getPixelValue(x1,y1) ---
    # Use top-left pixel (0,0)
    sample = (int(arr1[0, 0]), int(arr2[0, 0]))
    print(f"[overlay_gdal] sample_pixel(top-left)={sample} size=({ds1.RasterXSize},{ds1.RasterYSize})")

    # --- Write stacked 2-band GeoTIFF ---
    driver = gdal.GetDriverByName("GTiff")
    out_ds = driver.Create(out_path, ds1.RasterXSize, ds1.RasterYSize, 2, gdal.GDT_Int32)
    if out_ds is None:
        raise RuntimeError(f"Failed to create output: {out_path}")

    out_ds.SetGeoTransform(gt1)
    out_ds.SetProjection(prj1)

    out_ds.GetRasterBand(1).WriteArray(arr1)  # band 1 = B4
    out_ds.GetRasterBand(2).WriteArray(arr2)  # band 2 = B5

    out_ds.FlushCache()
    out_ds = None
    ds1 = None
    ds2 = None

    print(f"[overlay_gdal] wrote: {out_path}")


if __name__ == "__main__":
    path_one = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/data/landsat8/LA/B4/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B4.TIF"
    path_two = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/data/landsat8/LA/B5/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B5.TIF"
    out_path = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/workspace/overlay_B4_B5.tif"
    overlay_gdal(path_one, path_two, out_path)
