import gdal


def crop_to_mask(file_to_crop: str, mask_file: str, out_tiff: str):
    """
    Crop a file of a type GDAL can open to match the dimensions of another file,
    and output to a tiff file.

    If the LIDAR is not 1m resolution, stretch out to 1m.
    """
    to_crop = gdal.Open(file_to_crop)
    mask = gdal.Open(mask_file)
    ulx, xres, xskew, uly, yskew, yres = mask.GetGeoTransform()
    lrx = ulx + (mask.RasterXSize * xres)
    lry = uly + (mask.RasterYSize * yres)
    ds = gdal.Warp(out_tiff, to_crop, outputBounds=(ulx, lry, lrx, uly), xRes=xres, yRes=yres)
    ds = None
    mask = None
    to_crop = None
