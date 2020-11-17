import subprocess
from os.path import join

import numpy as np
from osgeo import gdal

from crop import crop_to_mask


def generate_aspect_polygons(mask_path: str, aspect_path: str, pg_uri: str, job_id: int, out_dir: str):
    cropped = join(out_dir, 'aspect_cropped.tif')
    bucketed = join(out_dir, 'aspect_bucketed.tif')
    masked = join(out_dir, 'aspect_masked.tif')

    crop_to_mask(aspect_path, mask_path, cropped)
    _bucket_raster(cropped, bucketed, 30)
    _mask_raster(bucketed, mask_path, masked)
    _polygonise(masked, pg_uri, job_id)


def generate_slope_polygons(mask_path: str, slope_path: str, pg_uri: str, job_id: int, out_dir: str):
    cropped = join(out_dir, 'slope_cropped.tif')
    bucketed = join(out_dir, 'slope_bucketed.tif')
    masked = join(out_dir, 'slope_masked.tif')

    crop_to_mask(slope_path, mask_path, cropped)
    _bucket_raster(cropped, bucketed, 15)
    _mask_raster(bucketed, mask_path, masked)
    _polygonise(masked, pg_uri, job_id)


def _bucket_raster(raster_to_bucket: str, out_tif: str, bucket_size):
    file = gdal.Open(raster_to_bucket)
    band = file.GetRasterBand(1)
    nodata = band.GetNoDataValue()
    xsize = band.XSize
    ysize = band.YSize
    a = band.ReadAsArray()
    a[a != nodata] /= bucket_size
    np.around(a, out=a)
    a[a != nodata] *= bucket_size
    a = a.astype(int)

    driver = gdal.GetDriverByName('GTiff')
    new_tiff = driver.Create(out_tif, xsize, ysize, 1, gdal.GDT_Int16)
    new_tiff.SetGeoTransform(file.GetGeoTransform())
    new_tiff.SetProjection(file.GetProjection())
    new_tiff.GetRasterBand(1).SetNoDataValue(nodata)
    new_tiff.GetRasterBand(1).WriteArray(a)
    new_tiff.FlushCache()

    new_tiff = None
    file = None
    band = None


def _mask_raster(raster_to_mask: str, mask_tif: str, out_tif: str):
    """
    Mask a raster using another raster. Every pixel where the mask == 0
    will be set the the nodata value of the target raster.
    """
    mask = gdal.Open(mask_tif)
    file = gdal.Open(raster_to_mask)
    band = file.GetRasterBand(1)
    nodata = band.GetNoDataValue()
    xsize = band.XSize
    ysize = band.YSize
    a = band.ReadAsArray()

    mband = mask.GetRasterBand(1)
    ma = mband.ReadAsArray().astype(int)

    a[ma == 0] = nodata

    driver = gdal.GetDriverByName('GTiff')
    new_tiff = driver.Create(out_tif, xsize, ysize, 1, gdal.GDT_Int16)
    new_tiff.SetGeoTransform(file.GetGeoTransform())
    new_tiff.SetProjection(file.GetProjection())
    new_tiff.GetRasterBand(1).SetNoDataValue(nodata)
    new_tiff.GetRasterBand(1).WriteArray(a)
    new_tiff.FlushCache()

    new_tiff = None
    file = None
    mask = None
    band = None
    mband = None


def _polygonise(masked_tif: str, pg_uri: str, job_id: int):
    job_id = int(job_id)
    res = subprocess.run(
        f'gdal_polygonize.py -b 1 -f PostgreSQL {masked_tif} '
        f'PG:"{pg_uri}" models.roof_polygons_job_{job_id} slope',
        capture_output=True, text=True, shell=True)
    print(res.stdout)
    print(res.stderr)
    if res.returncode != 0:
        raise ValueError(res.stderr)

