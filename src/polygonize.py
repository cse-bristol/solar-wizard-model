import subprocess
from os.path import join

import numpy as np
from osgeo import gdal
from psycopg2 import connect
from psycopg2.sql import SQL

from src.crop import crop_to_mask


def generate_aspect_polygons(mask_path: str, aspect_path: str, pg_uri: str, job_id: int, out_dir: str):
    cropped = join(out_dir, 'aspect_cropped.tif')
    bucketed = join(out_dir, 'aspect_bucketed.tif')
    masked = join(out_dir, 'aspect_masked.tif')

    crop_to_mask(aspect_path, mask_path, cropped)
    _bucket_raster(cropped, bucketed, 30)
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
    a[a == 360] = 0

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
        f'PG:"{pg_uri}" models.roof_polygons_job_{job_id} aspect',
        capture_output=True, text=True, shell=True)
    print(res.stdout)
    print(res.stderr)
    if res.returncode != 0:
        raise ValueError(res.stderr)


def filter_polygons(pg_uri: str, job_id: int, max_roof_slope_degrees: int, min_roof_area_m: int, max_roof_degrees_from_north: int):
    pg_conn = connect(pg_uri)
    try:
        with pg_conn.cursor() as cursor:
            # todo table names
            # todo won't work with horizon slices arg
            cursor.execute(SQL("""
            CREATE TABLE models.roof_horizons_job_24 AS
            SELECT
                c.ogc_fid,
                c.wkb_geometry::geometry(Polygon, 27700),
                avg(h.slope) AS slope,
                avg(h.aspect) AS aspect,
                ST_X(ST_SetSRID(ST_Centroid(c.wkb_geometry), 27700)) AS easting,
                ST_Y(ST_SetSRID(ST_Centroid(c.wkb_geometry), 27700)) AS northing,
                ST_Area(c.wkb_geometry) / cos(avg(h.slope)) as area,
                ST_Area(c.wkb_geometry) as footprint,
                max(h.angle_rad_0) AS angle_rad_0,
                max(h.angle_rad_45) AS angle_rad_45,
                max(h.angle_rad_90) AS angle_rad_90,
                max(h.angle_rad_135) AS angle_rad_135,
                max(h.angle_rad_180) AS angle_rad_180,
                max(h.angle_rad_225) AS angle_rad_225,
                max(h.angle_rad_270) AS angle_rad_270,
                max(h.angle_rad_315) AS angle_rad_315
            FROM
                models.roof_polygons_job_24 c
                LEFT JOIN models.horizons_job_24 h ON ST_Contains(c.wkb_geometry, h.en)
            GROUP BY ogc_fid
            HAVING ST_Area(c.wkb_geometry) / cos(avg(h.slope)) >= %(min_roof_area_m)s;
            
            DELETE FROM models.roof_horizons_job_24 WHERE degrees(slope) > %(max_roof_slope_degrees)s;
            DELETE FROM models.roof_horizons_job_24 WHERE degrees(aspect) >= (360-%(max_roof_degrees_from_north)s) 
                                                       OR degrees(aspect) <= %(max_roof_degrees_from_north)s;
            """), {
                "max_roof_slope_degrees": max_roof_slope_degrees,
                "min_roof_area_m": min_roof_area_m,
                "max_roof_degrees_from_north": max_roof_degrees_from_north,
            })
            pg_conn.commit()
    finally:
        pg_conn.close()
