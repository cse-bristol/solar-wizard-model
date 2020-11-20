import subprocess
from os.path import join

import numpy as np
from osgeo import gdal
from psycopg2 import connect
from psycopg2.sql import SQL, Identifier

import tables
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
    nodata = band.GetNoDataValue() or -9999
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
    schema = tables.schema(job_id)
    roof_polygon_table = tables.ROOF_POLYGON_TABLE

    res = subprocess.run(
        f'gdal_polygonize.py -b 1 -f PostgreSQL {masked_tif} '
        f'PG:"{pg_uri}" {schema}.{roof_polygon_table} aspect',
        capture_output=True, text=True, shell=True)
    print(res.stdout)
    print(res.stderr)
    if res.returncode != 0:
        raise ValueError(res.stderr)


def filter_polygons(pg_uri: str,
                    job_id: int,
                    horizon_slices: int,
                    max_roof_slope_degrees: int,
                    min_roof_area_m: int,
                    min_roof_degrees_from_north: int,
                    flat_roof_degrees: int):
    pg_conn = connect(pg_uri)

    horizon_cols = ','.join([f'max(h.horizon_slice_{i}) AS horizon_slice_{i}' for i in range(0, horizon_slices)])

    try:
        with pg_conn.cursor() as cursor:
            cursor.execute(SQL("""
            CREATE TABLE {roof_horizons} AS
            SELECT
                c.ogc_fid,
                c.wkb_geometry::geometry(Polygon, 27700),
                avg(h.slope) AS slope,
                avg(h.aspect) AS aspect,
                avg(sky_view_factor) AS sky_view_factor,
                avg(percent_visible) AS percent_visible,
                ST_X(ST_SetSRID(ST_Centroid(c.wkb_geometry), 27700)) AS easting,
                ST_Y(ST_SetSRID(ST_Centroid(c.wkb_geometry), 27700)) AS northing,
                ST_Area(c.wkb_geometry) / cos(avg(h.slope)) as area,
                ST_Area(c.wkb_geometry) as footprint,
            """ + horizon_cols + """
            FROM
                {roof_polygons} c
                LEFT JOIN {pixel_horizons} h ON ST_Contains(c.wkb_geometry, h.en)
            GROUP BY ogc_fid
            HAVING ST_Area(c.wkb_geometry) / cos(avg(h.slope)) >= %(min_roof_area_m)s;
            
            DELETE FROM {roof_horizons} WHERE degrees(slope) > %(max_roof_slope_degrees)s;
            DELETE FROM {roof_horizons} WHERE degrees(aspect) >= (360-%(min_roof_degrees_from_north)s)
                                          AND degrees(slope) > 5;
            DELETE FROM {roof_horizons} WHERE degrees(aspect) <= %(min_roof_degrees_from_north)s
                                          AND degrees(slope) > 5;
            """).format(
                pixel_horizons=Identifier(tables.schema(job_id), tables.PIXEL_HORIZON_TABLE),
                roof_polygons=Identifier(tables.schema(job_id), tables.ROOF_POLYGON_TABLE),
                roof_horizons=Identifier(tables.schema(job_id), tables.ROOF_HORIZON_TABLE),
            ), {
                "max_roof_slope_degrees": max_roof_slope_degrees,
                "min_roof_area_m": min_roof_area_m,
                "min_roof_degrees_from_north": min_roof_degrees_from_north,
                "flat_roof_degrees": flat_roof_degrees,
            })
            pg_conn.commit()
    finally:
        pg_conn.close()
