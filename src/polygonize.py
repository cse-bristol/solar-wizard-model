import subprocess
from os.path import join

import numpy as np
from osgeo import gdal
from psycopg2.sql import SQL, Identifier

import src.tables as tables
from src.db_funcs import connect, sql_script_with_bindings
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


def aggregate_horizons(pg_uri: str,
                       job_id: int,
                       horizon_slices: int,
                       max_roof_slope_degrees: int,
                       min_roof_area_m: int,
                       min_roof_degrees_from_north: int,
                       flat_roof_degrees: int):
    pg_conn = connect(pg_uri)

    schema = tables.schema(job_id)
    horizon_cols = ','.join([f'max(h.horizon_slice_{i}) AS horizon_slice_{i}' for i in range(0, horizon_slices)])

    try:
        sql_script_with_bindings(
            pg_conn, 'create.roof-horizons.sql',
            {
                "max_roof_slope_degrees": max_roof_slope_degrees,
                "min_roof_area_m": min_roof_area_m,
                "min_roof_degrees_from_north": min_roof_degrees_from_north,
                "flat_roof_degrees": flat_roof_degrees,
            },
            schema=Identifier(schema),
            pixel_horizons=Identifier(schema, tables.PIXEL_HORIZON_TABLE),
            roof_polygons=Identifier(schema, tables.ROOF_POLYGON_TABLE),
            roof_horizons=Identifier(schema, tables.ROOF_HORIZON_TABLE),
            horizon_cols=SQL(horizon_cols),
        )
    finally:
        pg_conn.close()
