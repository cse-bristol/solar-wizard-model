import subprocess
from os.path import join
from typing import List

import numpy as np
from osgeo import gdal
from psycopg2.sql import SQL, Identifier

import albion_models.solar_pv.tables as tables
from albion_models.db_funcs import connect, sql_script_with_bindings


def generate_aspect_polygons(mask_path: str, aspect_path: str, pg_uri: str, job_id: int, out_dir: str):
    bucketed = join(out_dir, 'aspect_bucketed.tif')
    masked = join(out_dir, 'aspect_masked.tif')

    _bucket_raster(aspect_path, bucketed, 30)
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
                       flat_roof_degrees: int,
                       max_avg_southerly_horizon_degrees: int):
    pg_conn = connect(pg_uri)
    schema = tables.schema(job_id)
    aggregated_horizon_cols = _aggregated_horizon_cols(horizon_slices, 'avg')

    try:
        sql_script_with_bindings(
            pg_conn, 'create.roof-horizons.sql',
            {
                "job_id": job_id,
                "max_roof_slope_degrees": max_roof_slope_degrees,
                "min_roof_area_m": min_roof_area_m,
                "min_roof_degrees_from_north": min_roof_degrees_from_north,
                "flat_roof_degrees": flat_roof_degrees,
                "max_avg_southerly_horizon_degrees": max_avg_southerly_horizon_degrees
            },
            schema=Identifier(schema),
            pixel_horizons=Identifier(schema, tables.PIXEL_HORIZON_TABLE),
            roof_polygons=Identifier(schema, tables.ROOF_POLYGON_TABLE),
            roof_horizons=Identifier(schema, tables.ROOF_HORIZON_TABLE),
            bounds_4326=Identifier(schema, tables.BOUNDS_TABLE),
            aggregated_horizon_cols=SQL(aggregated_horizon_cols),
            avg_southerly_horizon_rads=SQL(_avg_southerly_horizon_rads(horizon_slices)),
            horizon_cols=SQL(','.join(_horizon_cols(horizon_slices))),
            southerly_horizon_cols=SQL(','.join(_southerly_horizon_cols(horizon_slices))),
        )
    finally:
        pg_conn.close()


def aggregate_user_submitted_polygon_horizons(pg_uri: str,
                                              job_id: int,
                                              horizon_slices: int,
                                              flat_roof_degrees: int,
                                              aggregate_fn: str):
    pg_conn = connect(pg_uri)
    schema = tables.schema(job_id)
    aggregated_horizon_cols = _aggregated_horizon_cols(horizon_slices, aggregate_fn)

    try:
        sql_script_with_bindings(
            pg_conn, 'create.user-submitted-polygon-horizons.sql',
            {
                "job_id": job_id,
                "flat_roof_degrees": flat_roof_degrees,
            },
            schema=Identifier(schema),
            pixel_horizons=Identifier(schema, tables.PIXEL_HORIZON_TABLE),
            roof_horizons=Identifier(schema, tables.ROOF_HORIZON_TABLE),
            aggregated_horizon_cols=SQL(aggregated_horizon_cols),
        )
    finally:
        pg_conn.close()


def _aggregated_horizon_cols(horizon_slices: int, aggregate_fn: str) -> str:
    horizon_slices = int(horizon_slices)
    if aggregate_fn not in ("avg", "min", "max"):
        raise ValueError(f"Invalid horizon aggregate function '{aggregate_fn}")
    return ','.join([f'{aggregate_fn}(h.horizon_slice_{i}) AS horizon_slice_{i}' for i in range(0, horizon_slices)])


def _horizon_cols(horizon_slices: int) -> List[str]:
    horizon_slices = int(horizon_slices)
    return [f'h.horizon_slice_{i}' for i in range(0, horizon_slices)]


def _southerly_horizon_cols(horizon_slices: int, degrees_around_south: int = 135) -> List[str]:
    """
    Get horizon cols which count as southerly.
    By default southerly is defined as the 135 degrees between ESE and WSW inclusive.
    """
    horizon_slices = int(horizon_slices)
    degrees_around_south = int(degrees_around_south)

    centre = horizon_slices / 2  # South
    segment_size = 360 / horizon_slices
    segment_start = centre - (degrees_around_south / 2 / segment_size)
    segment_end = centre + (degrees_around_south / 2 / segment_size)
    return [f'h.horizon_slice_{i}' for i in range(0, horizon_slices) if segment_start <= i <= segment_end]


def _avg_southerly_horizon_rads(horizon_slices: int, degrees_around_south: int = 135) -> str:
    """
    Get the SQL for calculating the average southerly horizon radians.
    By default southerly is defined as the 135 degrees between ESE and WSW inclusive.
    """
    horizon_slices = int(horizon_slices)
    degrees_around_south = int(degrees_around_south)

    cols = _southerly_horizon_cols(horizon_slices, degrees_around_south)
    return f"({' + '.join(cols)}) / {len(cols)}"
