from os.path import join

import logging
import os
from psycopg2.sql import Identifier, Literal
from typing import Tuple

import albion_models.solar_pv.tables as tables
import psycopg2.extras
from albion_models import gdal_helpers
from albion_models.db_funcs import sql_script, copy_csv, count, connection
from albion_models.postgis import get_merged_lidar
from albion_models.solar_pv import mask
from albion_models.transformations import _7_PARAM_SHIFT


def generate_rasters(pg_uri: str,
                     job_id: int,
                     solar_dir: str,
                     horizon_search_radius: int,
                     debug_mode: bool = False) -> Tuple[str, str, float]:
    """
    Generate a single geoTIFF for the entire job area, as well as rasters for
    aspect, slope and a building mask.

    Generates all rasters in both EPSG:4326 (long/lat) and in whatever SRS the
    input LIDAR was in (probably 27700 E/N), but the filenames returned reference
    the 4326 rasters.
    """

    elevation_raster = join(solar_dir, 'elevation.tif')
    aspect_raster = join(solar_dir, 'aspect.tif')
    slope_raster = join(solar_dir, 'slope.tif')
    mask_raster = join(solar_dir, 'mask.tif')

    if count(pg_uri, tables.schema(job_id), tables.LIDAR_PIXEL_TABLE) > 0:
        logging.info("Not creating rasters, raster data already loaded.")
        res = gdal_helpers.get_res(elevation_raster)
        return (join(solar_dir, 'elevation_4326.tif'),
                join(solar_dir, 'mask_4326.tif'), res)

    with connection(pg_uri, cursor_factory=psycopg2.extras.DictCursor) as pg_conn:
        get_merged_lidar(pg_conn, job_id, elevation_raster)

    srid = gdal_helpers.get_srid(elevation_raster, fallback=27700)
    res = gdal_helpers.get_res(elevation_raster)

    unit_dims, unit = gdal_helpers.get_srs_units(elevation_raster)
    if unit_dims != 1.0 or unit != 'metre':
        # If this ever needs changing - the `resolution_metres` param of `create_roof_polygons()`
        # needs a resolution per metre rather than per whatever the unit of the SRS is -
        # otherwise the calculated areas/footprints of PV installations will be wrong.
        # See `create.roof-horizons.sql`
        # Also, gdal_helpers.expand() assumes that the buffer arg is in the same unit as
        # the SRS, and the buffer arg is currently assumed to be metres
        raise ValueError(f"Albion cannot currently handle LIDAR where the SRS unit is "
                         f"not 1m: was {unit} {unit_dims}")

    logging.info("Creating raster mask...")
    mask_sql = mask.buildings_mask_sql(pg_uri, job_id, buffer=1)

    unbuffered_mask_raster = join(solar_dir, 'unbuffered_mask.tif')
    mask.create_mask(mask_sql, unbuffered_mask_raster, pg_uri, res=res, srid=srid)
    gdal_helpers.expand(unbuffered_mask_raster, mask_raster, buffer=horizon_search_radius)

    logging.info("Cropping lidar to mask dimensions...")
    gdal_helpers.crop_or_expand(elevation_raster, mask_raster, elevation_raster,
                                adjust_resolution=True)

    logging.info("Creating aspect raster...")
    gdal_helpers.aspect(elevation_raster, aspect_raster)

    logging.info("Creating slope raster...")
    gdal_helpers.slope(elevation_raster, slope_raster)

    logging.info("Converting to 4326...")
    cropped_lidar_4326, mask_raster_4326 = _generate_4326_rasters(
        solar_dir, srid, elevation_raster, mask_raster)

    logging.info("Loading raster data...")
    _load_rasters_to_db(pg_uri, job_id, srid, res, solar_dir, elevation_raster,
                        aspect_raster, slope_raster, mask_raster, debug_mode)

    return cropped_lidar_4326, mask_raster_4326, res


def _generate_4326_rasters(solar_dir: str,
                           srid: int,
                           elevation_raster: str,
                           mask_raster: str):
    elevation_raster_4326 = join(solar_dir, 'elevation_4326.tif')
    mask_raster_4326 = join(solar_dir, 'mask_4326.tif')

    if srid == 27700:
        # Use the 7-parameter shift rather than GDAL's default 3-parameter shift
        # for EN->long/lat transformation as it's much more accurate:
        # https://digimap.edina.ac.uk/webhelp/digimapgis/projections_and_transformations/transformations_in_gdalogr.htm
        src_srs = _7_PARAM_SHIFT
    else:
        src_srs = f"EPSG:{srid}"

    gdal_helpers.reproject(elevation_raster, elevation_raster_4326, src_srs=src_srs, dst_srs="EPSG:4326")
    gdal_helpers.reproject(mask_raster, mask_raster_4326, src_srs=src_srs, dst_srs="EPSG:4326")

    return elevation_raster_4326, mask_raster_4326


def _load_rasters_to_db(pg_uri: str,
                        job_id: int,
                        srid: int,
                        res: float,
                        solar_dir: str,
                        cropped_lidar: str,
                        aspect_raster: str,
                        slope_raster: str,
                        mask_raster: str,
                        debug_mode: bool):
    # TODO this could be made a db-only operation, going from db-rasters to per-pixel info
    with connection(pg_uri) as pg_conn:
        schema = tables.schema(job_id)
        lidar_pixels_table = tables.LIDAR_PIXEL_TABLE

        sql_script(
            pg_conn, 'pv/create.lidar-pixels.sql',
            lidar_pixels=Identifier(schema, lidar_pixels_table),
            aspect_pixels=Identifier(schema, "aspect_pixels"),
            slope_pixels=Identifier(schema, "slope_pixels"),
        )

        copy_raster(pg_conn, solar_dir, cropped_lidar, f"{schema}.{lidar_pixels_table}", mask_raster, debug_mode)
        copy_raster(pg_conn, solar_dir, aspect_raster, f"{schema}.aspect_pixels", mask_raster, debug_mode)
        copy_raster(pg_conn, solar_dir, slope_raster, f"{schema}.slope_pixels", mask_raster, debug_mode)

        sql_script(
            pg_conn, 'pv/post-load.lidar-pixels.sql',
            schema=Identifier(schema),
            lidar_pixels=Identifier(schema, lidar_pixels_table),
            aspect_pixels=Identifier(schema, "aspect_pixels"),
            slope_pixels=Identifier(schema, "slope_pixels"),
            buildings=Identifier(schema, tables.BUILDINGS_TABLE),
            srid=Literal(srid),
            res=Literal(res),
        )


def copy_raster(pg_conn,
                solar_dir: str,
                raster: str,
                table: str,
                mask_raster: str = None,
                include_nans: bool = True,
                debug_mode: bool = False):
    csv_file = join(solar_dir, f'temp-{table}.csv')
    gdal_helpers.raster_to_csv(raster_file=raster, csv_out=csv_file, mask_raster=mask_raster, include_nans=include_nans)
    copy_csv(pg_conn, csv_file, table)
    if not debug_mode:
        try:
            os.remove(csv_file)
        except OSError:
            pass
