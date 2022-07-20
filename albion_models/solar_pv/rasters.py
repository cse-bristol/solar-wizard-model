from os.path import join

import logging
import os
from psycopg2.sql import Identifier, Literal
from typing import Tuple

import albion_models.solar_pv.tables as tables
from albion_models import gdal_helpers
from albion_models.db_funcs import sql_script, copy_csv, connect, count
from albion_models.solar_pv import mask


def generate_rasters(pg_uri: str,
                     job_id: int,
                     solar_dir: str,
                     lidar_vrt_file: str,
                     override_mask_sql: str = None,
                     override_res: float = None,
                     debug_mode: bool = False) -> Tuple[str, str, str, str]:
    """
    Generate a single geoTIFF for the entire job area, as well as rasters for
    aspect, slope and a building mask.

    Generates all 4 rasters in both EPSG:4326 (long/lat) and in whatever SRS the
    input LIDAR was in (probably 27700 E/N), but the filenames returned reference
    the 4326 rasters.
    """
    srid = gdal_helpers.get_srid(lidar_vrt_file, fallback=27700)
    if override_res is None:
        res = gdal_helpers.get_res(lidar_vrt_file)
    else:
        res = override_res

    cropped_lidar = join(solar_dir, 'cropped_lidar.tif')
    aspect_raster = join(solar_dir, 'aspect.tif')
    slope_raster = join(solar_dir, 'slope.tif')
    mask_raster = join(solar_dir, 'mask.tif')

    if count(pg_uri, tables.schema(job_id), tables.LIDAR_PIXEL_TABLE) > 0:
        logging.info("Not creating rasters, raster data already loaded.")
        return cropped_lidar, aspect_raster, slope_raster, mask_raster

    unit_dims, unit = gdal_helpers.get_srs_units(lidar_vrt_file)
    if unit_dims != 1.0 or unit != 'metre':
        # If this ever needs changing - the `resolution_metres` param of `aggregate_horizons()`
        # needs a resolution per metre rather than per whatever the unit of the SRS is -
        # otherwise the calculated areas/footprints of PV installations will be wrong.
        # See `create.roof-horizons.sql`
        raise ValueError(f"Albion cannot currently handle LIDAR where the SRS unit is "
                         f"not 1m: was {unit} {unit_dims}")

    logging.info("Creating raster mask...")
    if override_mask_sql:
        mask_sql = override_mask_sql
    else:
        mask_sql = mask.buildings_mask_sql(pg_uri, job_id, buffer=1)

    mask.create_mask(mask_sql, mask_raster, pg_uri, res=res, srid=srid)

    logging.info("Cropping lidar to mask dimensions...")
    gdal_helpers.crop_or_expand(lidar_vrt_file, mask_raster, cropped_lidar,
                                adjust_resolution=True)

    logging.info("Creating aspect raster...")
    gdal_helpers.aspect(cropped_lidar, aspect_raster)

    logging.info("Creating slope raster...")
    gdal_helpers.slope(cropped_lidar, slope_raster)

    logging.info("Converting to 4326...")
    cropped_lidar_4326, aspect_raster_4326, slope_raster_4326, mask_raster_4326 =  \
        _generate_4326_rasters(solar_dir, srid, cropped_lidar, aspect_raster, slope_raster,
                               mask_raster)

    logging.info("Loading raster data...")
    _load_rasters_to_db(pg_uri, job_id, srid, solar_dir, cropped_lidar,
                        aspect_raster, slope_raster, mask_raster, debug_mode)

    return cropped_lidar_4326, aspect_raster_4326, slope_raster_4326, mask_raster_4326


def _generate_4326_rasters(solar_dir: str,
                           srid: int,
                           elevation_raster: str,
                           aspect_raster: str,
                           slope_raster: str,
                           mask_raster: str):
    elevation_raster_4326 = join(solar_dir, 'elevation_4326.tif')
    aspect_raster_4326 = join(solar_dir, 'aspect_4326.tif')
    slope_raster_4326 = join(solar_dir, 'slope_4326.tif')
    mask_raster_4326 = join(solar_dir, 'mask_4326.tif')

    if srid == 27700:
        # Use the 7-parameter shift rather than GDAL's default 3-parameter shift
        # for EN->long/lat transformation as it's much more accurate:
        # https://digimap.edina.ac.uk/webhelp/digimapgis/projections_and_transformations/transformations_in_gdalogr.htm
        src_srs = "+proj=tmerc +lat_0=49 +lon_0=-2 " \
                  "+k=0.999601 +x_0=400000 +y_0=-100000 +ellps=airy +units=m +no_defs " \
                  "+towgs84=446.448,-125.157,542.060,0.1502,0.2470,0.8421,-20.4894"
    else:
        src_srs = f"EPSG:{srid}"

    gdal_helpers.reproject(elevation_raster, elevation_raster_4326, src_srs=src_srs, dst_srs="EPSG:4326")
    gdal_helpers.reproject(aspect_raster, aspect_raster_4326, src_srs=src_srs, dst_srs="EPSG:4326")
    gdal_helpers.reproject(slope_raster, slope_raster_4326, src_srs=src_srs, dst_srs="EPSG:4326")
    gdal_helpers.reproject(mask_raster, mask_raster_4326, src_srs=src_srs, dst_srs="EPSG:4326")

    return elevation_raster_4326, aspect_raster_4326, slope_raster_4326, mask_raster_4326


def _load_rasters_to_db(pg_uri: str,
                        job_id: int,
                        srid: int,
                        solar_dir: str,
                        cropped_lidar: str,
                        aspect_raster: str,
                        slope_raster: str,
                        mask_raster: str,
                        debug_mode: bool):
    pg_conn = connect(pg_uri)
    schema = tables.schema(job_id)
    lidar_pixels_table = tables.LIDAR_PIXEL_TABLE
    try:
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
            srid=Literal(srid)
        )
    finally:
        pg_conn.close()


def copy_raster(pg_conn, solar_dir: str, raster: str, table: str, mask_raster: str = None, debug_mode: bool = False):
    csv_file = join(solar_dir, f'temp-{table}.csv')
    gdal_helpers.raster_to_csv(raster, csv_file, mask_raster)
    copy_csv(pg_conn, csv_file, table)
    if not debug_mode:
        try:
            os.remove(csv_file)
        except OSError:
            pass
