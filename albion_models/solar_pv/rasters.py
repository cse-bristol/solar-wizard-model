from os.path import join

import logging
import os
from psycopg2.sql import Identifier, Literal
from typing import Tuple, Optional

import albion_models.solar_pv.tables as tables
import psycopg2.extras
from albion_models import gdal_helpers
from albion_models.db_funcs import sql_script, copy_csv, count, connection
from albion_models.postgis import get_merged_lidar
from albion_models.solar_pv import mask
from albion_models.solar_pv.raster_names import MASK_4326_TIF, MASK_BUF1_TIF, ELEVATION_4326_TIF
from albion_models.solar_pv.roof_polygons.roof_polygons import get_flat_roof_aspect_sql, create_flat_roof_aspect, \
    has_flat_roof, get_outdated_lidar_building_h_sql_4326, has_outdated_lidar
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
    mask_raster_buf1 = join(solar_dir, MASK_BUF1_TIF)
    mask_raster_buf3 = join(solar_dir, 'mask_buf3.tif')

    if count(pg_uri, tables.schema(job_id), tables.LIDAR_PIXEL_TABLE) > 0:
        logging.info("Not creating rasters, raster data already loaded.")
        res = gdal_helpers.get_res(elevation_raster)
        return (join(solar_dir, ELEVATION_4326_TIF),
                join(solar_dir, MASK_4326_TIF), res)

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

    logging.info("Creating raster masks...")
    # Mask with a 1m buffer around buildings, for PVGIS:
    mask_sql_buf1 = mask.buildings_mask_sql(pg_uri, job_id, buffer=1)
    mask.create_mask(mask_sql_buf1, mask_raster_buf1, pg_uri, res=res, srid=srid)
    gdal_helpers.expand(mask_raster_buf1, mask_raster_buf1, buffer=horizon_search_radius)

    # Mask with a 3m buffer around buildings, for various usages of pixel
    # data. The main reason for the bigger buffer is for invalid LiDAR detection:
    mask_sql_buf3 = mask.buildings_mask_sql(pg_uri, job_id, buffer=3)
    mask.create_mask(mask_sql_buf3, mask_raster_buf3, pg_uri, res=res, srid=srid)
    gdal_helpers.expand(mask_raster_buf3, mask_raster_buf3, buffer=horizon_search_radius)

    logging.info("Cropping lidar to mask dimensions...")
    gdal_helpers.crop_or_expand(elevation_raster, mask_raster_buf3, elevation_raster,
                                adjust_resolution=True)

    logging.info("Creating aspect raster...")
    gdal_helpers.aspect(elevation_raster, aspect_raster)

    logging.info("Creating slope raster...")
    gdal_helpers.slope(elevation_raster, slope_raster)

    logging.info("Converting to 4326...")
    cropped_lidar_4326, mask_raster_4326 = _generate_4326_rasters(
        solar_dir, srid, elevation_raster, mask_raster_buf1)

    logging.info("Loading raster data...")
    _load_rasters_to_db(pg_uri, job_id, srid, res, solar_dir, elevation_raster,
                        aspect_raster, slope_raster, mask_raster_buf3, debug_mode)

    return cropped_lidar_4326, mask_raster_4326, res


def _generate_4326_rasters(solar_dir: str,
                           srid: int,
                           elevation_raster: str,
                           mask_raster: str):
    elevation_raster_4326 = join(solar_dir, ELEVATION_4326_TIF)
    mask_raster_4326 = join(solar_dir, MASK_4326_TIF)

    src_srs: str = _get_srs_for_reproject_to_4326(srid)

    gdal_helpers.reproject(elevation_raster, elevation_raster_4326, src_srs=src_srs, dst_srs="EPSG:4326")
    gdal_helpers.reproject(mask_raster, mask_raster_4326, src_srs=src_srs, dst_srs="EPSG:4326")

    return elevation_raster_4326, mask_raster_4326


def _get_srs_for_reproject_to_4326(srid: int) -> str:
    if srid == 27700:
        # Use the 7-parameter shift rather than GDAL's default 3-parameter shift
        # for EN->long/lat transformation as it's much more accurate:
        # https://digimap.edina.ac.uk/webhelp/digimapgis/projections_and_transformations/transformations_in_gdalogr.htm
        src_srs = _7_PARAM_SHIFT
    else:
        src_srs = f"EPSG:{srid}"
    return src_srs


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


def generate_flat_roof_aspect_raster_4326(pg_uri: str,
                                          job_id: int,
                                          solar_dir: str) -> Optional[str]:
    if has_flat_roof(pg_uri, job_id):
        mask_raster = join(solar_dir, MASK_BUF1_TIF)
        srid = gdal_helpers.get_srid(mask_raster, fallback=27700)
        res = gdal_helpers.get_res(mask_raster)

        flat_roof_aspect_raster_filename = join(solar_dir, 'flat_roof_aspect.tif')
        flat_roof_aspect_sql = get_flat_roof_aspect_sql(pg_uri=pg_uri, job_id=job_id)
        create_flat_roof_aspect(flat_roof_aspect_sql, flat_roof_aspect_raster_filename, pg_uri, res=res, srid=srid)

        flat_roof_aspect_raster_4326_filename = join(solar_dir, 'flat_roof_aspect_4326.tif')
        src_srs: str = _get_srs_for_reproject_to_4326(srid)
        gdal_helpers.reproject(flat_roof_aspect_raster_filename, flat_roof_aspect_raster_4326_filename,
                               src_srs=src_srs, dst_srs="EPSG:4326")

        return flat_roof_aspect_raster_4326_filename

    return None


def create_elevation_override_raster(pg_uri: str,
                                     job_id: int,
                                     solar_dir: str,
                                     elevation_raster_4326_filename: str) -> Optional[str]:
    if has_outdated_lidar(pg_uri, job_id):
        res = gdal_helpers.get_xres_yres(elevation_raster_4326_filename)
        srid = gdal_helpers.get_srid(elevation_raster_4326_filename)

        patch_raster_filename: str = join(solar_dir, 'elevation_override.tif')
        outdated_lidar_building_h_sql: str = get_outdated_lidar_building_h_sql_4326(pg_uri=pg_uri, job_id=job_id)
        gdal_helpers.rasterize_3d(pg_uri, outdated_lidar_building_h_sql, patch_raster_filename, res, srid, "Float32")

        return patch_raster_filename
    return None