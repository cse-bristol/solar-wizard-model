# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import shutil
from os.path import join

import logging
import os
from psycopg2.sql import Identifier
from typing import Tuple, Optional

import tables as tables
import psycopg2.extras
from solar_pv import gdal_helpers
from solar_pv.db_funcs import count, connection, sql_command
from solar_pv.lidar.lidar import LIDAR_NODATA
from solar_pv.postgis import get_merged_lidar_tiles, rasters_to_postgis, \
    add_raster_constraints
import mask
from constants import POSTGIS_TILESIZE
from raster_names import MASK_27700_BUF1_TIF, MASK_27700_BUF0_TIF, ELEVATION_27700_TIF, SLOPE_27700_TIF, \
    ASPECT_27700_TIF
from roof_polygons.roof_polygons import get_flat_roof_aspect_sql, create_flat_roof_aspect, \
    has_flat_roof, get_outdated_lidar_building_h_sql_27700, has_outdated_lidar
from solar_pv.transformations import _7_PARAM_SHIFT


def generate_rasters(pg_uri: str,
                     job_id: int,
                     job_lidar_dir: str,
                     solar_dir: str,
                     horizon_search_radius: int,
                     debug_mode: bool = False) -> Tuple[str, str, str, str, float]:
    """
    Generate a single geoTIFF for the entire job area, as well as rasters for
    aspect, slope and a building mask.

    Generates all rasters in whatever SRS the input LIDAR was in (probably 27700 E/N),
    and convert to 27700 if not in 27700.
    :return: Tuple of 27700 raster file names and the resolution: (elevation, mask, slope, aspect, res)
    """

    elevation_vrt = join(solar_dir, "elev.vrt")
    elevation_raster = join(solar_dir, 'elevation.tif')
    aspect_raster = join(solar_dir, 'aspect.tif')
    slope_raster = join(solar_dir, 'slope.tif')
    mask_raster_buf1 = join(solar_dir, 'mask_buf1.tif')
    mask_raster_buf0 = join(solar_dir, 'mask_buf0.tif')

    if count(pg_uri, tables.schema(job_id), tables.MASK) > 0:
        logging.info("Not creating rasters, raster data already loaded.")
        res = gdal_helpers.get_res(elevation_vrt)
        return (join(solar_dir, ELEVATION_27700_TIF),
                join(solar_dir, MASK_27700_BUF1_TIF),
                join(solar_dir, SLOPE_27700_TIF),
                join(solar_dir, ASPECT_27700_TIF),
                res)

    with connection(pg_uri, cursor_factory=psycopg2.extras.DictCursor) as pg_conn:
        elevation_tiles = get_merged_lidar_tiles(pg_conn, job_id, solar_dir)
        gdal_helpers.create_vrt(elevation_tiles, elevation_vrt)

    srid = gdal_helpers.get_srid(elevation_vrt, fallback=27700)
    res = gdal_helpers.get_res(elevation_vrt)

    unit_dims, unit = gdal_helpers.get_srs_units(elevation_vrt)
    if unit_dims != 1.0 or unit != 'metre':
        # If this ever needs changing - the `resolution_metres` param of `create_roof_polygons()`
        # needs a resolution per metre rather than per whatever the unit of the SRS is -
        # otherwise the calculated areas/footprints of PV installations will be wrong.
        # Also, gdal_helpers.expand() assumes that the buffer arg is in the same unit as
        # the SRS, and the buffer arg is currently assumed to be metres
        raise ValueError(f"Cannot currently handle LIDAR where the SRS unit is "
                         f"not 1m: was {unit} {unit_dims}")

    logging.info("Creating raster masks...")
    # Mask with a 1m buffer around buildings, for PVGIS:
    mask_sql_buf1 = mask.buildings_mask_sql(pg_uri, job_id, buffer=1)
    mask.create_mask(mask_sql_buf1, mask_raster_buf1, pg_uri, res=res, srid=srid)
    gdal_helpers.expand(mask_raster_buf1, mask_raster_buf1, buffer=horizon_search_radius)

    # Mask with a 0m buffer around buildings, for invalid LiDAR detection:
    mask_sql_buf0 = mask.buildings_mask_sql(pg_uri, job_id, buffer=0)
    mask.create_mask(mask_sql_buf0, mask_raster_buf0, pg_uri, res=res, srid=srid)
    gdal_helpers.expand(mask_raster_buf0, mask_raster_buf0, buffer=horizon_search_radius)

    logging.info("Cropping lidar to mask dimensions...")
    gdal_helpers.crop_or_expand(elevation_vrt, mask_raster_buf0, elevation_raster,
                                adjust_resolution=True)

    logging.info("Creating aspect raster...")
    gdal_helpers.aspect(elevation_raster, aspect_raster)

    logging.info("Creating slope raster...")
    gdal_helpers.slope(elevation_raster, slope_raster)

    logging.info("Check rasters are in / convert to 27700...")
    elevation_raster, mask_raster_buf1, mask_raster_buf0, slope_raster, aspect_raster = _generate_27700_rasters(
        solar_dir, srid, elevation_raster, mask_raster_buf1, mask_raster_buf0, slope_raster, aspect_raster)

    logging.info("Loading raster data...")
    _load_rasters_to_db(pg_uri, job_id, job_lidar_dir, solar_dir, elevation_raster, aspect_raster, mask_raster_buf0)

    return elevation_raster, mask_raster_buf1, slope_raster, aspect_raster, res


def _generate_27700_rasters(solar_dir: str,
                            srid: int,
                            elevation_raster: str,
                            mask_raster_buf1: str,
                            mask_raster_buf0: str,
                            slope_raster: str,
                            aspect_raster: str):
    """Reproject in 27700 if not already in 27700
    """
    elevation_raster_27700 = join(solar_dir, ELEVATION_27700_TIF)
    mask_raster_buf1_27700 = join(solar_dir, MASK_27700_BUF1_TIF)
    mask_raster_buf0_27700 = join(solar_dir, MASK_27700_BUF0_TIF)
    slope_raster_27700 = join(solar_dir, SLOPE_27700_TIF)
    aspect_raster_27700 = join(solar_dir, ASPECT_27700_TIF)

    if srid == 27700:
        shutil.move(elevation_raster, elevation_raster_27700)
        shutil.move(mask_raster_buf1, mask_raster_buf1_27700)
        shutil.move(mask_raster_buf0, mask_raster_buf0_27700)
        shutil.move(slope_raster, slope_raster_27700)
        shutil.move(aspect_raster, aspect_raster_27700)
    else:
        dst_srs = _get_dst_srs_for_reproject(srid)
        gdal_helpers.reproject(elevation_raster, elevation_raster_27700, src_srs=f"EPSG:{srid}", dst_srs=dst_srs)
        gdal_helpers.reproject(mask_raster_buf1, mask_raster_buf1_27700, src_srs=f"EPSG:{srid}", dst_srs=dst_srs)
        gdal_helpers.reproject(mask_raster_buf0, mask_raster_buf0_27700, src_srs=f"EPSG:{srid}", dst_srs=dst_srs)
        gdal_helpers.reproject(slope_raster, slope_raster_27700, src_srs=f"EPSG:{srid}", dst_srs=dst_srs)
        gdal_helpers.reproject(aspect_raster, aspect_raster_27700, src_srs=f"EPSG:{srid}", dst_srs=dst_srs)

    return elevation_raster_27700, mask_raster_buf1_27700, mask_raster_buf0_27700, slope_raster_27700, aspect_raster_27700


def _get_dst_srs_for_reproject(src_srid: int) -> str:
    if src_srid == 4326:
        # Use the 7-parameter shift rather than GDAL's default 3-parameter shift
        # for long/lat->EN transformation as it's much more accurate:
        # https://digimap.edina.ac.uk/webhelp/digimapgis/projections_and_transformations/transformations_in_gdalogr.htm
        # Above not working Dec 22 so also -
        # https://digimap.edina.ac.uk/help/gis/transformations/transform_gdal_ogr
        dst_srs = _7_PARAM_SHIFT
    else:
        dst_srs = f"EPSG:27700"
    return dst_srs


def _copy_to_dir(src: str, dst_dir: str):
    dst_filepath = join(dst_dir, os.path.basename(src))
    shutil.copyfile(src, dst_filepath)
    return dst_filepath


def _load_rasters_to_db(pg_uri: str,
                        job_id: int,
                        job_lidar_dir: str,
                        solar_dir: str,
                        cropped_lidar: str,
                        aspect_raster: str,
                        mask_raster: str):
    with connection(pg_uri) as pg_conn:
        elevation_table = f"{tables.schema(job_id)}.{tables.ELEVATION}"
        aspect_table = f"{tables.schema(job_id)}.{tables.ASPECT}"
        mask_table = f"{tables.schema(job_id)}.{tables.MASK}"
        masked_elevation_table = f"{tables.schema(job_id)}.{tables.MASKED_ELEVATION}"
        inverse_masked_elevation_table = f"{tables.schema(job_id)}.{tables.INVERSE_MASKED_ELEVATION}"

        (cropped_lidar, aspect_raster, mask_raster) = [_copy_to_dir(r, job_lidar_dir) for r in (cropped_lidar, aspect_raster, mask_raster)]

        rasters_to_postgis(pg_conn, [cropped_lidar], elevation_table, solar_dir, POSTGIS_TILESIZE, nodata_val=LIDAR_NODATA)
        rasters_to_postgis(pg_conn, [aspect_raster], aspect_table, solar_dir, POSTGIS_TILESIZE, nodata_val=LIDAR_NODATA)
        rasters_to_postgis(pg_conn, [mask_raster], mask_table, solar_dir, POSTGIS_TILESIZE, nodata_val=0)

        sql_command(
            pg_conn,
            """
            CREATE TABLE IF NOT EXISTS {masked_elevation} AS 
            SELECT ST_MapAlgebra(e.rast, m.rast, '[rast1.val]', '32BF', 'INTERSECTION', %(nodata_str)s, %(nodata_str)s, %(nodata)s ) rast
            FROM {elevation} e
            LEFT JOIN {mask} m
            ON ST_Intersects(e.rast, m.rast);

            CREATE INDEX IF NOT EXISTS masked_elevation_idx ON {masked_elevation} USING gist (st_convexhull(rast));
            
            CREATE TABLE IF NOT EXISTS {inverse_masked_elevation} AS 
            SELECT ST_MapAlgebra(e.rast, m.rast, %(nodata_str)s, '32BF', 'INTERSECTION', %(nodata_str)s, '[rast1.val]', %(nodata)s ) rast
            FROM {elevation} e
            LEFT JOIN {mask} m
            ON ST_Intersects(e.rast, m.rast);
            
            CREATE INDEX IF NOT EXISTS inverse_masked_elevation_idx ON {inverse_masked_elevation} USING gist (st_convexhull(rast));
            """,
            {'nodata': LIDAR_NODATA, 'nodata_str': str(LIDAR_NODATA)},
            elevation=Identifier(tables.schema(job_id), tables.ELEVATION),
            mask=Identifier(tables.schema(job_id), tables.MASK),
            masked_elevation=Identifier(tables.schema(job_id), tables.MASKED_ELEVATION),
            inverse_masked_elevation=Identifier(tables.schema(job_id), tables.INVERSE_MASKED_ELEVATION),
        )

        add_raster_constraints(pg_conn, masked_elevation_table)
        add_raster_constraints(pg_conn, inverse_masked_elevation_table)


def generate_flat_roof_aspect_raster(pg_uri: str,
                                     job_id: int,
                                     solar_dir: str,
                                     mask_raster_27700_filename: str) -> Optional[str]:
    if has_flat_roof(pg_uri, job_id):
        srid = gdal_helpers.get_srid(mask_raster_27700_filename, fallback=27700)
        res = gdal_helpers.get_res(mask_raster_27700_filename)

        flat_roof_aspect_raster_filename = join(solar_dir, 'flat_roof_aspect.tif')
        flat_roof_aspect_sql = get_flat_roof_aspect_sql(pg_uri=pg_uri, job_id=job_id)
        create_flat_roof_aspect(flat_roof_aspect_sql, flat_roof_aspect_raster_filename, pg_uri, res=res, srid=srid)

        return flat_roof_aspect_raster_filename

    return None


def create_elevation_override_raster(pg_uri: str,
                                     job_id: int,
                                     solar_dir: str,
                                     elevation_raster_27700_filename: str) -> Optional[str]:
    if has_outdated_lidar(pg_uri, job_id):
        srid = gdal_helpers.get_srid(elevation_raster_27700_filename, fallback=27700)
        res = gdal_helpers.get_xres_yres(elevation_raster_27700_filename)

        patch_raster_filename: str = join(solar_dir, 'elevation_override.tif')
        outdated_lidar_building_h_sql: str = get_outdated_lidar_building_h_sql_27700(pg_uri=pg_uri, job_id=job_id)
        gdal_helpers.rasterize_3d(pg_uri, outdated_lidar_building_h_sql, patch_raster_filename, res, srid, "Float32")

        return patch_raster_filename
    return None
