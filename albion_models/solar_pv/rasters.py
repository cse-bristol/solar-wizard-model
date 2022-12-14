import shutil
from os.path import join

import logging
import os
from psycopg2.sql import Identifier, Literal
from typing import Tuple, Optional, List, Callable

import albion_models.solar_pv.tables as tables
import psycopg2.extras
from albion_models import gdal_helpers
from albion_models.db_funcs import sql_script, copy_csv, count, connection
from albion_models.postgis import get_merged_lidar_tiles
from albion_models.solar_pv import mask
from albion_models.solar_pv.constants import LIDAR_DOWNSCALE_TO
from albion_models.solar_pv.raster_names import MASK_27700_TIF, MASK_BUF1_TIF, ELEVATION_27700_TIF, SLOPE_27700_TIF, \
    ASPECT_27700_TIF
from albion_models.solar_pv.roof_polygons.roof_polygons import get_flat_roof_aspect_sql, create_flat_roof_aspect, \
    has_flat_roof, get_outdated_lidar_building_h_sql_27700, has_outdated_lidar
from albion_models.transformations import _7_PARAM_SHIFT


def generate_rasters(pg_uri: str,
                     job_id: int,
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
    mask_raster_buf1 = join(solar_dir, MASK_BUF1_TIF)
    mask_raster_buf3 = join(solar_dir, 'mask_buf3.tif')

    if count(pg_uri, tables.schema(job_id), tables.LIDAR_PIXEL_TABLE) > 0:
        logging.info("Not creating rasters, raster data already loaded.")
        res = gdal_helpers.get_res(elevation_vrt)
        return (join(solar_dir, ELEVATION_27700_TIF),
                join(solar_dir, MASK_27700_TIF),
                join(solar_dir, SLOPE_27700_TIF),
                join(solar_dir, ASPECT_27700_TIF),
                res)

    with connection(pg_uri, cursor_factory=psycopg2.extras.DictCursor) as pg_conn:
        elevation_tiles = get_merged_lidar_tiles(pg_conn, job_id, solar_dir)
        gdal_helpers.create_vrt(elevation_tiles, elevation_vrt)

    srid = gdal_helpers.get_srid(elevation_vrt, fallback=27700)
    res = gdal_helpers.get_res(elevation_vrt)

    if res < LIDAR_DOWNSCALE_TO:
        res = LIDAR_DOWNSCALE_TO

    unit_dims, unit = gdal_helpers.get_srs_units(elevation_vrt)
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
    gdal_helpers.crop_or_expand(elevation_vrt, mask_raster_buf3, elevation_raster,
                                adjust_resolution=True)

    logging.info("Creating aspect raster...")
    gdal_helpers.aspect(elevation_raster, aspect_raster)

    logging.info("Creating slope raster...")
    gdal_helpers.slope(elevation_raster, slope_raster)

    logging.info("Check rasters are in / convert to 27700...")
    cropped_lidar_27700, mask_raster_27700, slope_raster_27700, aspect_raster_27700 = _generate_27700_rasters(
        solar_dir, srid, elevation_raster, mask_raster_buf1, slope_raster, aspect_raster)

    logging.info("Loading raster data...")
    _load_rasters_to_db(pg_uri, job_id, srid, res, solar_dir, elevation_raster,
                        aspect_raster, slope_raster, mask_raster_buf3, debug_mode)

    return cropped_lidar_27700, mask_raster_27700, slope_raster_27700, aspect_raster_27700, res


def _generate_27700_rasters(solar_dir: str,
                            srid: int,
                            elevation_raster: str,
                            mask_raster: str,
                            slope_raster: str,
                            aspect_raster: str):
    """Reproject in 27700 if not already in 27700
    """
    elevation_raster_27700 = join(solar_dir, ELEVATION_27700_TIF)
    mask_raster_27700 = join(solar_dir, MASK_27700_TIF)
    slope_raster_27700 = join(solar_dir, SLOPE_27700_TIF)
    aspect_raster_27700 = join(solar_dir, ASPECT_27700_TIF)

    if srid == 27700:
        shutil.copyfile(elevation_raster, elevation_raster_27700)
        shutil.copyfile(mask_raster, mask_raster_27700)
        shutil.copyfile(slope_raster, slope_raster_27700)
        shutil.copyfile(aspect_raster, aspect_raster_27700)
    else:
        dst_srs = _get_dst_srs_for_reproject(srid)
        gdal_helpers.reproject(elevation_raster, elevation_raster_27700, src_srs=f"EPSG:{srid}", dst_srs=dst_srs)
        gdal_helpers.reproject(mask_raster, mask_raster_27700, src_srs=f"EPSG:{srid}", dst_srs=dst_srs)
        gdal_helpers.reproject(slope_raster, slope_raster_27700, src_srs=f"EPSG:{srid}", dst_srs=dst_srs)
        gdal_helpers.reproject(aspect_raster, aspect_raster_27700, src_srs=f"EPSG:{srid}", dst_srs=dst_srs)

    return elevation_raster_27700, mask_raster_27700, slope_raster_27700, aspect_raster_27700


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
    with connection(pg_uri) as pg_conn:
        schema = tables.schema(job_id)
        lidar_pixels_table = tables.LIDAR_PIXEL_TABLE

        sql_script(
            pg_conn, 'pv/create.lidar-pixels.sql',
            lidar_pixels=Identifier(schema, lidar_pixels_table),
            aspect_pixels=Identifier(schema, "aspect_pixels"),
            slope_pixels=Identifier(schema, "slope_pixels"),
        )

        copy_rasters(pg_conn, solar_dir,
                     rasters=[cropped_lidar, aspect_raster, slope_raster],
                     table=f"{schema}.{lidar_pixels_table}",
                     mask_raster=mask_raster,
                     include_nans=False,
                     debug_mode=debug_mode)

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


def copy_rasters(pg_conn,
                 solar_dir: str,
                 rasters: List[str],
                 table: str,
                 mask_raster: str = None,
                 include_nans: bool = True,
                 value_transformer: Callable[[List[float]], str] = None,
                 debug_mode: bool = False,):
    csv_file = join(solar_dir, f'temp-{table}.csv')
    gdal_helpers.rasters_to_csv(
        raster_files=rasters,
        csv_out=csv_file,
        mask_raster=mask_raster,
        include_nans=include_nans,
        value_transformer=value_transformer)
    copy_csv(pg_conn, csv_file, table)
    if not debug_mode:
        try:
            os.remove(csv_file)
        except OSError:
            pass


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
