import logging
import os
import shutil
from os.path import join

import psycopg2.extras
from psycopg2.sql import Identifier

import albion_models.solar_pv.tables as tables
from albion_models.db_funcs import process_pg_uri, \
    connection, sql_command, sql_script
from albion_models.postgis import raster_tile_coverage_count
from albion_models.solar_pv.outdated_lidar.outdated_lidar_check import check_lidar
from albion_models.solar_pv.panels.panels import place_panels
from albion_models.solar_pv.pvgis.pvgis import pvgis
from albion_models.solar_pv.ransac.run_ransac import run_ransac
from albion_models.solar_pv.rasters import generate_rasters


def model_solar_pv(pg_uri: str,
                   root_solar_dir: str,
                   lidar_dir: str,
                   job_id: int,
                   horizon_search_radius: int,
                   horizon_slices: int,
                   max_roof_slope_degrees: int,
                   min_roof_area_m: int,
                   min_roof_degrees_from_north: int,
                   flat_roof_degrees: int,
                   peak_power_per_m2: float,
                   pv_tech: str,
                   panel_width_m: float,
                   panel_height_m: float,
                   panel_spacing_m: float,
                   large_building_threshold: float,
                   min_dist_to_edge_m: float,
                   min_dist_to_edge_large_m: float,
                   debug_mode: bool):

    pg_uri = process_pg_uri(pg_uri)
    _validate_params(
        horizon_search_radius=horizon_search_radius,
        horizon_slices=horizon_slices,
        max_roof_slope_degrees=max_roof_slope_degrees,
        min_roof_area_m=min_roof_area_m,
        min_roof_degrees_from_north=min_roof_degrees_from_north,
        flat_roof_degrees=flat_roof_degrees,
        peak_power_per_m2=peak_power_per_m2,
        panel_width_m=panel_width_m,
        panel_height_m=panel_height_m)

    solar_dir = join(root_solar_dir, f"job_{job_id}")
    os.makedirs(solar_dir, exist_ok=True)

    logging.info("Initialising postGIS schema...")
    _init_schema(pg_uri, job_id)

    if _skip(pg_uri, job_id):
        return

    job_lidar_dir = join(lidar_dir, f"job_{job_id}")
    os.makedirs(job_lidar_dir, exist_ok=True)

    logging.info("Generating and loading rasters...")
    elevation_raster_27700, mask_raster_27700, slope_raster_27700, aspect_raster_27700, res = generate_rasters(
        pg_uri=pg_uri,
        job_id=job_id,
        job_lidar_dir=job_lidar_dir,
        solar_dir=solar_dir,
        horizon_search_radius=horizon_search_radius,
        debug_mode=debug_mode)

    logging.info("Checking for outdated LiDAR and missing LiDAR coverage...")
    check_lidar(pg_uri, job_id, resolution_metres=res)

    logging.info("Detecting roof planes...")
    run_ransac(pg_uri, job_id,
               max_roof_slope_degrees=max_roof_slope_degrees,
               min_roof_area_m=min_roof_area_m,
               min_roof_degrees_from_north=min_roof_degrees_from_north,
               flat_roof_degrees=flat_roof_degrees,
               large_building_threshold=large_building_threshold,
               min_dist_to_edge_m=min_dist_to_edge_m,
               min_dist_to_edge_large_m=min_dist_to_edge_large_m,
               resolution_metres=res,
               panel_width_m=panel_width_m,
               panel_height_m=panel_height_m)

    logging.info("Adding individual PV panels...")
    place_panels(
        pg_uri=pg_uri,
        job_id=job_id,
        min_roof_area_m=min_roof_area_m,
        panel_width_m=panel_width_m,
        panel_height_m=panel_height_m,
        panel_spacing_m=panel_spacing_m)

    logging.info("Running PV-GIS...")
    pvgis(pg_uri=pg_uri,
          job_id=job_id,
          solar_dir=solar_dir,
          job_lidar_dir=job_lidar_dir,
          resolution_metres=res,
          pv_tech=pv_tech,
          horizon_search_radius=horizon_search_radius,
          horizon_slices=horizon_slices,
          peak_power_per_m2=peak_power_per_m2,
          flat_roof_degrees=flat_roof_degrees,
          elevation_raster=elevation_raster_27700,
          mask_raster=mask_raster_27700,
          slope_raster=slope_raster_27700,
          aspect_raster=aspect_raster_27700,
          debug_mode=debug_mode)

    if not debug_mode:
        logging.info("Removing temp dir...")
        shutil.rmtree(solar_dir)
        logging.info("Dropping schema...")
        _drop_schema(pg_uri, job_id)
        logging.info("Removing job LiDAR dir...")
        shutil.rmtree(job_lidar_dir)
    else:
        logging.info("Debug mode: not removing temp dir or dropping schema.")


def _init_schema(pg_uri: str, job_id: int):
    with connection(pg_uri, cursor_factory=psycopg2.extras.DictCursor) as pg_conn:
        sql_script(
            pg_conn, 'pv/create.schema.sql', {"job_id": job_id},
            schema=Identifier(tables.schema(job_id)),
            bounds_4326=Identifier(tables.schema(job_id), tables.BOUNDS_TABLE),
            buildings=Identifier(tables.schema(job_id), tables.BUILDINGS_TABLE),
            roof_polygons=Identifier(tables.schema(job_id), tables.ROOF_POLYGON_TABLE),
            panel_polygons=Identifier(tables.schema(job_id), tables.PANEL_POLYGON_TABLE),
            elevation=Identifier(tables.schema(job_id), tables.ELEVATION),
            aspect=Identifier(tables.schema(job_id), tables.ASPECT),
            mask=Identifier(tables.schema(job_id), tables.MASK),
        )


def _drop_schema(pg_uri: str, job_id: int):
    with connection(pg_uri, cursor_factory=psycopg2.extras.DictCursor) as pg_conn:
        sql_script(
            pg_conn, 'pv/drop.schema.sql', {"job_id": job_id},
            schema=Identifier(tables.schema(job_id)),
        )


def _skip(pg_uri: str, job_id: int) -> bool:
    with connection(pg_uri, cursor_factory=psycopg2.extras.DictCursor) as pg_conn:
        building_count = sql_command(
            pg_conn,
            "SELECT COUNT(*) FROM {buildings}",
            buildings=Identifier(tables.schema(job_id), tables.BUILDINGS_TABLE))
        if building_count == 0:
            logging.info("skipping PV job, no buildings found")
            return True

        tile_cov_count = raster_tile_coverage_count(pg_conn, job_id)
        if tile_cov_count == 0:
            logging.info("skipping PV job, no LiDAR tiles intersect the job bounds")
            sql_command(
                pg_conn,
                """
                UPDATE {buildings} SET exclusion_reason = 'NO_LIDAR_COVERAGE';
                
                INSERT INTO models.pv_building
                SELECT %(job_id)s, toid, exclusion_reason, height
                FROM {buildings};
                """,
                {"job_id": job_id},
                buildings=Identifier(tables.schema(job_id), tables.BUILDINGS_TABLE),
            )
            return True

        return False


def _validate_params(horizon_search_radius: int,
                     horizon_slices: int,
                     max_roof_slope_degrees: int,
                     min_roof_area_m: int,
                     min_roof_degrees_from_north: int,
                     flat_roof_degrees: int,
                     peak_power_per_m2: float,
                     panel_width_m: float,
                     panel_height_m: float):
    if horizon_search_radius < 0 or horizon_search_radius > 10000:
        raise ValueError(f"horizon search radius must be between 0 and 10000, was {horizon_search_radius}")
    if horizon_slices > 64 or horizon_slices < 8:
        raise ValueError(f"horizon slices must be between 8 and 64, was {horizon_slices}")
    if max_roof_slope_degrees < 0 or max_roof_slope_degrees > 90:
        raise ValueError(f"max_roof_slope_degrees must be between 0 and 90, was {max_roof_slope_degrees}")
    if min_roof_area_m < 0:
        raise ValueError(f"min_roof_area_m must be greater than or equal to 0, was {min_roof_area_m}")
    if min_roof_degrees_from_north < 0 or min_roof_degrees_from_north > 180:
        raise ValueError(f"min_roof_degrees_from_north must be between 0 and 180, was {min_roof_degrees_from_north}")
    if flat_roof_degrees < 0 or flat_roof_degrees > 90:
        raise ValueError(f"flat_roof_degrees must be between 0 and 90, was {flat_roof_degrees}")
    if peak_power_per_m2 < 0:
        raise ValueError(f"peak_power_per_m2 must be greater than or equal to 0, was {peak_power_per_m2}")
    if panel_width_m <= 0:
        raise ValueError(f"panel_width_m must be greater than 0, was {panel_width_m}")
    if panel_height_m <= 0:
        raise ValueError(f"panel_height_m must be greater than 0, was {panel_height_m}")
    if os.environ.get("PVGIS_DATA_TAR_FILE_DIR", None) is None:
        raise ValueError(f"env var PVGIS_DATA_TAR_FILE_DIR must be set")
    if os.environ.get("PVGIS_GRASS_DBASE_DIR", None) is None:
        raise ValueError(f"env var PVGIS_GRASS_DBASE_DIR must be set")
