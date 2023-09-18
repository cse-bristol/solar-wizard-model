# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import logging
import os
import shutil
from os.path import join
from typing import List

import psycopg2.extras
from psycopg2.sql import Identifier

from solar_pv import tables
from solar_pv.db_funcs import process_pg_uri, \
    connection, sql_command, sql_script
from solar_pv.postgis import raster_tile_coverage_count
from solar_pv.outdated_lidar.outdated_lidar_check import check_lidar
from solar_pv.pvgis.pvgis import pvgis
from solar_pv.roof_detection.detect_roofs import detect_roofs
from solar_pv.rasters import generate_rasters


def model_solar_pv(pg_uri: str,
                   root_solar_dir: str,
                   lidar_dir: str,
                   job_id: int,
                   job_bounds_27700: str,
                   horizon_search_radius: int,
                   horizon_slices: int,
                   max_roof_slope_degrees: int,
                   min_roof_area_m: int,
                   min_roof_degrees_from_north: int,
                   flat_roof_degrees: int,
                   peak_power_per_m2: float,
                   pv_tech: str,
                   min_dist_to_edge_m: float,
                   debug_mode: bool):

    pg_uri = _validate_str(pg_uri, "pg_uri")
    root_solar_dir = _validate_str(root_solar_dir, "root_solar_dir")
    lidar_dir = _validate_str(lidar_dir, "lidar_dir")
    pv_tech = _validate_str(pv_tech, "pv_tech", allowed=["crystSi", "CdTe"])
    job_id = _validate_int(job_id, "job_id")
    horizon_search_radius = _validate_int(horizon_search_radius, "horizon_search_radius", 0, 10000)
    horizon_slices = _validate_int(horizon_slices, "horizon_slices", 8, 128)
    max_roof_slope_degrees = _validate_int(max_roof_slope_degrees, "max_roof_slope_degrees", 0, 90)
    min_roof_area_m = _validate_int(min_roof_area_m, "min_roof_area_m", 0)
    min_roof_degrees_from_north = _validate_int(min_roof_degrees_from_north, "min_roof_degrees_from_north", 0, 180)
    flat_roof_degrees = _validate_int(flat_roof_degrees, "flat_roof_degrees", 0, 90)
    peak_power_per_m2 = _validate_float(peak_power_per_m2, "peak_power_per_m2", 0)
    min_dist_to_edge_m = _validate_float(min_dist_to_edge_m, "min_dist_to_edge_m", 0)

    _validate_env_var("PVGIS_DATA_TAR_FILE_DIR")
    _validate_env_var("PVGIS_GRASS_DBASE_DIR")

    pg_uri = process_pg_uri(pg_uri)

    solar_dir = join(root_solar_dir, f"job_{job_id}")
    os.makedirs(solar_dir, exist_ok=True)

    logging.info("Initialising postGIS schema...")
    _init_schema(pg_uri, job_id, job_bounds_27700)

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
    detect_roofs(pg_uri, job_id,
                 max_roof_slope_degrees=max_roof_slope_degrees,
                 min_roof_area_m=min_roof_area_m,
                 min_roof_degrees_from_north=min_roof_degrees_from_north,
                 flat_roof_degrees=flat_roof_degrees,
                 min_dist_to_edge_m=min_dist_to_edge_m,
                 resolution_metres=res)

    # logging.info("Adding individual PV panels...")
    # place_panels(
    #     pg_uri=pg_uri,
    #     job_id=job_id,
    #     min_roof_area_m=min_roof_area_m,
    #     panel_width_m=panel_width_m,
    #     panel_height_m=panel_height_m,
    #     panel_spacing_m=panel_spacing_m)

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


def _init_schema(pg_uri: str, job_id: int, job_bounds_27700: str):
    with connection(pg_uri, cursor_factory=psycopg2.extras.DictCursor) as pg_conn:
        sql_script(pg_conn, 'create.db.sql')
        sql_script(
            pg_conn,
            'create.schema.sql',
            {"job_id": job_id, "job_bounds_27700": job_bounds_27700},
            schema=Identifier(tables.schema(job_id)),
            bounds_27700=Identifier(tables.schema(job_id), tables.BOUNDS_TABLE),
            buildings=Identifier(tables.schema(job_id), tables.BUILDINGS_TABLE),
            roof_polygons=Identifier(tables.schema(job_id), tables.ROOF_POLYGON_TABLE),
            panel_polygons=Identifier(tables.schema(job_id), tables.PANEL_POLYGON_TABLE),
            elevation=Identifier(tables.schema(job_id), tables.ELEVATION),
            aspect=Identifier(tables.schema(job_id), tables.ASPECT),
            slope=Identifier(tables.schema(job_id), tables.SLOPE),
            mask=Identifier(tables.schema(job_id), tables.MASK),
        )


def _drop_schema(pg_uri: str, job_id: int):
    with connection(pg_uri) as pg_conn:
        sql_command(
            pg_conn,
            'DROP SCHEMA IF EXISTS {schema} CASCADE',
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


def _validate_str(val: str, name: str, allowed: List[str] = None) -> str:
    if val is None:
        raise ValueError(f"parameter {name} was None")
    val = str(val)
    if allowed is not None and val not in allowed:
        raise ValueError(f"parameter {name} not in {allowed}, was {val}")
    return val


def _validate_int(val: int, name: str, minval: int = None, maxval: int = None) -> int:
    if val is None:
        raise ValueError(f"parameter {name} was None")
    val = int(val)
    if minval is not None and val < minval:
        raise ValueError(f"parameter {name} must be greater or equal to {minval}, was {val}")
    if maxval is not None and val > maxval:
        raise ValueError(f"parameter {name} must be less than or equal to {maxval}, was {val}")
    return val


def _validate_float(val: float, name: str, minval: float = None, maxval: float = None) -> float:
    if val is None:
        raise ValueError(f"parameter {name} was None")
    val = float(val)
    if minval is not None and val < minval:
        raise ValueError(f"parameter {name} must be greater or equal to {minval}, was {val}")
    if maxval is not None and val > maxval:
        raise ValueError(f"parameter {name} must be less than or equal to {maxval}, was {val}")
    return val


def _validate_env_var(name: str):
    if os.environ.get(name, None) is None:
        raise ValueError(f"env var {name} must be set")
