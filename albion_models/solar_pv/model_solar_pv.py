import logging
import os
import shutil
from os.path import join

import psycopg2.extras
from psycopg2.sql import Identifier

import albion_models.solar_pv.pv_gis.pv_gis_client as pv_gis_client
import albion_models.solar_pv.tables as tables
from albion_models.db_funcs import connect, sql_script_with_bindings, process_pg_uri
from albion_models.gdal_helpers import get_res
from albion_models.solar_pv.rasters import generate_rasters
from albion_models.solar_pv.roof_polygons import create_roof_polygons
from albion_models.solar_pv.lidar_check import check_lidar
from albion_models.solar_pv.panels import add_panels
from albion_models.solar_pv.ransac.run_ransac import run_ransac


def model_solar_pv(pg_uri: str,
                   root_solar_dir: str,
                   job_id: int,
                   lidar_vrt_file: str,
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
        lidar_vrt_file=lidar_vrt_file,
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

    res = get_res(lidar_vrt_file)
    elevation_raster, aspect_raster, slope_raster, mask_raster = generate_rasters(
        pg_uri=pg_uri,
        job_id=job_id,
        solar_dir=solar_dir,
        lidar_vrt_file=lidar_vrt_file,
        debug_mode=debug_mode)

    logging.info("Checking for outdated LiDAR and missing LiDAR coverage...")
    check_lidar(pg_uri, job_id)

    logging.info("Detecting roof planes...")
    run_ransac(pg_uri, job_id, resolution_metres=res)

    logging.info("Creating and filtering roof polygons...")
    create_roof_polygons(
        pg_uri=pg_uri,
        job_id=job_id,
        max_roof_slope_degrees=max_roof_slope_degrees,
        min_roof_area_m=min_roof_area_m,
        min_roof_degrees_from_north=min_roof_degrees_from_north,
        flat_roof_degrees=flat_roof_degrees,
        large_building_threshold=large_building_threshold,
        min_dist_to_edge_m=min_dist_to_edge_m,
        min_dist_to_edge_large_m=min_dist_to_edge_large_m,
        resolution_metres=res)

    logging.info("Adding individual PV panels...")
    add_panels(
        pg_uri=pg_uri,
        job_id=job_id,
        min_roof_area_m=min_roof_area_m,
        panel_width_m=panel_width_m,
        panel_height_m=panel_height_m,
        panel_spacing_m=panel_spacing_m)

    logging.info("Sending requests to PV-GIS...")
    pv_gis_client.pv_gis(
        pg_uri=pg_uri,
        job_id=job_id,
        peak_power_per_m2=peak_power_per_m2,
        pv_tech=pv_tech,
        solar_dir=solar_dir)

    if not debug_mode:
        logging.info("Removing temp dir...")
        shutil.rmtree(solar_dir)
        logging.info("Dropping schema...")
        _drop_schema(pg_uri, job_id)
    else:
        logging.info("Debug mode: not removing temp dir or dropping schema.")


def _init_schema(pg_uri: str, job_id: int):
    pg_conn = connect(pg_uri, cursor_factory=psycopg2.extras.DictCursor)
    try:
        sql_script_with_bindings(
            pg_conn, 'pv/create.schema.sql', {"job_id": job_id},
            schema=Identifier(tables.schema(job_id)),
            bounds_4326=Identifier(tables.schema(job_id), tables.BOUNDS_TABLE),
            buildings=Identifier(tables.schema(job_id), tables.BUILDINGS_TABLE),
            roof_planes=Identifier(tables.schema(job_id), tables.ROOF_PLANE_TABLE),
            building_exclusion_reasons=Identifier(tables.schema(job_id), tables.BUILDING_EXCLUSION_REASONS_TABLE),
        )
    finally:
        pg_conn.close()


def _drop_schema(pg_uri: str, job_id: int):
    pg_conn = connect(pg_uri, cursor_factory=psycopg2.extras.DictCursor)
    try:
        sql_script_with_bindings(
            pg_conn, 'pv/drop.schema.sql', {"job_id": job_id},
            schema=Identifier(tables.schema(job_id)),
        )
    finally:
        pg_conn.close()


def _validate_params(lidar_vrt_file: str,
                     horizon_search_radius: int,
                     horizon_slices: int,
                     max_roof_slope_degrees: int,
                     min_roof_area_m: int,
                     min_roof_degrees_from_north: int,
                     flat_roof_degrees: int,
                     peak_power_per_m2: float,
                     panel_width_m: float,
                     panel_height_m: float):
    if not lidar_vrt_file or not os.path.exists(lidar_vrt_file):
        raise ValueError("No LIDAR tiles available, cannot run solar PV modelling.")
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
