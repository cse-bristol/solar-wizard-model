import logging
import os
from os.path import join
from typing import List

import psycopg2.extras
from psycopg2.sql import Identifier

import albion_models.solar_pv.pv_gis.pv_gis_client as pv_gis_client
import albion_models.solar_pv.tables as tables
from albion_models.db_funcs import connect, sql_script_with_bindings, process_pg_uri
from albion_models.solar_pv.polygonize import aggregate_horizons
from albion_models.solar_pv.saga_gis.horizons import find_horizons
from albion_models.solar_pv.ransac.run_ransac import run_ransac


def model_solar_pv(pg_uri: str,
                   root_solar_dir: str,
                   job_id: int,
                   lidar_paths: List[str],
                   horizon_search_radius: int,
                   horizon_slices: int,
                   max_roof_slope_degrees: int,
                   min_roof_area_m: int,
                   roof_area_percent_usable: int,
                   min_roof_degrees_from_north: int,
                   flat_roof_degrees: int,
                   peak_power_per_m2: float,
                   pv_tech: str,
                   max_avg_southerly_horizon_degrees: int):

    pg_uri = process_pg_uri(pg_uri)
    _validate_params(
        lidar_paths=lidar_paths,
        horizon_search_radius=horizon_search_radius,
        horizon_slices=horizon_slices,
        max_roof_slope_degrees=max_roof_slope_degrees,
        min_roof_area_m=min_roof_area_m,
        roof_area_percent_usable=roof_area_percent_usable,
        min_roof_degrees_from_north=min_roof_degrees_from_north,
        flat_roof_degrees=flat_roof_degrees,
        peak_power_per_m2=peak_power_per_m2,
        max_avg_southerly_horizon_degrees=max_avg_southerly_horizon_degrees)

    solar_dir = join(root_solar_dir, f"job_{job_id}")
    os.makedirs(solar_dir, exist_ok=True)

    logging.info("Initialising postGIS schema...")
    _init_schema(pg_uri, job_id)

    find_horizons(
        pg_uri=pg_uri,
        job_id=job_id,
        solar_dir=solar_dir,
        lidar_paths=lidar_paths,
        horizon_search_radius=horizon_search_radius,
        horizon_slices=horizon_slices,
        masking_strategy='building')

    logging.info("Detecting roof planes...")
    run_ransac(pg_uri, job_id)

    logging.info("Aggregating horizon data by roof plane and filtering...")
    aggregate_horizons(
        pg_uri=pg_uri,
        job_id=job_id,
        horizon_slices=horizon_slices,
        max_roof_slope_degrees=max_roof_slope_degrees,
        min_roof_area_m=min_roof_area_m,
        min_roof_degrees_from_north=min_roof_degrees_from_north,
        flat_roof_degrees=flat_roof_degrees,
        max_avg_southerly_horizon_degrees=max_avg_southerly_horizon_degrees)

    logging.info("Sending requests to PV-GIS...")
    pv_gis_client.pv_gis(
        pg_uri=pg_uri,
        job_id=job_id,
        peak_power_per_m2=peak_power_per_m2,
        pv_tech=pv_tech,
        roof_area_percent_usable=roof_area_percent_usable,
        solar_dir=solar_dir)


def _init_schema(pg_uri: str, job_id: int):
    pg_conn = connect(pg_uri, cursor_factory=psycopg2.extras.DictCursor)
    try:
        sql_script_with_bindings(
            pg_conn, 'create.schema.sql', {"job_id": job_id},
            schema=Identifier(tables.schema(job_id)),
            pixel_horizons=Identifier(tables.schema(job_id), tables.PIXEL_HORIZON_TABLE),
            roof_horizons=Identifier(tables.schema(job_id), tables.ROOF_HORIZON_TABLE),
            bounds_4326=Identifier(tables.schema(job_id), tables.BOUNDS_TABLE),
            buildings=Identifier(tables.schema(job_id), tables.BUILDINGS_TABLE),
            roof_planes=Identifier(tables.schema(job_id), tables.ROOF_PLANE_TABLE),
        )
    finally:
        pg_conn.close()


def _validate_params(lidar_paths: List[str],
                     horizon_search_radius: int,
                     horizon_slices: int,
                     max_roof_slope_degrees: int,
                     min_roof_area_m: int,
                     roof_area_percent_usable: int,
                     min_roof_degrees_from_north: int,
                     flat_roof_degrees: int,
                     peak_power_per_m2: float,
                     max_avg_southerly_horizon_degrees: int):
    if not lidar_paths or len(lidar_paths) == 0:
        raise ValueError(f"No LIDAR tiles available, cannot run solar PV modelling.")
    if horizon_search_radius < 0 or horizon_search_radius > 10000:
        raise ValueError(f"horizon search radius must be between 0 and 10000, was {horizon_search_radius}")
    if horizon_slices > 64 or horizon_slices < 8:
        raise ValueError(f"horizon slices must be between 8 and 64, was {horizon_slices}")
    if max_roof_slope_degrees < 0 or max_roof_slope_degrees > 90:
        raise ValueError(f"max_roof_slope_degrees must be between 0 and 90, was {max_roof_slope_degrees}")
    if max_avg_southerly_horizon_degrees < 0 or max_avg_southerly_horizon_degrees > 90:
        raise ValueError(f"max_avg_southerly_horizon_degrees must be between 0 and 90, was {max_avg_southerly_horizon_degrees}")
    if min_roof_area_m < 0:
        raise ValueError(f"min_roof_area_m must be greater than or equal to 0, was {min_roof_area_m}")
    if roof_area_percent_usable < 0 or roof_area_percent_usable > 100:
        raise ValueError(f"roof_area_percent_usable must be between 0 and 100, was {roof_area_percent_usable}")
    if min_roof_degrees_from_north < 0 or min_roof_degrees_from_north > 180:
        raise ValueError(f"min_roof_degrees_from_north must be between 0 and 180, was {min_roof_degrees_from_north}")
    if flat_roof_degrees < 0 or flat_roof_degrees > 90:
        raise ValueError(f"flat_roof_degrees must be between 0 and 90, was {flat_roof_degrees}")
    if peak_power_per_m2 < 0:
        raise ValueError(f"peak_power_per_m2 must be greater than or equal to 0, was {peak_power_per_m2}")
