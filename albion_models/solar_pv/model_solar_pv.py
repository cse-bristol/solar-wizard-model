import os
import subprocess
from os.path import join
from typing import List

import psycopg2.extras
from psycopg2.sql import SQL, Identifier, Literal

import albion_models.solar_pv.pv_gis.pv_gis_client as pv_gis_client
import albion_models.solar_pv.tables as tables
from albion_models.db_funcs import sql_script, connect, copy_csv, sql_script_with_bindings, \
    process_pg_uri
from albion_models.solar_pv.crop import crop_to_mask
from albion_models.solar_pv.polygonize import generate_aspect_polygons, aggregate_horizons
from albion_models.solar_pv.saga_gis.horizons import get_horizons, load_horizons_to_db


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
                   pv_tech: str):

    pg_uri = process_pg_uri(pg_uri)
    _validate_params(
        lidar_paths, horizon_search_radius, horizon_slices, max_roof_slope_degrees, min_roof_area_m,
        roof_area_percent_usable, min_roof_degrees_from_north, flat_roof_degrees, peak_power_per_m2)

    solar_dir = join(root_solar_dir, f"job_{job_id}")
    os.makedirs(solar_dir, exist_ok=True)

    print("Creating vrt...")
    vrt_file = join(solar_dir, 'tiles.vrt')
    _run(f"gdalbuildvrt {vrt_file} {' '.join(lidar_paths)}")

    print("Initialising postGIS schema...")
    _init_schema(pg_uri, job_id)

    print("Creating raster mask from mastermap.buildings polygon...")
    mask_file = _create_mask(job_id, solar_dir, pg_uri)
    cropped_lidar = join(solar_dir, 'cropped_lidar.tif')
    print("Cropping lidar to mask dimensions...")
    crop_to_mask(vrt_file, mask_file, cropped_lidar)

    print("Using 320-albion-saga-gis to find horizons...")
    horizons_csv = join(solar_dir, 'horizons.csv')
    get_horizons(cropped_lidar, solar_dir, mask_file, horizons_csv, horizon_search_radius, horizon_slices)
    print("Loading horizon data into postGIS...")
    load_horizons_to_db(pg_uri, job_id, horizons_csv, horizon_slices)

    print("Creating aspect raster...")
    aspect_file = join(solar_dir, 'aspect.tif')
    _run(f"gdaldem aspect {cropped_lidar} {aspect_file} -of GTiff -b 1 -zero_for_flat")

    print("Polygonising aspect raster...")
    generate_aspect_polygons(mask_file, aspect_file, pg_uri, job_id, solar_dir)

    print("Intersecting roof polygons with buildings, aggregating horizon data and filtering...")
    aggregate_horizons(pg_uri, job_id, horizon_slices, max_roof_slope_degrees, min_roof_area_m, min_roof_degrees_from_north, flat_roof_degrees)

    print("Sending requests to PV-GIS...")
    solar_pv_csv = _pv_gis(pg_uri, job_id, peak_power_per_m2, pv_tech, roof_area_percent_usable, solar_dir)

    print("Loading PV data into albion...")
    _write_results_to_db(pg_uri, job_id, solar_pv_csv)


def _run(command: str):
    res = subprocess.run(command, capture_output=True, text=True, shell=True)
    print(res.stdout)
    print(res.stderr)
    if res.returncode != 0:
        raise ValueError(res.stderr)


def _create_mask(job_id: int, solar_dir: str, pg_uri: str) -> str:
    """
    Create a raster mask from OS mastermap buildings that fall within the bounds
    of the job. Pixels inside a building will be 1, otherwise 0.
    """
    pg_conn = connect(pg_uri, cursor_factory=psycopg2.extras.DictCursor)
    try:
        with pg_conn.cursor() as cursor:
            cursor.execute(SQL(
                """
                CREATE TABLE {bounds_4326} AS 
                SELECT job_id, ST_Transform(bounds, 4326) AS bounds 
                FROM models.job_queue;
                CREATE INDEX ON {bounds_4326} using gist (bounds);
                """).format(
                    bounds_4326=Identifier(tables.schema(job_id), tables.BOUNDS_TABLE)))

            pg_conn.commit()
        mask_sql = SQL(
            """
            SELECT ST_Transform(b.geom_4326, 27700) 
            FROM mastermap.building b 
            LEFT JOIN {bounds_4326} q 
            ON ST_Intersects(b.geom_4326, q.bounds) 
            WHERE q.job_id={job_id}
            """).format(
                bounds_4326=Identifier(tables.schema(job_id), tables.BOUNDS_TABLE),
                job_id=Literal(job_id)).as_string(pg_conn)
    finally:
        pg_conn.close()

    mask_file = join(solar_dir, 'mask.tif')
    _run(f"""
        gdal_rasterize 
        -sql '{mask_sql}' 
        -burn 1 -tr 1 1 
        -init 0 -ot Int16 
        -of GTiff -a_srs EPSG:27700 
        "PG:{pg_uri}" 
        {mask_file}
        """.replace("\n", " "))
    return mask_file


def _init_schema(pg_uri: str, job_id: int):
    pg_conn = connect(pg_uri, cursor_factory=psycopg2.extras.DictCursor)
    try:
        sql_script(
            pg_conn, 'create.schema.sql',
            schema=Identifier(tables.schema(job_id)),
            pixel_horizons=Identifier(tables.schema(job_id), tables.PIXEL_HORIZON_TABLE),
            roof_polygons=Identifier(tables.schema(job_id), tables.ROOF_POLYGON_TABLE),
            roof_horizons=Identifier(tables.schema(job_id), tables.ROOF_HORIZON_TABLE),
        )
    finally:
        pg_conn.close()


def _pv_gis(pg_uri: str, job_id: int, peak_power_per_m2: float, pv_tech: str, roof_area_percent_usable: int, solar_dir: str) -> str:
    solar_pv_csv = join(solar_dir, 'solar_pv.csv')
    pg_conn = connect(pg_uri, cursor_factory=psycopg2.extras.DictCursor)
    try:
        with pg_conn.cursor() as cursor:
            cursor.execute(SQL("SELECT * FROM {roof_horizons}").format(
                roof_horizons=Identifier(tables.schema(job_id), tables.ROOF_HORIZON_TABLE))
            )
            rows = cursor.fetchall()
            pg_conn.commit()
            print(f"{len(rows)} queries to send:")
            pv_gis_client.solar_pv_estimate(rows, peak_power_per_m2, pv_tech, roof_area_percent_usable, solar_pv_csv)
    finally:
        pg_conn.close()
    return solar_pv_csv


def _write_results_to_db(pg_uri: str, job_id: int, csv_file: str):
    pg_conn = connect(pg_uri)
    try:
        sql_script(pg_conn, 'create.solar-pv.sql', solar_pv=Identifier(tables.schema(job_id), tables.SOLAR_PV_TABLE))
        copy_csv(pg_conn, csv_file, f'{tables.schema(job_id)}.{tables.SOLAR_PV_TABLE}')
        sql_script_with_bindings(
            pg_conn, 'post-load.solar-pv.sql', {"job_id": job_id},
            solar_pv=Identifier(tables.schema(job_id), tables.SOLAR_PV_TABLE),
            roof_horizons=Identifier(tables.schema(job_id), tables.ROOF_HORIZON_TABLE),
            job_view=Identifier(f"solar_pv_job_{job_id}")
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
                     peak_power_per_m2: float):
    if not lidar_paths or len(lidar_paths) == 0:
        raise ValueError(f"No LIDAR tiles available, cannot run solar PV modelling.")
    if horizon_search_radius < 0 or horizon_search_radius > 10000:
        raise ValueError(f"horizon search radius must be between 0 and 10000, was {horizon_search_radius}")
    if horizon_slices > 64 or horizon_slices < 8:
        raise ValueError(f"horizon slices must be between 8 and 64, was {horizon_slices}")
    if max_roof_slope_degrees < 0 or max_roof_slope_degrees > 90:
        raise ValueError(f"max_roof_slope_degrees must be between 0 and 90, was {max_roof_slope_degrees}")
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
