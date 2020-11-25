import os
import subprocess
from os.path import join
from typing import List

import psycopg2.extras
from psycopg2.sql import SQL, Identifier

import solar_pv.pv_gis.pv_gis_client as pv_gis_client
import solar_pv.tables as tables
from solar_pv.db_funcs import sql_script, connect
from solar_pv.crop import crop_to_mask
from solar_pv.polygonize import generate_aspect_polygons, aggregate_horizons
from solar_pv.saga_gis.horizons import get_horizons, load_horizons_to_db


def model_solar_pv(pg_uri: str,
                   root_solar_dir: str,
                   job_id: int,
                   lidar_paths: List[str],
                   horizon_search_radius: int,
                   horizon_slices: int,
                   max_roof_slope_degrees: int,
                   min_roof_area_m: int,
                   min_roof_degrees_from_north: int,
                   flat_roof_degrees: int,
                   peak_power_per_m2: float,
                   pv_tech: str):

    solar_dir = join(root_solar_dir, f"job_{job_id}")
    os.makedirs(solar_dir, exist_ok=True)

    print("Creating vrt...")
    vrt_file = join(solar_dir, 'tiles.vrt')
    _run(f"gdalbuildvrt {vrt_file} {' '.join(lidar_paths)}")

    print("Creating raster mask from mastermap.buildings polygon...")
    mask_file = _create_mask(job_id, solar_dir, pg_uri)
    cropped_lidar = join(solar_dir, 'cropped_lidar.tif')
    print("Cropping lidar to mask dimensions...")
    crop_to_mask(vrt_file, mask_file, cropped_lidar)

    print("Initialising postGIS schema...")
    _init_schema(pg_uri, job_id)

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
    _pv_gis(pg_uri, job_id, peak_power_per_m2, pv_tech, solar_dir)


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
    job_id = int(job_id)
    mask_sql = f"""
        SELECT ST_Transform(geom_4326, 27700) 
        FROM mastermap.building 
        WHERE ST_Intersects(geom_4326, ST_Transform((
            SELECT bounds FROM models.job_queue WHERE job_id={job_id} LIMIT 1
        ), 4326))
    """
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


def _pv_gis(pg_uri: str, job_id: int, peak_power_per_m2: float, pv_tech: str, solar_dir: str):
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
            pv_gis_client.solar_pv_estimate(rows, peak_power_per_m2, pv_tech, solar_pv_csv)
    finally:
        pg_conn.close()
