import argparse
import os
import subprocess
from os.path import join
from typing import List

import psycopg2.extras
from psycopg2.sql import SQL, Identifier

import src.pv_gis_client as pv_gis_client
import src.tables as tables
from src.db_funcs import sql_script, connect
from src.crop import crop_to_mask
from src.polygonize import generate_aspect_polygons, aggregate_horizons
from src.horizons import get_horizons, load_horizons_to_db


def model_solar_pv(pg_uri: str,
                   root_solar_dir: str,
                   job_id: int,
                   lidar_paths: List[str],
                   horizon_search_radius: int,
                   horizon_slices: int,
                   max_roof_slope_degrees: int,
                   min_roof_area_m: int,
                   min_roof_degrees_from_north: int,
                   flat_roof_degrees: int):

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
    get_horizons(cropped_lidar, mask_file, horizons_csv, horizon_search_radius, horizon_slices)
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
    _pv_gis(pg_uri, job_id, solar_dir)


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


def _pv_gis(pg_uri: str, job_id: int, solar_dir: str):
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
            pv_gis_client.solar_pv_estimate(rows, solar_pv_csv)
    finally:
        pg_conn.close()


if __name__ == '__main__':
    desc = "Model solar PV"
    parser = argparse.ArgumentParser(description=desc)

    parser.add_argument("--pg_uri", metavar="URI", required=True,
                        help="Postgres connection URI. See "
                             "https://www.postgresql.org/docs/current/libpq-connect.html#id-1.7.3.8.3.6 "
                             "for formatting details")
    parser.add_argument("--solar_dir", metavar="DIR", required=True, help="Directory where temporary files and outputs are stored")
    parser.add_argument("--job_id", metavar="ID", required=True, type=int, help="Albion job ID")
    parser.add_argument("--lidar_paths", metavar="FILE", required=True, action='append', help="All lidar tiles required for modelling")
    parser.add_argument("--horizon_search_radius", default=1000, type=int, metavar="INT", help="Horizon search radius in metres (default 1000)")
    parser.add_argument("--horizon_slices", default=8, type=int, metavar="INT", help="Horizon compass slices (default 8)")
    parser.add_argument("--max_roof_slope_degrees", default=80, type=int, metavar="INT", help="Maximum roof slope for PV (default 80)")
    parser.add_argument("--min_roof_area_m", default=10, type=int, metavar="INT", help="Minimum roof area m² for PV installation (default 10)")
    parser.add_argument("--min_roof_degrees_from_north", default=45, type=int, metavar="INT", help="Minimum degree distance from North for PV (default 45)")
    parser.add_argument("--flat_roof_degrees", default=10, type=int, metavar="INT", help="Angle (degrees) to mount panels on flat roofs (default 10)")

    args = parser.parse_args()

    model_solar_pv(
        pg_uri=args.pg_uri,
        root_solar_dir=args.solar_dir,
        job_id=args.job_id,
        lidar_paths=args.lidar_paths,
        horizon_search_radius=args.horizon_search_radius,
        horizon_slices=args.horizon_slices,
        max_roof_slope_degrees=args.max_roof_slope_degrees,
        min_roof_area_m=args.min_roof_area_m,
        min_roof_degrees_from_north=args.min_roof_degrees_from_north,
        flat_roof_degrees=args.flat_roof_degrees,
    )
