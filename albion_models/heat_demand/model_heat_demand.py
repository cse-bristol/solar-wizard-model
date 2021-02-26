import logging
import os
import subprocess
from os.path import join
from typing import List

import osgeo.ogr as ogr
import osgeo.osr as osr
from psycopg2.sql import SQL, Identifier

from albion_models.paths import PROJECT_ROOT


def model_heat_demand(pg_conn, job_id: int, bounds: str,
                      lidar_tiff_paths: List[str], heat_demand_dir: str, heat_degree_days: float):
    """
    Model the heat demand for buildings within the bounding box.

    Bounding box coordinates should be in SRS 4326.
    """
    os.makedirs(heat_demand_dir, exist_ok=True)
    logging.info("Preparing geojson for heat demand estimation")
    geojson_file = join(heat_demand_dir, f"heat_demand_job_{job_id}.json")
    results_file = join(heat_demand_dir, f"heat_demand_job_{job_id}.tab")

    rows = _get_rows(pg_conn, bounds)
    _create_geojson(rows, geojson_file)

    logging.info("Starting heat demand estimation...")
    _run_model(geojson_file, lidar_tiff_paths, results_file, heat_degree_days)

    _load_output_to_database(pg_conn, results_file, job_id, heat_degree_days)

    os.remove(geojson_file)
    os.remove(results_file)

    logging.info("Heat demand estimation complete")


def _get_rows(pg_conn, bounds: str) -> List[dict]:
    with pg_conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT toid, ST_AsText(geom_4326) as geom_wkt, is_residential, height 
            FROM building.building
            WHERE ST_Intersects(ST_GeomFromText(%(bounds)s, 4326), geom_4326) 
            """,
            {'bounds': bounds}
        )
        pg_conn.commit()
        return cursor.fetchall()


def _create_geojson(rows: List[dict], filename: str):
    driver = ogr.GetDriverByName("GeoJSON")
    data_source = driver.CreateDataSource(filename)
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(4326)
    layer = data_source.CreateLayer("buildings", srs, ogr.wkbPolygon)

    # Add the fields we're interested in
    toid_field = ogr.FieldDefn("toid", ogr.OFTString)
    toid_field.SetWidth(20)
    layer.CreateField(toid_field)
    resi_field = ogr.FieldDefn("resi", ogr.OFTInteger)
    resi_field.SetSubType(ogr.OFSTBoolean)
    layer.CreateField(resi_field)
    layer.CreateField(ogr.FieldDefn("height", ogr.OFTReal))

    for row in rows:
        feature = ogr.Feature(layer.GetLayerDefn())
        feature.SetField("toid", row['toid'])
        feature.SetField("resi", row['is_residential'])
        feature.SetField("height", row['height'])
        feature.SetGeometry(ogr.CreateGeometryFromWkt(row['geom_wkt']))
        layer.CreateFeature(feature)
        feature = None

    data_source = None


def _run_model(geojson_file: str, lidar_tiff_paths: List[str], outfile: str, heat_degree_days: float):
    lidar_tiff_paths = ' '.join(['--lidar ' + path for path in lidar_tiff_paths])
    res = subprocess.run(
        f"""java -jar {join(PROJECT_ROOT, "resources", "thermos-heat-model.jar")}
        --input {geojson_file}
        --key-field toid
        --output {outfile}
        --degree-days {heat_degree_days}
        {lidar_tiff_paths}
        """.replace("\n", " "),
        capture_output=True, text=True, shell=True)
    print(res.stdout)
    print(res.stderr)
    if res.returncode != 0:
        raise ValueError(res.stderr)


def _load_output_to_database(pg_conn, file_name: str, job_id: int, heat_degree_days: float):
    """Load heat demand data from tabfile using the postgres COPY command"""
    with pg_conn.cursor() as cursor, open(file_name, encoding='utf-8') as results:
        cursor.execute("""
            CREATE TABLE models.raw_heat_demand (
                toid text NOT NULL,
                annual_demand double precision NOT NULL,
                peak_demand double precision NOT NULL,
                sap_water_demand double precision NOT NULL,
                demand_source text NOT NULL,
                floor_area double precision NOT NULL,
                height double precision,
                perimeter double precision NOT NULL,
                shared_perimeter double precision NOT NULL,
                storeys double precision,
                footprint double precision NOT NULL,
                wall_area double precision,
                party_wall_area double precision,
                external_wall_area double precision,
                external_surface_area double precision,
                volume double precision,
                ext_surface_proportion double precision,
                ext_surface_per_volume double precision,
                tot_surface_per_volume double precision
            );
            """)

        results.readline()  # skip header
        cursor.copy_from(results, "models.raw_heat_demand", sep='\t', null='')
        cursor.execute(SQL(
            """
            INSERT INTO models.heat_demand 
            SELECT 
                r.*, 
                %(job_id)s, 
                b.geom_4326, 
                %(heat_degree_days)s,
                b.has_end_date = true
                    OR b.blacklisted_pao = true
                    OR b.blacklisted_sao = true
                    OR b.blacklisted_classification = true AS ignore,
                b.blacklist_reasons AS ignore_reasons
            FROM models.raw_heat_demand r 
            LEFT JOIN building.building b ON r.toid = b.toid;
            
            DROP TABLE models.raw_heat_demand;
            
            DROP VIEW IF EXISTS models.{job_view};
            CREATE VIEW models.{job_view} AS 
            SELECT * FROM models.heat_demand WHERE job_id = %(job_id)s;
            """).format(
                job_view=Identifier(f"heat_demand_job_{job_id}")),
            {
                'job_id': job_id,
                'heat_degree_days': heat_degree_days,
            })
        pg_conn.commit()
