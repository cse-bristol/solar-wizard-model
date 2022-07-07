import logging
import os
import subprocess
from os.path import join
from typing import List

import osgeo.ogr as ogr
import osgeo.osr as osr
from psycopg2.sql import SQL, Identifier, Literal

from albion_models.db_funcs import sql_script_with_bindings, sql_script
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
            FROM aggregates.building
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
    lidar_tiff_paths = ' '.join(['-l ' + path for path in lidar_tiff_paths])
    command = f"""java -jar {join(PROJECT_ROOT, "resources", "thermos-heat-model.jar")}
        --input {geojson_file}
        --key-field toid
        --output {outfile}
        --degree-days {heat_degree_days}
        {lidar_tiff_paths}
        """.replace("\n", " ")

    logging.info("Running command:")
    logging.info(command)

    res = subprocess.run(command.split(), capture_output=True, text=True)
    print(res.stdout)
    print(res.stderr)
    if res.returncode != 0:
        raise ValueError(res.stderr)


def _load_output_to_database(pg_conn, file_name: str, job_id: int, heat_degree_days: float):
    """Load heat demand data from tabfile using the postgres COPY command"""
    with pg_conn.cursor() as cursor, open(file_name, encoding='utf-8') as results:
        cursor.execute("""
            DROP TABLE IF EXISTS models.raw_heat_demand;
            
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
                tot_surface_per_volume double precision,
                height_source text
            );
            CREATE INDEX ON models.raw_heat_demand (toid);
            """)
        results.readline()  # skip header
        cursor.copy_from(results, "models.raw_heat_demand", sep='\t', null='')
        pg_conn.commit()

        cursor.execute(SQL(
            """
            DELETE FROM models.heat_demand WHERE job_id = %(job_id)s;
            
            INSERT INTO models.heat_demand
            SELECT
                r.toid,
                r.annual_demand,
                r.peak_demand,
                r.sap_water_demand,
                r.demand_source,
                r.floor_area,
                CASE WHEN r.height_source = ':default' THEN NULL ELSE r.height END AS height,
                r.perimeter,
                r.shared_perimeter,
                r.storeys,
                r.footprint,
                r.wall_area,
                r.party_wall_area,
                r.external_wall_area,
                r.external_surface_area,
                r.volume,
                r.ext_surface_proportion,
                r.ext_surface_per_volume,
                r.tot_surface_per_volume,
                %(job_id)s,
                b.geom_4326,
                %(heat_degree_days)s,
                b.num_addresses = 0 AND b.num_epc_certs = 0 AS ignore,
                string_to_array(b.ignore_reasons, '|') AS ignore_reasons,
                CASE WHEN r.height_source = ':lidar' THEN 'lidar'
                     WHEN r.height_source = ':fallback' THEN 'osmm'
                     WHEN r.height_source = ':default' THEN 'ignore'
                     ELSE r.height_source
                     END AS height_source
            FROM models.raw_heat_demand r
            LEFT JOIN aggregates.building b ON r.toid = b.toid;

            DROP TABLE models.raw_heat_demand;

            DROP VIEW IF EXISTS models.{job_view};
            CREATE VIEW models.{job_view} AS
            SELECT * FROM models.heat_demand WHERE job_id = %(job_id)s;
            """).format(
                job_view=Identifier(f"heat_demand_job_{job_id}")),
            {
                'job_id': job_id,
                'heat_degree_days': heat_degree_days})
        pg_conn.commit()


def model_insulation_measure_costs(
        pg_conn,
        job_id: int,
        include_cwi: bool,
        include_swi: bool,
        include_loft_ins: bool,
        include_roof_ins: bool,
        include_floor_ins: bool,
        include_glazing: bool,

        cwi_max_pct_area: float,
        swi_max_pct_area: float,
        loft_ins_max_pct_area: float,
        roof_ins_max_pct_area: float,
        floor_ins_max_pct_area: float,
        glazing_max_pct_area: float,

        cwi_per_m2_cost: float,
        swi_per_m2_cost: float,
        loft_ins_per_m2_cost: float,
        roof_ins_per_m2_cost: float,
        floor_ins_per_m2_cost: float,
        glazing_per_m2_cost: float,

        cwi_fixed_cost: float,
        swi_fixed_cost: float,
        loft_ins_fixed_cost: float,
        roof_ins_fixed_cost: float,
        floor_ins_fixed_cost: float,
        glazing_fixed_cost: float,

        cwi_pct_demand_reduction: float,
        swi_pct_demand_reduction: float,
        loft_ins_pct_demand_reduction: float,
        roof_ins_pct_demand_reduction: float,
        floor_ins_pct_demand_reduction: float,
        glazing_pct_demand_reduction: float):
    """Assumes heat demand model has already run"""

    logging.info(f"include_cwi: {include_cwi}")
    logging.info(f"include_swi: {include_swi}")
    logging.info(f"include_loft_ins: {include_loft_ins}")
    logging.info(f"include_roof_ins: {include_roof_ins}")
    logging.info(f"include_floor_ins: {include_floor_ins}")
    logging.info(f"include_glazing: {include_glazing}")
    logging.info(f"cwi_max_pct_area: {cwi_max_pct_area}")
    logging.info(f"swi_max_pct_area: {swi_max_pct_area}")
    logging.info(f"loft_ins_max_pct_area: {loft_ins_max_pct_area}")
    logging.info(f"roof_ins_max_pct_area: {roof_ins_max_pct_area}")
    logging.info(f"floor_ins_max_pct_area: {floor_ins_max_pct_area}")
    logging.info(f"glazing_max_pct_area: {glazing_max_pct_area}")
    logging.info(f"cwi_per_m2_cost: {cwi_per_m2_cost}")
    logging.info(f"swi_per_m2_cost: {swi_per_m2_cost}")
    logging.info(f"loft_ins_per_m2_cost: {loft_ins_per_m2_cost}")
    logging.info(f"roof_ins_per_m2_cost: {roof_ins_per_m2_cost}")
    logging.info(f"floor_ins_per_m2_cost: {floor_ins_per_m2_cost}")
    logging.info(f"glazing_per_m2_cost: {glazing_per_m2_cost}")
    logging.info(f"cwi_fixed_cost: {cwi_fixed_cost}")
    logging.info(f"swi_fixed_cost: {swi_fixed_cost}")
    logging.info(f"loft_ins_fixed_cost: {loft_ins_fixed_cost}")
    logging.info(f"roof_ins_fixed_cost: {roof_ins_fixed_cost}")
    logging.info(f"floor_ins_fixed_cost: {floor_ins_fixed_cost}")
    logging.info(f"glazing_fixed_cost: {glazing_fixed_cost}")
    logging.info(f"cwi_pct_demand_reduction: {cwi_pct_demand_reduction}")
    logging.info(f"swi_pct_demand_reduction: {swi_pct_demand_reduction}")
    logging.info(f"loft_ins_pct_demand_reduction: {loft_ins_pct_demand_reduction}")
    logging.info(f"roof_ins_pct_demand_reduction: {roof_ins_pct_demand_reduction}")
    logging.info(f"floor_ins_pct_demand_reduction: {floor_ins_pct_demand_reduction}")
    logging.info(f"glazing_pct_demand_reduction: {glazing_pct_demand_reduction}")

    sql_script(
        pg_conn,
        "ins_measure_costs.sql",
        job_id=Literal(job_id),
        include_cwi=Literal(include_cwi),
        include_swi=Literal(include_swi),
        include_loft_ins=Literal(include_loft_ins),
        include_roof_ins=Literal(include_roof_ins),
        include_floor_ins=Literal(include_floor_ins),
        include_glazing=Literal(include_glazing),

        cwi_max_pct_area=Literal(cwi_max_pct_area),
        swi_max_pct_area=Literal(swi_max_pct_area),
        loft_ins_max_pct_area=Literal(loft_ins_max_pct_area),
        roof_ins_max_pct_area=Literal(roof_ins_max_pct_area),
        floor_ins_max_pct_area=Literal(floor_ins_max_pct_area),
        glazing_max_pct_area=Literal(glazing_max_pct_area),

        cwi_per_m2_cost=Literal(cwi_per_m2_cost),
        swi_per_m2_cost=Literal(swi_per_m2_cost),
        loft_ins_per_m2_cost=Literal(loft_ins_per_m2_cost),
        roof_ins_per_m2_cost=Literal(roof_ins_per_m2_cost),
        floor_ins_per_m2_cost=Literal(floor_ins_per_m2_cost),
        glazing_per_m2_cost=Literal(glazing_per_m2_cost),

        cwi_fixed_cost=Literal(cwi_fixed_cost),
        swi_fixed_cost=Literal(swi_fixed_cost),
        loft_ins_fixed_cost=Literal(loft_ins_fixed_cost),
        roof_ins_fixed_cost=Literal(roof_ins_fixed_cost),
        floor_ins_fixed_cost=Literal(floor_ins_fixed_cost),
        glazing_fixed_cost=Literal(glazing_fixed_cost),

        cwi_pct_demand_reduction=Literal(cwi_pct_demand_reduction),
        swi_pct_demand_reduction=Literal(swi_pct_demand_reduction),
        loft_ins_pct_demand_reduction=Literal(loft_ins_pct_demand_reduction),
        roof_ins_pct_demand_reduction=Literal(roof_ins_pct_demand_reduction),
        floor_ins_pct_demand_reduction=Literal(floor_ins_pct_demand_reduction),
        glazing_pct_demand_reduction=Literal(glazing_pct_demand_reduction))
