import argparse
import json
import logging
import os
import psycopg2.extras
import sys
from psycopg2.extras import Json
from psycopg2.sql import Literal
from typing import List

from albion_models.db_funcs import sql_command, sql_script, connect, command_to_gpkg

model_params = {
    "horizon_search_radius": {
        "default": 1000,
        "help": "How far in each direction to look when determining horizon height. Unit: metres"},
    "horizon_slices": {
        "default": 16,
        "help": "The number of rays traced from each point to determine horizon height"},
    "max_roof_slope_degrees": {
        "default": 80,
        "help": "Unit: degrees"},
    "min_roof_area_m": {
        "default": 8,
        "help": "Roofs smaller than this area will be excluded"},
    "min_roof_degrees_from_north": {
        "default": 45,
        "help": "Roofs whose aspect differs from 0° (North) by less than this amount will be excluded"},
    "flat_roof_degrees": {
        "default": 10,
        "help": "10° is normally recommended as it allows fitting more panels than the optimal "
                "angle for an individual panel, as the gaps between rows can be smaller. "
                "Ballast and frame costs are also lower (not modeled)"},
    "peak_power_per_m2": {
        "default": 0.2,
        "help": ""},
    "pv_tech": {
        "default": "crystSi",
        "choices": ["crystSi", "CIS", "CdTe"],
        "help": "crystSi: crystalline silicon (conventional solar cell).\n"
                "CIS: Copper Indium Selenide (a thin-film cell)\n"
                "CdTe: Cadmium telluride (also thin-film).\n"
                "Cost-benefit modelling assumes crystSi"},
    "panel_width_m": {
        "default": 0.99,
        "help": ""},
    "panel_height_m": {
        "default": 1.64,
        "help": ""},
    "panel_spacing_m": {
        "default": 0.01,
        "help": "Except spacing between rows of panels on flat roofs, which is a "
                "function of the angle that flat roof panels are mounted"},
    "large_building_threshold": {
        "default": 200,
        "help": "This is currently only used to switch between alternative "
                "minimum distances to edge of roof, but might be used for "
                "more in the future"},
    "min_dist_to_edge_m": {
        "default": 0.3,
        "help": "This only counts the edge of the building, not the edges of "
                "other areas of roof"},
    "min_dist_to_edge_large_m": {
        "default": 1,
        "help": "This only counts the edge of the building, not the edges of "
                "other areas of roof"},
    "debug_mode": {
        "default": False,
        "help": "if ticked, do not delete temporary files and database objects"},
}


def create_run(pg_conn, name: str, cell_size: int, params: dict) -> int:
    with pg_conn.cursor() as cursor:
        os_run_id = sql_command(
            cursor,
            "INSERT INTO models.open_solar_run (name) VALUES ({name}) RETURNING os_run_id",
            name=Literal(name),
            result_extractor=lambda res: res[0][0])

        job_ids = sql_script(
            cursor,
            "open_solar/create.run.sql",
            bindings={"name": name,
                      "params": Json(params)},
            cell_size=Literal(cell_size),
            result_extractor=lambda res: [(os_run_id, row[0]) for row in res]
        )

        psycopg2.extras.execute_values(
            cursor,
            "INSERT INTO models.open_solar_jobs (os_run_id, job_id) VALUES %s",
            argslist=job_ids)

        pg_conn.commit()

    return os_run_id


def list_runs(pg_conn):
    return sql_script(
        pg_conn,
        "open_solar/list.runs.sql",
        result_extractor=lambda res: res
    )


def cancel_run(pg_conn, os_run_id: int):
    sql_script(
        pg_conn,
        "open_solar/cancel.run.sql",
        os_run_id=Literal(os_run_id)
    )


def run_progress_geojson(pg_conn, os_run_id: int):
    def to_geojson(row: dict):
        geometry = row['geojson']
        properties = dict(row.copy())
        del properties['geojson']

        return {"type": "Feature",
                "geometry": geometry,
                "properties": properties}

    geojson = sql_script(
        pg_conn,
        "open_solar/run.geojson.sql",
        result_extractor=lambda res: [to_geojson(row) for row in res],
        os_run_id=Literal(os_run_id)
    )

    return json.dumps({"type": "FeatureCollection",
                       "features": geojson}, default=str)


def extract_run_data(pg_conn, pg_uri: str, os_run_id: int, gpkg: str):
    # TODO talk to Mark about output formats
    # TODO will doing this in a single query work with such large amount of data?
    #  could always convert it into a loop, one query per model job
    try:
        os.remove(gpkg)
    except OSError:
        pass

    command_to_gpkg(
        pg_conn, pg_uri, gpkg, "panels",
        src_srs=27700, dst_srs=4326,
        command="""
        SELECT pv.*
        FROM
            models.job_queue q
            LEFT JOIN models.open_solar_jobs osj ON osj.job_id = q.job_id
            LEFT JOIN models.solar_pv pv ON pv.job_id = osj.job_id
        WHERE osj.os_run_id = %(os_run_id)s
        """,
        os_run_id=os_run_id)

    command_to_gpkg(
        pg_conn, pg_uri, gpkg, "buildings",
        src_srs=4326, dst_srs=4326,
        command="""
        SELECT pv.*
        FROM
            models.job_queue q
            LEFT JOIN models.open_solar_jobs osj ON osj.job_id = q.job_id
            LEFT JOIN models.building_exclusion_reasons ber ON ber.job_id = osj.job_id
            LEFT JOIN mastermap.building b ON b.toid = ber.toid
        WHERE osj.os_run_id = %(os_run_id)s
        """,
        os_run_id=os_run_id)


def _print_table(data: List[dict], sep: str = ","):
    if len(data) == 0:
        print("No data")
        return

    header = list(data[0].keys())
    print(sep.join(header))
    for row in data:
        print(sep.join([str(cell) for cell in row]))


def parse_cli_args():
    desc = "Open Solar CLI tool"
    parser = argparse.ArgumentParser(description=desc)
    subparsers = parser.add_subparsers(dest="op", required=True, title="op")

    create_parser = subparsers.add_parser('create', help="Create an Open Solar run")
    create_parser.add_argument('-n', '--name', required=True,
                               help="Name of the Open Solar run to create")
    create_parser.add_argument('-c', '--cell-size', default=30000,
                               help="Edge length of individual job bound squares in metres")
    for param, data in model_params.items():
        create_parser.add_argument(f"--{param}", **data)

    list_parser = subparsers.add_parser('list',
                                        help="List existing Open Solar runs and their progress")

    cancel_parser = subparsers.add_parser('cancel', help="Cancel an Open Solar run")
    cancel_parser.add_argument('id', help="Open Solar run ID")

    progress_parser = subparsers.add_parser('progress',
                                            help="Output Open Solar job progress as geoJSON")
    progress_parser.add_argument('id', help="Open Solar run ID")

    extract_parser = subparsers.add_parser('extract',
                                           help="Extract Open Solar job outputs to CSV")
    extract_parser.add_argument('id', help="Open Solar run ID")
    extract_parser.add_argument('--gpkg', help="Geopackage output file location")

    parser.add_argument("--pg_uri", metavar="URI", required=True,
                        help="Postgres connection URI. See "
                             "https://www.postgresql.org/docs/current/libpq-connect.html#id-1.7.3.8.3.6 "
                             "for formatting details")

    return parser.parse_args()


def open_solar_cli():
    args = parse_cli_args()
    pg_conn = connect(args.pg_uri, cursor_factory=psycopg2.extras.DictCursor)
    logging.basicConfig(level=logging.INFO,
                        format='[%(asctime)s] %(levelname)s: %(message)s',
                        stream=sys.stdout)

    try:
        if args.op == "create":
            params = vars(args)
            del params['name']
            del params['cell_size']
            del params['pg_uri']
            del params['op']
            create_run(pg_conn, args.name, args.cell_size, params)
        elif args.op == "list":
            _print_table(list_runs(pg_conn))
        elif args.op == "cancel":
            cancel_run(pg_conn, args.id)
        elif args.op == "progress":
            geojson = run_progress_geojson(pg_conn, args.id)
            print(geojson)
        elif args.op == "extract":
            extract_run_data(pg_conn, args.pg_uri, args.id, args.gpkg)

    except Exception as e:
        pg_conn.rollback()
        raise e
    finally:
        pg_conn.close()


if __name__ == "__main__":
    open_solar_cli()
