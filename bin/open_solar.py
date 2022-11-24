"""
!!! Use via the "open_solar" script (in same dir as this file)!!!
- this will use the "open_solar.nix" script to setup a suitable nix shell
"""

import argparse
import json
import logging
from os.path import join

import sys
from psycopg2.extras import Json, execute_values, DictCursor
from psycopg2.sql import Literal
from typing import List, Optional

from albion_models import paths
from albion_models.db_funcs import sql_command, sql_script, connect
from albion_models.geos import get_grid_cells, from_geojson_file, from_geojson, project_geom, to_geojson
from albion_models.solar_pv.open_solar import open_solar_export

model_params = {
    "horizon_search_radius": {
        "default": 1000,
        "help": "How far in each direction to look when determining horizon height. Unit: metres. (default: %(default)s)"},
    "horizon_slices": {
        "default": 36,
        "help": "The number of rays traced from each point to determine horizon height. (default: %(default)s)"},
    # source: https://mcssco.wpenginepowered.com/wp-content/uploads/2021/10/MIS-3002_Solar-PV-Systems-V4.0.pdf#page=16
    "max_roof_slope_degrees": {
        "default": 70,
        "help": "Unit: degrees. (default: %(default)s)"},
    "min_roof_area_m": {
        "default": 8,
        "help": "Roofs smaller than this area will be excluded. (default: %(default)s)"},
    "min_roof_degrees_from_north": {
        "default": 45,
        "help": "Roofs whose aspect differs from 0° (North) by less than this amount will be excluded. (default: %(default)s)"},
    "flat_roof_degrees": {
        "default": 10,
        "help": "10° is normally recommended as it allows fitting more panels than the optimal "
                "angle for an individual panel, as the gaps between rows can be smaller. "
                "Ballast and frame costs are also lower (not modeled). (default: %(default)s)"},
    "peak_power_per_m2": {
        "default": 0.2,
        "help": "(default: %(default)s)"},
    "pv_tech": {
        "default": "crystSi",
        "choices": ["crystSi", "CIS", "CdTe"],
        "help": "crystSi: crystalline silicon (conventional solar cell).\n"
                "CIS: Copper Indium Selenide (a thin-film cell)\n"
                "CdTe: Cadmium telluride (also thin-film).\n"
                "Cost-benefit modelling assumes crystSi. (default: %(default)s)"},
    "panel_width_m": {
        "default": 0.99,
        "help": "(default: %(default)s)"},
    "panel_height_m": {
        "default": 1.64,
        "help": "(default: %(default)s)"},
    "panel_spacing_m": {
        "default": 0.01,
        "help": "Except spacing between rows of panels on flat roofs, which is a "
                "function of the angle that flat roof panels are mounted. (default: %(default)s)"},
    "large_building_threshold": {
        "default": 200,
        "help": "This is currently only used to switch between alternative "
                "minimum distances to edge of roof, but might be used for "
                "more in the future. (default: %(default)s)"},
    # source: clause 5.8.7 https://mcssco.wpenginepowered.com/wp-content/uploads/2021/10/MIS-3002_Solar-PV-Systems-V4.0.pdf#page=18
    "min_dist_to_edge_m": {
        "default": 0.4,
        "help": "This only counts the edge of the building, not the edges of "
                "other areas of roof. (default: %(default)s)"},
    "min_dist_to_edge_large_m": {
        "default": 1,
        "help": "This only counts the edge of the building, not the edges of "
                "other areas of roof. (default: %(default)s)"},
    "debug_mode": {
        "default": False,
        "help": "if ticked, do not delete temporary files and database objects. (default: %(default)s)"},
}


def create_run(pg_conn, name: str, cell_size: int, cell_ids: Optional[List[int]], run_boundary_27700_json: Optional[str], params: dict) -> int:
    with pg_conn.cursor() as cursor:
        os_run_id = sql_command(
            cursor,
            "INSERT INTO models.open_solar_run (name) VALUES ({name}) RETURNING os_run_id",
            name=Literal(name),
            result_extractor=lambda res: res[0][0])

        if run_boundary_27700_json:
            run_boundary_27700 = from_geojson(run_boundary_27700_json)
        else:
            run_boundary_27700 = from_geojson_file(join(paths.RESOURCES_DIR, "gb.geojson"))

        cells = get_grid_cells(run_boundary_27700, cell_size, cell_size)
        if cell_ids is not None:
            cells = [cells[cid] for cid in cell_ids]

        job_ids = execute_values(
            cursor,
            """
            INSERT INTO models.job_queue (
                project,
                bounds,
                solar_pv,
                params,
                open_solar)
            VALUES %s
            RETURNING job_id
            """,
            template="""(
                'open_solar:' || %s || ':' || %s || ',' || %s,
                ST_Multi(ST_GeomFromText( %s )),
                true,
                %s,
                true)
            """,
            argslist=[(name, cell.bounds[0], cell.bounds[1], cell.wkt, Json(params)) for cell in cells],
            fetch=True,
        )

        execute_values(
            cursor,
            "INSERT INTO models.open_solar_jobs (os_run_id, job_id) VALUES %s",
            argslist=[(os_run_id, job[0]) for job in job_ids])

        pg_conn.commit()

    return os_run_id


def list_runs(pg_conn):
    return sql_script(
        pg_conn,
        "open_solar/list.runs.sql",
        result_extractor=lambda res: res
    )


def cancel_run(pg_conn, os_run_id: int):
    sql_command(
        pg_conn,
        """
        UPDATE models.job_queue q SET status = 'CANCELLED'
        FROM models.open_solar_jobs osj
        WHERE
            status = 'NOT_STARTED'
            AND osj.job_id = q.job_id
            AND osj.os_run_id = {os_run_id}
        """,
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


def _print_table(data: List[dict], sep: str = ","):
    if len(data) == 0:
        print("No data")
        return

    header = list(data[0].keys())
    print(sep.join(header))
    for row in data:
        print(sep.join([str(cell) for cell in row]))


def _get_boundary_27700_from_boundary_4326(boundary_4326: str) -> Optional[str]:
    if boundary_4326:
        boundary_27700 = to_geojson(project_geom(from_geojson(boundary_4326), 4326, 27700))
        return boundary_27700
    return None


def parse_cli_args():
    desc = "Open Solar CLI tool"
    parser = argparse.ArgumentParser(description=desc)
    subparsers = parser.add_subparsers(dest="op", required=True, title="op")

    pg_uri_arg = {
        "metavar": "URI",
        "required": True,
        "help": "Postgres connection URI. See "
                "https://www.postgresql.org/docs/current/libpq-connect.html#id-1.7.3.8.3.6 "
                "for formatting details"
    }

    create_parser = subparsers.add_parser('create',
                                          help="Create an Open Solar run",
                                          description="Create an Open Solar run. Model parameters all have defaults")
    create_parser.add_argument("--pg_uri", **pg_uri_arg)
    create_parser.add_argument('-n', '--name', required=True,
                               help="Name of the Open Solar run to create")
    create_parser.add_argument('-c', '--cell_size', default=30000,
                               help="Edge length of individual job bound squares in metres. (default: %(default)s)")
    create_parser.add_argument('--cell_ids',
                               help="Comma-separated list of cell ids (numbers). "
                                    "Only create these cells, With 0 being SW-most cell "
                                    "and counting in rows East and then North")
    create_parser.add_argument('--run_boundary',
                               help="GeoJSON as used in Albion job boundary. Restrict the run area using this"
                                    "instead of the GB boundary.")

    for param, data in model_params.items():
        create_parser.add_argument(f"--{param}", **data)

    list_parser = subparsers.add_parser('list',
                                        help="List existing Open Solar runs and their progress",
                                        description="List existing Open Solar runs and their progress")
    list_parser.add_argument("--pg_uri", **pg_uri_arg)

    cancel_parser = subparsers.add_parser('cancel',
                                          help="Cancel an Open Solar run",
                                          description="Cancel an Open Solar run")
    cancel_parser.add_argument('id', help="Open Solar run ID")
    cancel_parser.add_argument("--pg_uri", **pg_uri_arg)

    progress_parser = subparsers.add_parser('progress',
                                            help="Output Open Solar job progress as geoJSON",
                                            description="Output Open Solar job progress as geoJSON")
    progress_parser.add_argument('id', help="Open Solar run ID")
    progress_parser.add_argument("--pg_uri", **pg_uri_arg)

    extract_parser = subparsers.add_parser('extract',
                                           help="Extract Open Solar job outputs to GPKG",
                                           description="Extract Open Solar job outputs to GPKG")
    extract_parser.add_argument('id', help="Open Solar run ID", nargs="?")
    extract_parser.add_argument('--gpkg_dir', help="Geopackage output file location (dir / folder)")
    extract_parser.add_argument("--pg_uri", **pg_uri_arg)
    extract_parser.add_argument("--extract_job_info", help="Extract job information (Open Solar run ID needed)",
                                action='store_true', default=False)
    extract_parser.add_argument("--extract_base_info", help="Extract base information (e.g. LSOAs, LAs)",
                                action='store_true', default=False)
    extract_parser.add_argument("--start_job_id", help="Minimum job id to export (inclusive)")
    extract_parser.add_argument("--end_job_id", help="Maximum job id to export (inclusive)")
    extract_parser.add_argument("--regenerate", help="Create outputs even if they exist already",
                                action='store_true', default=False)

    return parser.parse_args()


def open_solar_cli():
    args = parse_cli_args()
    pg_conn = connect(args.pg_uri, cursor_factory=DictCursor)
    logging.basicConfig(level=logging.INFO,
                        format='[%(asctime)s] %(levelname)s: %(message)s',
                        stream=sys.stdout)

    try:
        if args.op == "create":
            params = vars(args).copy()
            del params['name']
            del params['cell_size']
            del params['cell_ids']
            del params['run_boundary']
            del params['pg_uri']
            del params['op']
            cell_ids = [int(c.strip()) for c in args.cell_ids.split(",")] if args.cell_ids else None
            create_run(pg_conn, args.name, int(args.cell_size), cell_ids,
                       _get_boundary_27700_from_boundary_4326(args.run_boundary), params)
        elif args.op == "list":
            _print_table(list_runs(pg_conn))
        elif args.op == "cancel":
            cancel_run(pg_conn, args.id)
        elif args.op == "progress":
            geojson = run_progress_geojson(pg_conn, args.id)
            print(geojson)
        elif args.op == "extract":
            open_solar_export.export(args.pg_uri, args.id, args.gpkg_dir,
                                     args.extract_job_info, args.extract_base_info,
                                     args.start_job_id, args.end_job_id,
                                     args.regenerate)
    except Exception as e:
        pg_conn.rollback()
        raise e
    finally:
        pg_conn.close()


if __name__ == "__main__":
    open_solar_cli()
