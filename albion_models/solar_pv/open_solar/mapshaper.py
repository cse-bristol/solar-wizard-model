import json
import os
import subprocess
from os.path import join
from typing import Dict, Tuple, List

import psycopg2
from psycopg2.extras import DictCursor
from psycopg2.sql import Identifier, SQL

from albion_models.db_funcs import sql_command, connect

_MAPSHAPER_R: str = join(os.path.realpath(os.path.dirname(__file__)), "mapshaper.R")
_SIMPLIFIED_GEOM_TEMP_TBL: str = "geom_simplified"


def ms_simplify(pg_conn, from_sql: str, id_col: Identifier, geom_col: Identifier, bindings: dict = None) -> str:
    """ Use mapshaper to simplify geometries selected from the db. Writes results into a temp table.
    :param pg_conn:
    :param from_sql: The part of the query from the "FROM" onwards
    :param id_col: The name of the id column to get an id from
    :param geom_col: The name of the geometry column to get geometry from
    :param bindings: Values to bind in the FROM clause
    :return: Name of the temp table
    """
    geojson_in = _get_geojson(pg_conn, from_sql, id_col, geom_col, bindings)
    geojson_out = _ms_simplify(geojson_in)
    simplified_geos = _parse_geojson(geojson_out)
    return _create_temp_table(pg_conn, simplified_geos)


def _get_geojson(pg_conn, from_sql: str, id_col: Identifier, geom_col: Identifier, bindings: dict = None):
    geojson = sql_command(pg_conn,
                          "SELECT json_build_object( "
                          " 'type', 'FeatureCollection', "
                          " 'features', json_agg( "
                          "  json_build_object( "
                          "   'type', 'Feature', "
                          "   'properties', json_build_object( 'id', {id_col} ), "
                          "   'geometry', ST_AsGeoJSON({geom_col})::jsonb "
                          "  )::json) "
                          " )::text " + from_sql,
                          bindings=bindings,
                          id_col=id_col,
                          geom_col=geom_col,
                          result_extractor=lambda res: res[0][0]
                          )
    return geojson


def _ms_simplify(geojson: str) -> str:
    p = subprocess.run(_MAPSHAPER_R, input=f"{geojson}\n", capture_output=True, text=True)
    if p.returncode == 0:
        return str(p.stdout)
    else:
        raise Exception(f"Error running mapshaper:\nreturncode = {p.returncode}\nstdout = {p.stdout}\nstderr = {p.stderr}")


def _parse_geojson(geojson: str) -> List[Tuple[str, str]]:
    j = json.loads(geojson)
    features = j["features"]
    geo_by_id = [(feature["properties"]["id"], json.dumps(feature["geometry"])) for feature in features]
    return geo_by_id


def _create_temp_table(pg_conn, geo_by_id: List[Tuple[str, str]]):
    sql_command(
        pg_conn,
        "CREATE TEMP TABLE {geom_simplified} ("
        "id VARCHAR PRIMARY KEY, "
        "geojson VARCHAR NOT NULL"
        ")",
        geom_simplified=Identifier(_SIMPLIFIED_GEOM_TEMP_TBL)
    )

    insert = SQL("INSERT INTO {geom_simplified} (id, geojson) VALUES %s")\
        .format(geom_simplified=Identifier(_SIMPLIFIED_GEOM_TEMP_TBL))
    with pg_conn.cursor() as cursor:
        psycopg2.extras.execute_values(
            cursor,
            insert,
            geo_by_id, template=None, page_size=100
        )

    return _SIMPLIFIED_GEOM_TEMP_TBL

def print_temp_table(pg_conn):
    contents = sql_command(
        pg_conn,
        "SELECT id, ST_AsText(ST_GeomFromGeoJSON(geojson)) "
        "FROM geom_simplified ",
        result_extractor=lambda rows: [dict(row) for row in rows]
    )
    for c in contents:
        print(c)


if __name__ == "__main__":
    job_id = 10
    pg_uri = "postgresql://albion_webapp:ydBbE3JCnJ4@localhost:5432/albion"
    with connect(pg_uri, cursor_factory=DictCursor) as pg_conn:
        temp_table = ms_simplify(
            pg_conn,
            "FROM models.pv_building mpb "
            "JOIN mastermap.building mb USING (toid) "
            "WHERE mpb.job_id = %(job_id)s ",
            Identifier("toid"),
            Identifier("mb", "geom_4326"),
            {"job_id": job_id})
        print_temp_table(pg_conn)
