import json
import logging
import os
import subprocess
from os.path import join
from typing import Tuple, List

import psycopg2
from psycopg2.extras import DictCursor
from psycopg2.sql import Identifier, SQL

from albion_models.db_funcs import sql_command

_MAPSHAPER_R: str = join(os.path.realpath(os.path.dirname(__file__)), "mapshaper.R")

_GET_GEOJSON_SQL: str = """
SELECT json_build_object( 
'type', 'FeatureCollection', 
'features', COALESCE(json_agg( 
json_build_object( 
 'type', 'Feature', 
 'properties', json_build_object( 'id', {id_sql} ), 
 'geometry', ST_AsGeoJSON({geom_col})::jsonb 
)::json), '[]' 
))::text {from_sql}
""".replace("\n", " ")


def ms_simplify(pg_conn,
                to_table: Identifier,
                from_sql: str, id_sql: str, geom_col: Identifier, bindings: dict = None):
    """
    Use mapshaper to simplify geometries selected from the db. Writes results into a temp table.
    :param pg_conn: db connection
    :param to_table: Identifier object for table to put simplified geoms into
    :param from_sql: The part of the query from the "FROM" onwards
    :param id_col: The name of the id column to get an id from
    :param geom_col: The name of the geometry column to get geometry from
    :param bindings: Values to bind in the FROM clause
    :return: Name of the temp table
    """
    has_panels = sql_command(pg_conn,
                             "SELECT count(*) > 0 FROM models.pv_building mpb WHERE mpb.job_id = %(job_id)s",
                             result_extractor=lambda res: res[0][0],
                             bindings=bindings
                             )
    if has_panels:
        geojson_in = _get_geojson(pg_conn, from_sql, id_sql, geom_col, bindings)
        geojson_out = _ms_simplify(geojson_in)
        simplified_geos = _parse_geojson(geojson_out)
    else:
        simplified_geos = []
    _create_output_table(pg_conn, to_table, simplified_geos)


def _get_geojson(pg_conn, from_sql: str, id_sql: str, geom_col: Identifier, bindings: dict = None):
    geojson = sql_command(pg_conn,
                          _GET_GEOJSON_SQL,
                          bindings=bindings,
                          result_extractor=lambda res: res[0][0],
                          id_sql=SQL(id_sql),
                          from_sql=SQL(from_sql),
                          geom_col=geom_col
                          )
    return geojson


def _ms_simplify(geojson: str) -> str:
    p = subprocess.run(_MAPSHAPER_R, input=f"{geojson}\n", capture_output=True, text=True)
    if p.returncode == 0:
        return str(p.stdout)
    else:
        raise RuntimeError(f"Error running mapshaper:\nreturncode = {p.returncode}\n"
                           f"stdout = {p.stdout}\nstderr = {p.stderr}")


def _parse_geojson(geojson: str) -> List[Tuple[str, str]]:
    j = json.loads(geojson)
    features = j["features"]
    geo_by_id = [(feature["properties"]["id"], json.dumps(feature["geometry"])) for feature in features]
    return geo_by_id


def _create_output_table(pg_conn, to_table: Identifier, geo_by_id: List[Tuple[str, str]]):
    sql_command(
        pg_conn,
        "CREATE TABLE IF NOT EXISTS {geom_simplified} ("
        "id VARCHAR PRIMARY KEY, "
        "geojson VARCHAR NOT NULL"
        ")",
        geom_simplified=to_table
    )

    sql_command(
        pg_conn,
        "TRUNCATE TABLE  {geom_simplified}",
        geom_simplified=to_table
    )

    insert = SQL("INSERT INTO {geom_simplified} (id, geojson) VALUES %s")\
        .format(geom_simplified=to_table)
    with pg_conn.cursor() as cursor:
        psycopg2.extras.execute_values(
            cursor,
            insert,
            geo_by_id, template=None, page_size=100
        )

    pg_conn.commit()
