import logging

import psycopg2.extras
from psycopg2.sql import SQL, Identifier, Literal

from psycopg2.sql import Identifier

import albion_models.solar_pv.tables as tables
from albion_models import gdal_helpers
from albion_models.db_funcs import connect, sql_script_with_bindings, count, connection, sql_command


def has_flat_roof(pg_uri: str, job_id: int) -> bool:
    """
    :return: true if there is one or more flat roof in the job
    """
    pg_conn = connect(pg_uri)
    try:
        return sql_command(
            pg_conn,
            "SELECT COUNT(*) != 0 FROM {roof_polygons} WHERE is_flat = true",
            roof_polygons=Identifier(tables.schema(job_id), tables.ROOF_POLYGON_TABLE),
            result_extractor=lambda rows: rows[0][0]
        )
    finally:
        pg_conn.close()


def get_flat_roof_aspect_sql(pg_uri: str, job_id: int) -> str:
    with connection(pg_uri, cursor_factory=psycopg2.extras.DictCursor) as pg_conn:
        return SQL(
            "SELECT ST_Force3D(roof_geom_27700, aspect) FROM {roof_polygons} WHERE is_flat = true"
        ).format(
            roof_polygons=Identifier(tables.schema(job_id), tables.ROOF_POLYGON_TABLE)
        ).as_string(pg_conn)


def create_flat_roof_aspect(mask_sql: str,
                mask_out: str,
                pg_uri: str,
                res: float,
                srid: int):
    gdal_helpers.rasterize_3d(pg_uri, mask_sql, mask_out, res, srid)
