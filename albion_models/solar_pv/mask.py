from os.path import join

import psycopg2.extras
from psycopg2.sql import SQL, Identifier, Literal

import albion_models.solar_pv.tables as tables
from albion_models.db_funcs import connect
from albion_models import gdal_helpers


def create_mask(mask_sql: str,
                solar_dir: str,
                pg_uri: str,
                res: float,
                srid: int):
    mask_file = join(solar_dir, 'mask.tif')
    gdal_helpers.rasterize(pg_uri, mask_sql, mask_file, res, srid)
    return mask_file


def buildings_mask_sql(pg_uri: str, job_id: int, buffer: int) -> str:
    pg_conn = connect(pg_uri, cursor_factory=psycopg2.extras.DictCursor)
    try:
        mask_sql = SQL(
            "SELECT ST_Buffer(b.geom_27700, {buffer}) FROM {buildings} b"
        ).format(
            buildings=Identifier(tables.schema(job_id), tables.BUILDINGS_TABLE),
            buffer=Literal(buffer),
        ).as_string(pg_conn)
    finally:
        pg_conn.close()

    return mask_sql


def bounds_mask_sql(pg_uri: str, job_id: int, srid: int) -> str:
    pg_conn = connect(pg_uri, cursor_factory=psycopg2.extras.DictCursor)
    try:
        mask_sql = SQL(
            "SELECT ST_Transform(bounds, {srid}) "
            "FROM models.job_queue q "
            "WHERE q.job_id={job_id} "
        ).format(job_id=Literal(job_id), srid=Literal(srid)).as_string(pg_conn)
    finally:
        pg_conn.close()

    return mask_sql
