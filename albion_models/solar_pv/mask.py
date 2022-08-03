import psycopg2.extras
from psycopg2.sql import SQL, Identifier, Literal

import albion_models.solar_pv.tables as tables
from albion_models.db_funcs import connection
from albion_models import gdal_helpers


def create_mask(mask_sql: str,
                mask_out: str,
                pg_uri: str,
                res: float,
                srid: int):
    """
    Create a mask raster such that all pixels that intersect polygons
    selected by `mask_sql` have a value of 1, otherwise 0.
    """
    gdal_helpers.rasterize(pg_uri, mask_sql, mask_out, res, srid)


def buildings_mask_sql(pg_uri: str, job_id: int, buffer: int) -> str:
    with connection(pg_uri, cursor_factory=psycopg2.extras.DictCursor) as pg_conn:
        return SQL(
            "SELECT ST_Buffer(b.geom_27700, {buffer}) FROM {buildings} b"
        ).format(
            buildings=Identifier(tables.schema(job_id), tables.BUILDINGS_TABLE),
            buffer=Literal(buffer),
        ).as_string(pg_conn)


def bounds_mask_sql(pg_uri: str, job_id: int, srid: int) -> str:
    with connection(pg_uri, cursor_factory=psycopg2.extras.DictCursor) as pg_conn:
        return SQL(
            "SELECT ST_Transform(bounds, {srid}) "
            "FROM models.job_queue q "
            "WHERE q.job_id={job_id} "
        ).format(job_id=Literal(job_id), srid=Literal(srid)).as_string(pg_conn)