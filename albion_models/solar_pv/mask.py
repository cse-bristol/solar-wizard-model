from os.path import join

import psycopg2.extras
from psycopg2.sql import SQL, Identifier, Literal

import albion_models.solar_pv.tables as tables
from albion_models.db_funcs import connect
from albion_models import gdal_helpers


def create_buildings_mask(job_id: int,
                          solar_dir: str,
                          pg_uri: str,
                          res: float,
                          mask_table: str,
                          srid: int) -> str:
    """
    Create a raster mask from OS mastermap buildings that fall within the bounds
    of the job. Pixels inside a building will be 1, otherwise 0.
    """
    pg_conn = connect(pg_uri, cursor_factory=psycopg2.extras.DictCursor)
    try:
        mask_sql = SQL(
            """
            SELECT ST_Transform(b.geom_4326, {srid}) 
            FROM {mask_table} b 
            LEFT JOIN {bounds_4326} q 
            ON ST_Intersects(b.geom_4326, q.bounds) 
            WHERE q.job_id={job_id}
            """).format(
                bounds_4326=Identifier(tables.schema(job_id), tables.BOUNDS_TABLE),
                mask_table=Identifier(*mask_table.split(".")),
                job_id=Literal(job_id),
                srid=Literal(srid)).as_string(pg_conn)
    finally:
        pg_conn.close()

    mask_file = join(solar_dir, 'mask.tif')
    gdal_helpers.rasterize(pg_uri, mask_sql, mask_file, res, srid)
    return mask_file


def create_bounds_mask(job_id: int, solar_dir: str, pg_uri: str, res: float, srid: int) -> str:
    """
    Create a raster mask from the bounds polygon associated with the job. Pixels
    inside the bounds will be 1, all others 0.
    """
    pg_conn = connect(pg_uri, cursor_factory=psycopg2.extras.DictCursor)
    try:
        mask_sql = SQL(
            "SELECT ST_Transform(bounds, {srid}) FROM models.job_queue q WHERE q.job_id={job_id}"
        ).format(job_id=Literal(job_id), srid=Literal(srid)).as_string(pg_conn)
    finally:
        pg_conn.close()

    mask_file = join(solar_dir, 'mask.tif')
    gdal_helpers.rasterize(pg_uri, mask_sql, mask_file, res, srid)
    return mask_file
