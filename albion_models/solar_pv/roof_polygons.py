import logging

import psycopg2.extras
from psycopg2.sql import SQL, Identifier, Literal

from psycopg2.sql import Identifier

import albion_models.solar_pv.tables as tables
from albion_models import gdal_helpers
from albion_models.db_funcs import connect, sql_script_with_bindings, count, connection


def create_roof_polygons(pg_uri: str,
                         job_id: int,
                         max_roof_slope_degrees: int,
                         min_roof_area_m: int,
                         min_roof_degrees_from_north: int,
                         flat_roof_degrees: int,
                         large_building_threshold: float,
                         min_dist_to_edge_m: float,
                         min_dist_to_edge_large_m: float,
                         resolution_metres: float):
    schema = tables.schema(job_id)

    if count(pg_uri, schema, tables.ROOF_POLYGON_TABLE) > 0:
        logging.info("Not creating roof polygons, already done.")
        return

    pg_conn = connect(pg_uri)

    try:
        sql_script_with_bindings(
            pg_conn, 'pv/create.roof-polygons.sql',
            {
                "job_id": job_id,
                "max_roof_slope_degrees": max_roof_slope_degrees,
                "min_roof_area_m": min_roof_area_m,
                "min_roof_degrees_from_north": min_roof_degrees_from_north,
                "flat_roof_degrees": flat_roof_degrees,
                "resolution": resolution_metres,
                "large_building_threshold": large_building_threshold,
                "min_dist_to_edge_m": min_dist_to_edge_m,
                "min_dist_to_edge_large_m": min_dist_to_edge_large_m,
            },
            schema=Identifier(schema),
            lidar_pixels=Identifier(schema, tables.LIDAR_PIXEL_TABLE),
            roof_planes=Identifier(schema, tables.ROOF_PLANE_TABLE),
            roof_polygons=Identifier(schema, tables.ROOF_POLYGON_TABLE),
            buildings=Identifier(schema, tables.BUILDINGS_TABLE)
        )
    finally:
        pg_conn.close()


def get_flat_roof_mask_sql(pg_uri: str, job_id: int) -> str:
    with connection(pg_uri, cursor_factory=psycopg2.extras.DictCursor) as pg_conn:
        return SQL(
            "SELECT ST_Force3D(roof_geom_27700, aspect) FROM {roof_polygons} WHERE is_flat = true"
        ).format(
            roof_polygons=Identifier(tables.schema(job_id), tables.ROOF_POLYGON_TABLE)
        ).as_string(pg_conn)


def create_flat_roof_mask(mask_sql: str,
                mask_out: str,
                pg_uri: str,
                res: float,
                srid: int):
    gdal_helpers.rasterize_3d(pg_uri, mask_sql, mask_out, res, srid)
