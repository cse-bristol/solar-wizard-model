import logging

import os
from os.path import join

from psycopg2.sql import Identifier
from typing import List, Set, Dict

from albion_models.db_funcs import sql_script, sql_command
from albion_models.gdal_helpers import run
from albion_models.lidar.lidar import LidarTile, Resolution, USE_50CM_THRESHOLD


def load_lidar(pg_conn, tiles: List[LidarTile], temp_dir: str):
    tiles_by_res = _split_by_res(tiles)
    os.makedirs(temp_dir, exist_ok=True)

    # tile sizes of 1000/500/250 mean that all resolutions have the same tile sizes
    # and all the different lidar sources (Eng/Scot/Wales) tiles can be chopped
    # up to fit exactly
    # This is relied on by functionality in the lidar coverage model and the lidar
    # tile preparation for the heat demand model
    r = _tiles_to_insert(pg_conn, tiles_by_res[Resolution.R_50CM], Resolution.R_50CM)
    rasters_to_postgis(pg_conn, r, "models.lidar_50cm", temp_dir, tile_size=1000)

    r = _tiles_to_insert(pg_conn, tiles_by_res[Resolution.R_1M], Resolution.R_1M)
    rasters_to_postgis(pg_conn, r, "models.lidar_1m", temp_dir, tile_size=500)

    r = _tiles_to_insert(pg_conn, tiles_by_res[Resolution.R_2M], Resolution.R_2M)
    rasters_to_postgis(pg_conn, r, "models.lidar_2m", temp_dir, tile_size=250)


def rasters_to_postgis(pg_conn, rasters: List[str], table: str, temp_dir: str, tile_size: int):
    if len(rasters) == 0:
        return

    sql_file = join(temp_dir, "raster.sql")
    rasters_str = ' '.join(rasters)
    cmd = f"raster2pgsql -n filename -x -a -R -t {tile_size}x{tile_size} {rasters_str} {table} > {sql_file}"
    run(cmd)
    sql_script(pg_conn, sql_file)
    _add_raster_constraints(pg_conn, table)

    try:
        os.remove(sql_file)
    except OSError:
        pass


def _tiles_to_insert(pg_conn, paths: List[str], res: Resolution) -> List[str]:
    """
    Returns the tiles in the `tiles` list that are
    not already on the database.
    """
    if len(paths) == 0:
        return []

    if res == Resolution.R_50CM:
        table = ("models", "lidar_50cm")
    elif res == Resolution.R_1M:
        table = ("models", "lidar_1m")
    elif res == Resolution.R_2M:
        table = ("models", "lidar_2m")
    else:
        raise ValueError(f"Unknown resolution {res}")

    return sql_command(
        pg_conn,
        """
        WITH ins as (
            SELECT UNNEST(%(paths)s) AS filepath
        ) 
        SELECT ins.filepath 
        FROM ins 
        LEFT JOIN {table} ON ins.filepath LIKE '%%' || {table}.filename 
        WHERE {table}.filename is null;
        """,
        bindings={"paths": paths},
        table=Identifier(*table),
        result_extractor=lambda rows: [row[0] for row in rows])


def _split_by_res(tiles: List[LidarTile]) -> Dict[Resolution, List[str]]:
    t_res = {
        Resolution.R_50CM: [],
        Resolution.R_1M: [],
        Resolution.R_2M: [],
    }
    for tile in tiles:
        t_res[tile.resolution].append(tile.filename)
    return t_res


def _has_raster_constraints(pg_conn, table: str) -> bool:
    schema, table = tuple(table.split('.')) if "." in table else (None, table)
    return sql_command(
        pg_conn,
        """
        SELECT srid != 0
        FROM raster_columns
        WHERE r_table_name = %(table)s AND r_table_schema = %(schema)s
        """,
        bindings={"table": table,
                  "schema": schema},
        result_extractor=lambda rows: rows[0][0])


def _add_raster_constraints(pg_conn, table: str):
    """
    Raster table constraints need to be added after there is some data in the
    table, as they're calculated from the existing files.
    """
    if not _has_raster_constraints(pg_conn, table):
        logging.info(f"No raster constraints detected for {table}, adding them")
        schema, table = tuple(table.split('.')) if "." in table else (None, table)
        sql_command(
            pg_conn,
            # srid scale_x scale_y blocksize_x blocksize_y same_alignment regular_blocking num_bands pixel_types nodata_values out_db extent
            """
            SELECT AddRasterConstraints(%(schema)s,%(table)s,'rast',TRUE,TRUE,TRUE,TRUE,TRUE,TRUE,FALSE,TRUE,TRUE,TRUE,TRUE,FALSE);
            """,
            bindings={"table": table,
                      "schema": schema})


def _50cm_coverage(pg_conn, job_id: int) -> float:
    """
    This won't be exact due to not snapping the bounds polygon
    to a 50cm grid - but it's close enough for the types of bounds
    polygons we expect.
    """
    return sql_command(
        pg_conn,
        """
        SELECT
            -- Divide by 4 as this is the count of 0.5m2 pixels 
            (SUM(st_count(st_clip(l.rast, jq.bounds))) / 4) 
                / MAX(st_area(jq.bounds)) 
        FROM models.lidar_50cm l
        LEFT JOIN models.job_queue jq ON st_intersects(l.rast, jq.bounds) 
        WHERE jq.job_id = %(job_id)s
        """,
        bindings={"job_id": job_id},
        result_extractor=lambda rows: rows[0][0] or 0.0
    )


def get_merged_lidar(pg_conn, job_id: int, output_file: str):
    _50cm_cov = _50cm_coverage(pg_conn, job_id)
    logging.info(f"50cm LiDAR coverage is {_50cm_cov}, threshold is {USE_50CM_THRESHOLD}")

    if _50cm_cov >= USE_50CM_THRESHOLD:
        logging.info(f"Using 50cm coverage")
        sql = """
        WITH all_res AS (
                SELECT filename, ST_Resample(rast, (select rast from models.lidar_50cm limit 1)) AS rast 
                FROM models.lidar_2m
                LEFT JOIN models.job_queue q 
                ON st_intersects(rast, ST_Buffer(q.bounds, coalesce((q.params->>'horizon_search_radius')::int, 0)))
                WHERE q.job_id = %(job_id)s
            UNION ALL
                SELECT filename, ST_Resample(rast, (select rast from models.lidar_50cm limit 1)) AS rast 
                FROM models.lidar_1m
                LEFT JOIN models.job_queue q 
                ON st_intersects(rast, ST_Buffer(q.bounds, coalesce((q.params->>'horizon_search_radius')::int, 0)))
                WHERE q.job_id = %(job_id)s
            UNION ALL
                SELECT filename, rast 
                FROM models.lidar_50cm
                LEFT JOIN models.job_queue q 
                ON st_intersects(rast, ST_Buffer(q.bounds, coalesce((q.params->>'horizon_search_radius')::int, 0)))
                WHERE q.job_id = %(job_id)s
        ) 
        SELECT 
            ST_AsGDALRaster(ST_Union(rast), 'GTiff') AS rast 
        FROM all_res
        """
    else:
        logging.info(f"Not using 50cm coverage")
        sql = """
        WITH all_res AS (
                SELECT filename, ST_Resample(rast, (select rast from models.lidar_1m limit 1)) AS rast 
                FROM models.lidar_2m
                LEFT JOIN models.job_queue q 
                ON st_intersects(rast, ST_Buffer(q.bounds, coalesce((q.params->>'horizon_search_radius')::int, 0)))
                WHERE q.job_id = %(job_id)s
            UNION ALL
                SELECT filename, rast 
                FROM models.lidar_1m
                LEFT JOIN models.job_queue q 
                ON st_intersects(rast, ST_Buffer(q.bounds, coalesce((q.params->>'horizon_search_radius')::int, 0)))
                WHERE q.job_id = %(job_id)s
        ) 
        SELECT 
            ST_AsGDALRaster(ST_Union(rast), 'GTiff') AS rast 
        FROM all_res
        """

    raster = sql_command(
        pg_conn, sql, {"job_id": job_id}, result_extractor=lambda res: res[0][0])

    with open(output_file, 'wb') as f:
        f.write(raster)


def get_merged_lidar_tiles(pg_conn, job_id, output_dir: str) -> List[str]:
    _50cm_cov = _50cm_coverage(pg_conn, job_id)
    logging.info(f"50cm LiDAR coverage is {_50cm_cov}, threshold is {USE_50CM_THRESHOLD}")

    if _50cm_cov >= USE_50CM_THRESHOLD:
        sql = """
        WITH all_res AS (
            SELECT 
                ST_Resample(l.rast, (select rast from models.lidar_50cm limit 1)) AS rast, 
                ST_UpperLeftX(l.rast) x, 
                ST_UpperLeftY(l.rast) y
            FROM models.job_queue q 
            LEFT JOIN models.lidar_2m l ON st_intersects(l.rast, q.bounds)
            WHERE q.job_id = %(job_id)s
        UNION ALL
            SELECT 
                ST_Resample(l.rast, (select rast from models.lidar_50cm limit 1)) AS rast, 
                ST_UpperLeftX(l.rast) x, 
                ST_UpperLeftY(l.rast) y
            FROM models.job_queue q 
            LEFT JOIN models.lidar_1m l ON st_intersects(l.rast, q.bounds)
            WHERE q.job_id = %(job_id)s
        UNION ALL
            SELECT l.rast, ST_UpperLeftX(l.rast) x, ST_UpperLeftY(l.rast) y
            FROM models.job_queue q 
            LEFT JOIN models.lidar_50cm l ON st_intersects(l.rast, q.bounds)
            WHERE q.job_id = %(job_id)s
        )
        SELECT
            x, y, ST_AsGDALRaster(ST_Union(rast), 'GTiff') AS rast 
        FROM all_res
        GROUP BY x, y
        """
    else:
        sql = """
        WITH all_res AS (
            SELECT 
                ST_Resample(l.rast, (select rast from models.lidar_1m limit 1)) AS rast, 
                ST_UpperLeftX(l.rast) x, 
                ST_UpperLeftY(l.rast) y
            FROM models.job_queue q 
            LEFT JOIN models.lidar_2m l ON st_intersects(l.rast, q.bounds)
            WHERE q.job_id = %(job_id)s
        UNION ALL
            SELECT l.rast, ST_UpperLeftX(l.rast) x, ST_UpperLeftY(l.rast) y
            FROM models.job_queue q 
            LEFT JOIN models.lidar_1m l ON st_intersects(l.rast, q.bounds)
            WHERE q.job_id = %(job_id)s
        )
        SELECT
            x, y, ST_AsGDALRaster(ST_Union(rast), 'GTiff') AS rast 
        FROM all_res
        GROUP BY x, y
        """

    rasters = sql_command(
        pg_conn,
        sql,
        bindings={"job_id": job_id},
        result_extractor=lambda res: res
    )

    paths = []
    for raster in rasters:
        filename = f"{int(raster['x'])}.{int(raster['y'])}.{job_id}.tiff"
        file_path = join(output_dir, filename)
        with open(file_path, 'wb') as f:
            f.write(raster['rast'])
        paths.append(file_path)
    return paths
