import logging

import os
from os.path import join

from psycopg2.sql import Identifier
from typing import List, Dict

from albion_models.db_funcs import sql_script, sql_command
from albion_models.gdal_helpers import run
from albion_models.lidar.lidar import LidarTile, Resolution


def load_lidar(pg_conn, tiles: List[LidarTile], temp_dir: str):
    if len(tiles) == 0:
        return

    tiles_by_res = _split_by_res(tiles)
    os.makedirs(temp_dir, exist_ok=True)
    errors = 0

    # tile sizes of 1000/500/250 mean that all resolutions have the same tile sizes
    # and all the different lidar sources (Eng/Scot/Wales) tiles can be chopped
    # up to fit exactly
    # This is relied on by functionality in the lidar coverage model and the lidar
    # tile preparation for the heat demand model
    r = _tiles_to_insert(pg_conn, tiles_by_res[Resolution.R_50CM], Resolution.R_50CM)
    errors += rasters_to_postgis(pg_conn, r, "models.lidar_50cm", temp_dir, tile_size=1000)

    r = _tiles_to_insert(pg_conn, tiles_by_res[Resolution.R_1M], Resolution.R_1M)
    errors += rasters_to_postgis(pg_conn, r, "models.lidar_1m", temp_dir, tile_size=500)

    r = _tiles_to_insert(pg_conn, tiles_by_res[Resolution.R_2M], Resolution.R_2M)
    errors += rasters_to_postgis(pg_conn, r, "models.lidar_2m", temp_dir, tile_size=250)

    error_pct = round(errors / len(tiles) * 100, 2)
    logging.info(f"LiDAR loaded, {errors} / {len(tiles)} ({error_pct}%) errored")


def rasters_to_postgis(pg_conn, rasters: List[str], table: str, temp_dir: str, tile_size: int) -> int:
    if len(rasters) == 0:
        return 0

    sql_file = join(temp_dir, "raster.sql")
    errors = 0
    for raster in rasters:
        try:
            cmd = f"raster2pgsql -n filename -x -a -R -t {tile_size}x{tile_size} {raster} {table} > {sql_file}"
            run(cmd)
            sql_script(pg_conn, sql_file)
        except Exception as e:
            pg_conn.rollback()
            logging.warning("Failed to import raster", exc_info=e)
            errors += 1

    _add_raster_constraints(pg_conn, table)

    try:
        os.remove(sql_file)
    except OSError:
        pass

    return errors


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


def _coverage(pg_conn, job_id: int, res: Resolution) -> float:
    """
    This won't be exact due to not snapping the bounds polygon
    to a grid - but it's close enough for the types of bounds
    polygons we expect.
    """
    if res == Resolution.R_50CM:
        lidar_table = "lidar_50cm"
        divisor = 4
    elif res == Resolution.R_1M:
        lidar_table = "lidar_1m"
        divisor = 1
    elif res == Resolution.R_2M:
        lidar_table = "lidar_2m"
        divisor = 0.25
    else:
        raise ValueError(f"Unknown resolution {res}")

    return sql_command(
        pg_conn,
        """
        SELECT
            (SUM(st_count(st_clip(l.rast, jq.bounds))) / %(divisor)s ) 
                / MAX(st_area(jq.bounds)) 
        FROM {lidar_table} l
        LEFT JOIN models.job_queue jq ON st_intersects(l.rast, jq.bounds) 
        WHERE jq.job_id = %(job_id)s
        """,
        bindings={"job_id": job_id, "divisor": divisor},
        lidar_table=Identifier("models", lidar_table),
        result_extractor=lambda rows: rows[0][0] or 0.0)


def _target_resolution(pg_conn, job_id) -> Resolution:
    _50cm_cov = _coverage(pg_conn, job_id, Resolution.R_50CM)
    _1m_cov = _coverage(pg_conn, job_id, Resolution.R_1M)
    _2m_cov = _coverage(pg_conn, job_id, Resolution.R_2M)
    logging.info(f"LiDAR coverage:  50cm: {_50cm_cov}, 1m: {_1m_cov}, 2m: {_2m_cov}")

    # We don't currently use 50cm res LiDAR unless merged into 1m or 2m as it's too slow
    # when loading raster pixel data into the database or running RANSAC.
    # it will still be merged into the 1m, though, so if there's more of it than 1m use
    # its coverage %:
    _1m_cov = max(_50cm_cov, _1m_cov)
    if _1m_cov < 0.25 and _2m_cov > _1m_cov + 0.5:
        target_res = Resolution.R_2M
    else:
        target_res = Resolution.R_1M
    logging.info(f"Using resolution {target_res}")
    return target_res


def get_merged_lidar_tiles(pg_conn, job_id, output_dir: str) -> List[str]:
    target_res = _target_resolution(pg_conn, job_id)

    # RANSAC produces bad outputs if lower resolutions are merged into higher (e.g. 2m into 1m)
    # as essentially the 2m tile is converted into 4 1m tiles and RANSAC rightly treats that
    # as a flat step. So we only merge higher res into lower (e.g. 50cm into 1m)

    # Use 2m, with 1m and 50cm merged in:
    if target_res == Resolution.R_2M:
        logging.info(f"Using 2m LiDAR")
        sql = """
        WITH all_res AS (
            SELECT 
                ST_Resample(l.rast, (SELECT rast FROM models.lidar_2m ORDER BY filename LIMIT 1)) AS rast, 
                ST_UpperLeftX(l.rast) x, 
                ST_UpperLeftY(l.rast) y,
                0.5 AS res
            FROM models.job_queue q 
            INNER JOIN models.lidar_50cm l ON st_intersects(l.rast, q.bounds)
            WHERE q.job_id = %(job_id)s
        UNION ALL
            SELECT 
                ST_Resample(l.rast, (SELECT rast FROM models.lidar_2m ORDER BY filename LIMIT 1)) AS rast, 
                ST_UpperLeftX(l.rast) x, 
                ST_UpperLeftY(l.rast) y,
                1.0 AS res
            FROM models.job_queue q 
            INNER JOIN models.lidar_1m l ON st_intersects(l.rast, q.bounds)
            WHERE q.job_id = %(job_id)s
        UNION ALL
            SELECT 
                l.rast, 
                ST_UpperLeftX(l.rast) x, 
                ST_UpperLeftY(l.rast) y, 
                2.0 AS res
            FROM models.job_queue q 
            INNER JOIN models.lidar_2m l ON st_intersects(l.rast, q.bounds)
            WHERE q.job_id = %(job_id)s
        )
        SELECT
            x, y, ST_AsGDALRaster(ST_Union(rast ORDER BY res DESC), 'GTiff') AS rast 
        FROM all_res
        GROUP BY x, y
        """
    # Use 1m, with 50cm merged in:
    else:
        sql = """
        WITH all_res AS (
            SELECT 
                ST_Resample(l.rast, (SELECT rast FROM models.lidar_1m ORDER BY filename LIMIT 1)) AS rast, 
                ST_UpperLeftX(l.rast) x, 
                ST_UpperLeftY(l.rast) y,
                0.5 AS res
            FROM models.job_queue q 
            INNER JOIN models.lidar_50cm l ON st_intersects(l.rast, q.bounds)
            WHERE q.job_id = %(job_id)s
        UNION ALL
            SELECT 
                l.rast, 
                ST_UpperLeftX(l.rast) x, 
                ST_UpperLeftY(l.rast) y, 
                1.0 AS res
            FROM models.job_queue q 
            INNER JOIN models.lidar_1m l ON st_intersects(l.rast, q.bounds)
            WHERE q.job_id = %(job_id)s
        )
        SELECT
            x, y, ST_AsGDALRaster(ST_Union(rast ORDER BY res DESC), 'GTiff') AS rast 
        FROM all_res
        GROUP BY x, y
        """

    rasters = sql_command(
        pg_conn,
        sql,
        bindings={"job_id": job_id},
        result_extractor=lambda res: res)

    paths = []
    for raster in rasters:
        filename = f"{int(raster['x'])}.{int(raster['y'])}.{job_id}.tiff"
        file_path = join(output_dir, filename)
        with open(file_path, 'wb') as f:
            f.write(raster['rast'])
        paths.append(file_path)
    return paths


def raster_tile_coverage_count(pg_conn, job_id: int) -> int:
    target_res = _target_resolution(pg_conn, job_id)

    sql = """
    SELECT COUNT(*)
    FROM models.job_queue q 
    INNER JOIN {lidar_table} l ON st_intersects(l.rast, q.bounds)
    WHERE q.job_id = %(job_id)s
    """

    if target_res == Resolution.R_2M:
        count_2m = sql_command(
            pg_conn,
            sql,
            bindings={"job_id": job_id},
            lidar_table=Identifier("models", "lidar_2m"),
            result_extractor=lambda res: res[0][0])
    else:
        count_2m = 0

    count_1m = sql_command(
        pg_conn,
        sql,
        bindings={"job_id": job_id},
        lidar_table=Identifier("models", "lidar_1m"),
        result_extractor=lambda res: res[0][0])

    count_50cm = sql_command(
        pg_conn,
        sql,
        bindings={"job_id": job_id},
        lidar_table=Identifier("models", "lidar_50cm"),
        result_extractor=lambda res: res[0][0])

    return count_2m + count_1m + count_50cm
