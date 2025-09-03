# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import logging

import os
import tempfile
from collections import defaultdict
from os.path import join

from psycopg2.sql import Identifier, SQL, Literal
from typing import List, Dict

from solar_pv.db_funcs import sql_script, sql_command
from solar_pv.gdal_helpers import run
from solar_pv.lidar.lidar import LidarTile, Resolution
from solar_pv import tables


def load_lidar(pg_conn, tiles: List[LidarTile]):
    if len(tiles) == 0:
        return

    tiles_by_res = defaultdict(list)
    for tile in tiles:
        tiles_by_res[tile.resolution].append(tile)
    errors = 0
    loaded = 0

    # tile sizes of 1000/500/250 mean that all resolutions have the same tile sizes
    # and all the different lidar sources (Eng/Scot/Wales) tiles can be chopped
    # up to fit exactly:
    for res, tile_size, table in [[Resolution.R_50CM, 1000, "models.lidar_50cm"],
                                  [Resolution.R_1M,    500, "models.lidar_1m"],
                                  [Resolution.R_2M,    250, "models.lidar_2m"]]:
        res_tiles = _tiles_to_insert(pg_conn, tiles_by_res[res], res)
        paths = [t.filename for t in res_tiles]
        errors += rasters_to_postgis(pg_conn, paths, table, tile_size=tile_size, allow_errs=True)
        loaded += len(res_tiles)
        for tile in res_tiles:
            set_tile_metadata(pg_conn, tile, table)

    error_pct = round(errors / loaded * 100, 2) if loaded != 0 else 0.0
    logging.info(f"LiDAR loaded, {errors} / {loaded} ({error_pct}%) errored")
    if errors > 0:
        raise ValueError("Failed to import some rasters")


def rasters_to_postgis(pg_conn, rasters: List[str], table: str, tile_size: int,
                       allow_errs: bool = False,
                       nodata_val: int = None,
                       srid: int = None) -> int:
    if len(rasters) == 0:
        return 0

    with tempfile.TemporaryDirectory() as temp_dir:
        sql_file = join(temp_dir, "raster.sql")
        errors = 0
        nodata = f'-N "{nodata_val}"' if nodata_val is not None else ''
        srid = f'-s "{int(srid)}"' if srid is not None else ''
        for raster in rasters:
            try:
                cmd = f'raster2pgsql -n filename {nodata} {srid} -x -a -R -t "{tile_size}x{tile_size}" "{raster}" "{table}" > {sql_file}'
                run(cmd)
                sql_script(pg_conn, sql_file)
            except Exception as e:
                pg_conn.rollback()
                logging.warning("Failed to import raster", exc_info=e)
                errors += 1
                if not allow_errs:
                    raise e

    add_raster_constraints(pg_conn, table)

    return errors


def create_raster_table(pg_conn, raster_table: str, drop: bool = False) -> None:
    schema, rtable = raster_table.split(".") if "." in raster_table else ("public", raster_table)
    if drop:
        sql_command(pg_conn, "DROP TABLE IF EXISTS {table}", table=Identifier(schema, rtable))

    sql_command(
        pg_conn,
        """
        CREATE TABLE IF NOT EXISTS {table} (
            rid serial PRIMARY KEY,
            rast raster NOT NULL,
            filename text NOT NULL
        );
        
        CREATE INDEX ON {table} USING gist (st_convexhull(rast));
        """,
        table=Identifier(schema, rtable)
    )


def _tiles_to_insert(pg_conn, tiles: List[LidarTile], res: Resolution) -> List[LidarTile]:
    """
    Returns the tiles in the `tiles` list that are
    not already on the database.
    """
    if len(tiles) == 0:
        return []

    if res == Resolution.R_50CM:
        table = ("models", "lidar_50cm")
    elif res == Resolution.R_1M:
        table = ("models", "lidar_1m")
    elif res == Resolution.R_2M:
        table = ("models", "lidar_2m")
    else:
        raise ValueError(f"Unknown resolution {res}")

    wanted_paths = sql_command(
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
        bindings={"paths": [t.filename for t in tiles]},
        table=Identifier(*table),
        result_extractor=lambda rows: [row[0] for row in rows])

    wanted_paths = set(wanted_paths)
    return [t for t in tiles if t.filename in wanted_paths]


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
        result_extractor=lambda rows: rows[0][0] if len(rows) > 0 else False)


def add_raster_constraints(pg_conn, table: str):
    """
    Raster table constraints need to be added after there is some data in the table,
    as they're calculated from the existing files.

    * Don't set the 'regular_blocking' restraint, as we allow each resolution to contain
    multiple tiles with the same extent from different lidar sources.
    * Don't set the 'extent' restraint, as we load tiles in multiple batches, after the
    setting of the constraints, and the extent allowed would be calculated from the
    first batch of tiles only.
    """
    if not _has_raster_constraints(pg_conn, table):
        schema, table = tuple(table.split('.')) if "." in table else (None, table)
        sql_command(
            pg_conn,
            # srid scale_x scale_y blocksize_x blocksize_y same_alignment regular_blocking num_bands pixel_types nodata_values out_db extent
            """
            SELECT AddRasterConstraints(%(schema)s,%(table)s,'rast',TRUE,TRUE,TRUE,TRUE,TRUE,TRUE,FALSE,TRUE,TRUE,TRUE,TRUE,FALSE);
            """,
            bindings={"table": table,
                      "schema": schema})


def set_tile_metadata(pg_conn, tile: LidarTile, table: str):
    """
    Update the raster entries created from the tile with the year and product of the tile.
    """
    schema, table = tuple(table.split('.')) if "." in table else (None, table)
    sql_command(
        pg_conn,
        """
        UPDATE {table} SET year = %(year)s, product = %(product)s 
        WHERE filename = %(file)s
        """,
        table=Identifier(schema, table),
        bindings={
            "year": tile.year,
            "product": tile.product,
            "file": os.path.basename(tile.filename),
        }
    )


# TODO remove dependency on models.job_queue
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


# TODO remove dependency on models.job_queue
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


# TODO remove dependency on models.job_queue
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
                year,
                0.5 AS res
            FROM models.job_queue q 
            INNER JOIN models.lidar_50cm l ON st_intersects(l.rast, q.bounds)
            WHERE q.job_id = %(job_id)s
        UNION ALL
            SELECT 
                ST_Resample(l.rast, (SELECT rast FROM models.lidar_2m ORDER BY filename LIMIT 1)) AS rast, 
                ST_UpperLeftX(l.rast) x, 
                ST_UpperLeftY(l.rast) y,
                year,
                1.0 AS res
            FROM models.job_queue q 
            INNER JOIN models.lidar_1m l ON st_intersects(l.rast, q.bounds)
            WHERE q.job_id = %(job_id)s
        UNION ALL
            SELECT 
                l.rast, 
                ST_UpperLeftX(l.rast) x, 
                ST_UpperLeftY(l.rast) y, 
                year,
                2.0 AS res
            FROM models.job_queue q 
            INNER JOIN models.lidar_2m l ON st_intersects(l.rast, q.bounds)
            WHERE q.job_id = %(job_id)s
        )
        SELECT
            x, y, ST_AsGDALRaster(ST_Union(rast ORDER BY year ASC, res DESC), 'GTiff') AS rast 
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
                year,
                0.5 AS res
            FROM models.job_queue q 
            INNER JOIN models.lidar_50cm l ON st_intersects(l.rast, q.bounds)
            WHERE q.job_id = %(job_id)s
        UNION ALL
            SELECT 
                l.rast, 
                ST_UpperLeftX(l.rast) x, 
                ST_UpperLeftY(l.rast) y, 
                year,
                1.0 AS res
            FROM models.job_queue q 
            INNER JOIN models.lidar_1m l ON st_intersects(l.rast, q.bounds)
            WHERE q.job_id = %(job_id)s
        )
        SELECT
            x, y, ST_AsGDALRaster(ST_Union(rast ORDER BY year ASC, res DESC), 'GTiff') AS rast 
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


# TODO remove dependency on models.job_queue
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


def pixels_for_buildings(pg_conn,
                         job_id: int,
                         page: int,
                         page_size: int,
                         raster_tables: List[str],
                         toids: List[str] = None,
                         geom_col: str = 'geom_27700',
                         force_load: bool = False) -> Dict[str, List[dict]]:
    """
    Get a list of pixels by toid. Each pixel dict will have keys x, y, pixel_id and toid,
    and one for each table in `raster_tables`, where the key will be the table name (without
    schema).

    If force_load is True, load buildings despite a set exclusion_reason. This is for
    debugging only.
    """
    if toids:
        toid_filter = SQL("AND b.toid = ANY( {toids} )").format(toids=Literal(toids))
    else:
        toid_filter = SQL("")

    if force_load:
        where_clause = SQL("true")
    else:
        where_clause = SQL("b.exclusion_reason IS NULL")

    if geom_col not in ('geom_27700', 'geom_27700_buffered_5'):
        raise ValueError(f"Unrecognised geom col: {geom_col}")

    by_pixel_id = {}
    fields = []

    for raster_table in raster_tables:
        schema, rtable = raster_table.split(".") if "." in raster_table else ("public", raster_table)
        fields.append(rtable)
        pixels = sql_command(
            pg_conn,
            """        
            WITH building_page AS (
                SELECT b.toid, {geom_col}
                FROM {buildings} b
                WHERE {where_clause}
                {toid_filter}
                ORDER BY b.toid
                OFFSET %(offset)s LIMIT %(limit)s
            ),
            raster_pixels AS (
                SELECT
                    b.toid,
                    (ST_PixelAsCentroids(ST_Clip(rast, {geom_col}))).*
                FROM building_page b
                LEFT JOIN {raster_table} r ON ST_Intersects({geom_col}, r.rast)
            )
            SELECT
                toid || ':' || ST_X(geom)::text || ':' || ST_Y(geom)::text AS pixel_id,
                val,
                toid,
                ST_X(geom) x,
                ST_Y(geom) y
            FROM raster_pixels;
            """,
            {
                "offset": page * page_size,
                "limit": page_size,
            },
            buildings=Identifier(tables.schema(job_id), tables.BUILDINGS_TABLE),
            raster_table=Identifier(schema, rtable),
            geom_col=Identifier("b", geom_col),
            toid_filter=toid_filter,
            where_clause=where_clause,
            result_extractor=lambda rows: rows)

        for pixel in pixels:
            pixel_id = pixel['pixel_id']
            if pixel_id not in by_pixel_id:
                by_pixel_id[pixel_id] = dict(pixel)
            by_pixel_id[pixel_id][rtable] = pixel['val']

    by_toid = defaultdict(list)
    for pixel in by_pixel_id.values():
        del pixel['val']
        # Only return pixels that have a value in every table:
        if all(field in pixel for field in fields):
            by_toid[pixel['toid']].append(pixel)
    return dict(by_toid)
