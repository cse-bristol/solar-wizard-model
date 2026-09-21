# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import logging

import os
import tempfile
from collections import defaultdict
from os.path import join

from psycopg2.sql import Identifier, SQL, Literal
from typing import List, Dict, Tuple

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
    TODO ideally this would still say we should insert a file if the name matches but the
         year or product doesn't...
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


def get_job_bounds(pg_conn, job_id: int) -> Tuple[float, float, float, float]:
    """
    The job bounds (building extent) as (xmin, ymin, xmax, ymax) in EPSG:27700,
    read from the per-job `bounds_27700` table.
    """
    return sql_command(
        pg_conn,
        """
        SELECT ST_XMin(bounds_27700), ST_YMin(bounds_27700),
               ST_XMax(bounds_27700), ST_YMax(bounds_27700)
        FROM {bounds}
        """,
        bounds=Identifier(tables.schema(job_id), tables.BOUNDS_TABLE),
        result_extractor=lambda rows: tuple(rows[0]))


def pixels_for_buildings(pg_conn,
                         job_id: int,
                         page: int,
                         page_size: int,
                         raster_tables: List[str],
                         building_ids: List[str] = None,
                         geom_col: str = 'geom_27700',
                         force_load: bool = False) -> Dict[str, List[dict]]:
    """
    Get a list of pixels by building_id. Each pixel dict will have keys x, y, pixel_id and building_id,
    and one for each table in `raster_tables`, where the key will be the table name (without
    schema).

    If force_load is True, load buildings despite a set exclusion_reason. This is for
    debugging only.
    """
    if building_ids:
        building_id_filter = SQL("AND b.building_id = ANY( {building_ids} )").format(building_ids=Literal(building_ids))
    else:
        building_id_filter = SQL("")

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
                SELECT b.building_id, {geom_col}
                FROM {buildings} b
                WHERE {where_clause}
                {building_id_filter}
                ORDER BY b.building_id
                OFFSET %(offset)s LIMIT %(limit)s
            ),
            raster_pixels AS (
                SELECT
                    b.building_id,
                    (ST_PixelAsCentroids(ST_Clip(rast, {geom_col}))).*
                FROM building_page b
                LEFT JOIN {raster_table} r ON ST_Intersects({geom_col}, r.rast)
            )
            SELECT
                building_id || ':' || ST_X(geom)::text || ':' || ST_Y(geom)::text AS pixel_id,
                val,
                building_id,
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
            building_id_filter=building_id_filter,
            where_clause=where_clause,
            result_extractor=lambda rows: rows)

        for pixel in pixels:
            pixel_id = pixel['pixel_id']
            if pixel_id not in by_pixel_id:
                by_pixel_id[pixel_id] = dict(pixel)
            by_pixel_id[pixel_id][rtable] = pixel['val']

    by_building_id = defaultdict(list)
    for pixel in by_pixel_id.values():
        del pixel['val']
        # Only return pixels that have a value in every table:
        if all(field in pixel for field in fields):
            by_building_id[pixel['building_id']].append(pixel)
    return dict(by_building_id)
