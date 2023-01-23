import json
import logging
import multiprocessing as mp
import time
from typing import Tuple, List

import math
import psycopg2.extras
from psycopg2.sql import SQL, Identifier, Literal

from albion_models.db_funcs import count, sql_command, connection
from albion_models.lidar.lidar import LIDAR_NODATA
from albion_models.solar_pv import tables
from albion_models.solar_pv.outdated_lidar.perimeter_gradient import \
    check_perimeter_gradient, HeightAggregator
from albion_models.util import get_cpu_count


def _lidar_check_cpu_count():
    """Use 3/4s of available CPUs for lidar checking"""
    return min(int(get_cpu_count() * 0.75), 100)


def check_lidar(pg_uri: str,
                job_id: int,
                resolution_metres: float,
                workers: int = _lidar_check_cpu_count(),
                page_size: int = 3000):
    """
    Check for discrepancies between OS MasterMap building polygon data and
    LiDAR data.

    Currently English LiDAR data is mostly from 2017 so is starting to be
    out-of-date for newly built things. In these cases, if unhandled, the LiDAR
    detects all buildings as flat (or like the ground that they were built on was) -
    or occasionally with a now-nonexistent building intersecting the polygon weirdly.
    """
    with connection(pg_uri, cursor_factory=psycopg2.extras.DictCursor) as pg_conn:
        if _already_checked(pg_conn, job_id):
            logging.info("Already checked LiDAR coverage, skipping...")
            return

    pages = math.ceil(count(pg_uri, tables.schema(job_id), tables.BUILDINGS_TABLE) / page_size)
    logging.info(f"{pages} pages of size {page_size} buildings to check LiDAR coverage for")
    logging.info(f"Using {workers} processes for LiDAR coverage check")

    with mp.get_context("spawn").Pool(workers) as pool:
        wrapped_iterable = ((pg_uri, job_id, resolution_metres, page, page_size)
                            for page in range(0, pages))
        for res in pool.starmap(_check_lidar_page, wrapped_iterable):
            pass

    logging.info(f"LiDAR coverage check complete")


def _check_lidar_page(pg_uri: str, job_id: int, resolution_metres: float, page: int, page_size: int = 1000):
    start_time = time.time()
    with connection(pg_uri, cursor_factory=psycopg2.extras.DictCursor) as pg_conn:
        buildings = _load_buildings(pg_conn, job_id, page, page_size)
        to_write = []

        for building in buildings:
            try:
                reason = _check_building(building, resolution_metres)
                height = HeightAggregator(building['pixels']).height() if reason is None else None
                to_write.append((building['toid'], reason, height))
            except Exception as e:
                print("outdated LiDAR check failed on building:")
                print(json.dumps(building, sort_keys=True, default=str))
                raise e

        _write_exclusions(pg_conn, job_id, to_write)
        print(f"Checked page {page} of LiDAR, took {round(time.time() - start_time, 2)} s.")


def _check_building(building, resolution_metres: float, debug: bool = False):
    reason = _check_coverage(building)
    if not reason:
        reason = check_perimeter_gradient(building, resolution_metres, debug=debug)
    return reason


def _check_coverage(building):
    for pixel in building['pixels']:
        if pixel['within_building']:
            return None
    return 'NO_LIDAR_COVERAGE'


def _write_exclusions(pg_conn, job_id: int, to_exclude: List[Tuple[str, str, float]]):
    with pg_conn.cursor() as cursor:
        psycopg2.extras.execute_values(
            cursor,
            SQL("""
                UPDATE {buildings}
                SET 
                    exclusion_reason = data.exclusion_reason::models.pv_exclusion_reason,
                    height = data.height::real
                FROM (VALUES %s) AS data (toid, exclusion_reason, height)
                WHERE {buildings}.toid = data.toid;
            """).format(
                buildings=Identifier(tables.schema(job_id), tables.BUILDINGS_TABLE),
            ), argslist=to_exclude)
        pg_conn.commit()


def _load_buildings(pg_conn, job_id: int, page: int, page_size: int = 1000, toids: List[str] = None):
    if toids:
        where_clause = SQL("WHERE b.toid = ANY( {toids} )") \
            .format(toids=Literal(toids)) \
            .as_string(pg_conn)
    else:
        where_clause = ""

    buildings = sql_command(
        pg_conn,
        """
        SELECT b.toid, ST_AsText(b.geom_27700) AS geom
        FROM {buildings} b
        """ + where_clause + """
        ORDER BY b.toid
        OFFSET %(offset)s LIMIT %(limit)s
        """,
        {
            "offset": page * page_size,
            "limit": page_size,
        },
        buildings=Identifier(tables.schema(job_id), tables.BUILDINGS_TABLE),
        result_extractor=lambda rows: [dict(row) for row in rows]
    )

    pixels = sql_command(
        pg_conn,
        """
        WITH building_page AS (
            SELECT b.toid, b.geom_27700
            FROM {buildings} b
            """ + where_clause + """
            ORDER BY b.toid
            OFFSET %(offset)s LIMIT %(limit)s
        )
        SELECT
            h.pixel_id,
            h.elevation,
            b.toid,
            ST_Contains(b.geom_27700, h.en) AS within_building,
            h.toid IS NULL AS without_building,
            h.easting AS x,
            h.northing AS y
        FROM building_page b
        LEFT JOIN {lidar_pixels} h ON ST_Contains(ST_Buffer(b.geom_27700, 5), h.en)
        WHERE h.elevation != %(lidar_nodata)s
        ORDER BY b.toid;
        """,
        {
            "offset": page * page_size,
            "limit": page_size,
            "lidar_nodata": LIDAR_NODATA,
        },
        lidar_pixels=Identifier(tables.schema(job_id), tables.LIDAR_PIXEL_TABLE),
        buildings=Identifier(tables.schema(job_id), tables.BUILDINGS_TABLE),
        result_extractor=lambda rows: [dict(row) for row in rows])

    buildings_by_toid = {}
    for building in buildings:
        building['pixels'] = []
        buildings_by_toid[building['toid']] = building
    for pixel in pixels:
        building = buildings_by_toid[pixel['toid']]
        building['pixels'].append(pixel)
    return buildings


def _already_checked(pg_conn, job_id: int) -> bool:
    return sql_command(
        pg_conn,
        """
        SELECT COUNT(*) != 0 FROM {buildings} WHERE exclusion_reason IS NOT NULL
        """,
        buildings=Identifier(tables.schema(job_id), tables.BUILDINGS_TABLE),
        result_extractor=lambda rows: rows[0][0])


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='[%(asctime)s] %(levelname)s: %(message)s')
    check_lidar("postgresql://albion_ddl:albion320@localhost:5432/albion", 1618, 1.0)
