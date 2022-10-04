import logging
import math
from dataclasses import dataclass
from typing import Dict, Any, Tuple, List, Optional
import multiprocessing as mp

import psycopg2.extras
from psycopg2.sql import SQL, Identifier, Literal

from albion_models.db_funcs import count, sql_command, connection
from albion_models.lidar.lidar import LIDAR_NODATA
from albion_models.solar_pv import tables
from albion_models.solar_pv.outdated_lidar.perimeter_gradient import \
    check_perimeter_gradient
from albion_models.util import get_cpu_count


@dataclass
class HeightChecker:
    pixels_within: int = 0
    pixels_without: int = 0
    within_elevation_sum: float = 0.0
    without_elevation_sum: float = 0.0
    osmm_height: Optional[float] = 0.0
    osmm_base_roof_height: Optional[float] = 0.0

    def __init__(self, building, debug: bool = False) -> None:
        self.osmm_height = building.get('height', None)
        self.osmm_base_roof_height = building.get('base_roof_height', None)
        self.debug = debug
        for pixel in building.get('pixels', []):
            self._process_pixel(pixel)

    def _process_pixel(self, pixel: Dict[str, Any]):
        if pixel['within_building']:
            self.pixels_within += 1
            self.within_elevation_sum += pixel['elevation']
        elif pixel['without_building']:
            self.pixels_without += 1
            self.without_elevation_sum += pixel['elevation']

    def average_heights(self) -> Tuple[float, float]:
        return (self.within_elevation_sum / self.pixels_within,
                self.without_elevation_sum / self.pixels_without)

    def lidar_height(self) -> float:
        avg_height_within, avg_height_without = self.average_heights()
        return avg_height_within - avg_height_without

    def lidar_ground_height(self) -> float:
        _, avg_height_without = self.average_heights()
        return avg_height_without

    def height_threshold(self) -> float:
        if self.pixels_within < 1000:
            return 1.1
        else:
            return 2

    def exclusion_reason(self) -> Optional[str]:
        """
        Conditions for excluding a building based on there being no LiDAR coverage:
        * no LiDAR pixels are found intersecting the building polygon.

        Conditions for excluding a building based on LiDAR being out-of-date:

        * More than 1 pixel in the 1m buffer around the building polygon does not
        fall inside any other buildings,
        * The height difference between the LiDAR pixels inside the polygon and
        those in a 1m buffer around the polygon that are not inside another building
        is less than or equal to 1m,
        * That height differs from the OS MasterMap building height for that toid
        by more than 1m (if there is an OSMM height).
        """
        if self.pixels_within == 0:
            return 'NO_LIDAR_COVERAGE'

        if self.pixels_without <= 1:
            return None

        lidar_height = self.lidar_height()
        height_threshold = self.height_threshold()

        if self.debug:
            print(f"OSMM heights: min {self.osmm_base_roof_height} avg {self.osmm_height}")
            print(f"avg LiDAR height: {lidar_height}")

        if lidar_height < 0.0:
            return 'OUTDATED_LIDAR_COVERAGE'
        elif lidar_height <= height_threshold:
            if self.osmm_height and abs(self.osmm_height - lidar_height) > 1:
                return 'OUTDATED_LIDAR_COVERAGE'
            elif not self.osmm_height:
                return 'OUTDATED_LIDAR_COVERAGE'

        if self.osmm_base_roof_height and self.osmm_base_roof_height / 4 > lidar_height:
            return 'OUTDATED_LIDAR_COVERAGE'

        return None


def _lidar_check_cpu_count():
    """Use 3/4s of available CPUs for lidar checking"""
    return int(get_cpu_count() * 0.75)


def check_lidar(pg_uri: str,
                job_id: int,
                workers: int = _lidar_check_cpu_count(),
                page_size: int = 1000):
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
        wrapped_iterable = ((pg_uri, job_id, page, page_size)
                            for page in range(0, pages))
        for res in pool.starmap(_check_lidar_page, wrapped_iterable):
            pass

    logging.info(f"LiDAR coverage check complete")


def _check_lidar_page(pg_uri: str, job_id: int, page: int, page_size: int = 1000):
    with connection(pg_uri, cursor_factory=psycopg2.extras.DictCursor) as pg_conn:
        buildings = _load_buildings(pg_conn, job_id, page, page_size)
        to_exclude = []

        for building in buildings:
            reason = _check_building(building)
            if reason:
                to_exclude.append((building['toid'], reason))

        _write_exclusions(pg_conn, job_id, to_exclude)
        print(f"Checked page {page} of LiDAR")


def _check_building(building, debug: bool = False):
    ha = HeightChecker(building, debug=debug)
    reason = ha.exclusion_reason()
    if not reason:
        reason = check_perimeter_gradient(building, debug=debug)
    return reason


def _write_exclusions(pg_conn, job_id: int, to_exclude: List[Tuple[str, str]]):
    with pg_conn.cursor() as cursor:
        psycopg2.extras.execute_values(
            cursor,
            SQL("""
                UPDATE {building_exclusion_reasons}
                SET exclusion_reason = data.exclusion_reason::models.pv_exclusion_reason
                FROM (VALUES %s) AS data (toid, exclusion_reason)
                WHERE {building_exclusion_reasons}.toid = data.toid;
            """).format(
                building_exclusion_reasons=Identifier(tables.schema(job_id),
                                                      tables.BUILDING_EXCLUSION_REASONS_TABLE),
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
        SELECT 
            b.toid, 
            ST_AsText(b.geom_27700) AS geom,
            hh.height,
            hh.rel_h2 AS base_roof_height
        FROM {buildings} b
        LEFT JOIN mastermap.height hh ON b.toid = hh.toid
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
        SELECT COUNT(*) != 0 
        FROM {building_exclusion_reasons}
        WHERE exclusion_reason IS NOT NULL
        """,
        building_exclusion_reasons=Identifier(tables.schema(job_id),
                                              tables.BUILDING_EXCLUSION_REASONS_TABLE),
        result_extractor=lambda rows: rows[0][0]
    )


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='[%(asctime)s] %(levelname)s: %(message)s')
    check_lidar("postgresql://albion_ddl:albion320@localhost:5432/albion", 1618)
