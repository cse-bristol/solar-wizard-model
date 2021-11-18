from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, Any, Tuple, List, Optional

import psycopg2.extras
from psycopg2.sql import SQL, Identifier

from albion_models.db_funcs import connect
from albion_models.solar_pv import tables


@dataclass
class HeightAggregator:
    pixels_within: int = 0
    pixels_without: int = 0
    within_elevation_sum: float = 0.0
    without_elevation_sum: float = 0.0
    osmm_height: Optional[float] = 0.0

    def aggregate_row(self, row: Dict[str, Any]):
        self.osmm_height = row['height']
        if row['within_building']:
            self.pixels_within += 1
            self.within_elevation_sum += row['elevation']
        elif row['without_building']:
            self.pixels_without += 1
            self.without_elevation_sum += row['elevation']

    def average_heights(self) -> Tuple[float, float]:
        return (self.within_elevation_sum / self.pixels_within,
                self.without_elevation_sum / self.pixels_without)

    def lidar_height(self) -> float:
        avg_height_within, avg_height_without = self.average_heights()
        return avg_height_within - avg_height_without

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
        if lidar_height <= 1:
            if self.osmm_height and abs(self.osmm_height - lidar_height) > 1:
                return 'OUTDATED_LIDAR_COVERAGE'
            elif not self.osmm_height:
                return 'OUTDATED_LIDAR_COVERAGE'

        return None


def check_lidar(pg_uri: str, job_id: int):
    """
    Check for discrepancies between OS MasterMap building polygon data and
    LiDAR data.

    Currently LiDAR data is mostly from 2017 so is starting to be
    out-of-date for newly built things. In these cases, if unhandled, the LiDAR
    detects all buildings as flat (or like the ground that they were built on was) -
    or occasionally with a now-nonexistent building intersecting the polygon weirdly.
    """
    # todo skip if done
    pg_conn = connect(pg_uri, cursor_factory=psycopg2.extras.DictCursor)
    try:
        rows = _load_building_pixels(pg_conn, job_id)
        by_toid = defaultdict(HeightAggregator)
        for row in rows:
            by_toid[row['toid']].aggregate_row(row)
        rows = None

        to_exclude = []
        for toid, building in by_toid.items():
            reason = building.exclusion_reason()
            if reason:
                to_exclude.append((toid, reason))

        _write_exclusions(pg_conn, job_id, to_exclude)
    finally:
        pg_conn.close()


def _write_exclusions(pg_conn, job_id: int, to_exclude: List[Tuple[str, str]], chunk_size: int = 1000):
    with pg_conn.cursor() as cursor:
        for i in range(0, len(to_exclude), chunk_size):
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
                ), argslist=to_exclude[i:i + chunk_size])
            pg_conn.commit()


def _load_building_pixels(pg_conn, job_id: int):
    with pg_conn.cursor() as cursor:
        cursor.execute(SQL("""            
            SELECT 
                h.pixel_id,
                h.elevation,
                b.toid,
                ST_Contains(b.geom_27700, h.en) AS within_building,
                NOT ST_Contains((SELECT geom_27700 FROM {all_buildings}), h.en) AS without_building,
                hh.height
            FROM {buildings} b 
            LEFT JOIN mastermap.height hh ON b.toid = hh.toid
            LEFT JOIN {pixel_horizons} h ON ST_Contains(ST_Buffer(b.geom_27700, 1), h.en)
            WHERE h.elevation != -9999
            ORDER BY b.toid;
            """).format(
            all_buildings=Identifier(tables.schema(job_id), tables.ALL_BUILDINGS_TABLE),
            pixel_horizons=Identifier(tables.schema(job_id), tables.PIXEL_HORIZON_TABLE),
            buildings=Identifier(tables.schema(job_id), tables.BUILDINGS_TABLE),
        ))
        pg_conn.commit()
        return cursor.fetchall()
