from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, Any, Tuple

from psycopg2.sql import SQL, Identifier

from albion_models.solar_pv import tables


@dataclass
class HeightAggregator:
    pixels_within: int
    pixels_without: int
    within_elevation_sum: float
    without_elevation_sum: float

    def aggregate_row(self, row: Dict[str, Any]):
        if row['within_building']:
            self.pixels_within += 1
            self.within_elevation_sum += row['elevation']
        else:
            self.pixels_without += 1
            self.without_elevation_sum += row['elevation']

    def average_heights(self) -> Tuple[float, float]:
        return (self.within_elevation_sum / self.pixels_within,
                self.without_elevation_sum / self.pixels_without)


def check_for_bad_coverage(pg_conn, job_id: int):
    """
    Check for discrepancies between OS MasterMap building polygon data and
    LiDAR data.

    Currently LiDAR data is mostly from 2017 so is starting to be
    out-of-date for newly built things. In these cases, if unhandled, the LiDAR
    detects all buildings as flat (or like the ground that they were built on was) -
    or occasionally with a now-nonexistent building intersecting the polygon weirdly.

    :return:
    """
    rows = _load_building_pixels(pg_conn, job_id)
    by_toid = defaultdict(HeightAggregator)
    for row in rows:
        by_toid[row['toid']].aggregate_row(row)
    rows = None

    for toid, building in by_toid.items():
        avg_height_within, avg_height_without = building.average_heights()



def _load_building_pixels(pg_conn, job_id: int):
    with pg_conn.cursor() as cursor:
        cursor.execute(SQL("""
            SELECT 
                h.pixel_id,
                h.elevation,
                b.toid,
                ST_Contains(b.geom_27700, h.en) AS within_building 
            FROM {buildings} b 
            LEFT JOIN {pixel_horizons} h
            ON ST_Contains(ST_Buffer(b.geom_27700, 1), h.en)
            WHERE h.elevation != -9999
            ORDER BY b.toid;
            """).format(
            pixel_horizons=Identifier(tables.schema(job_id), tables.PIXEL_HORIZON_TABLE),
            buildings=Identifier(tables.schema(job_id), tables.BUILDINGS_TABLE),
        ))
        pg_conn.commit()
        return cursor.fetchall()
