from os.path import join
from typing import List

from psycopg2.extras import DictCursor
from psycopg2.sql import Identifier

from albion_models import paths
from albion_models.db_funcs import sql_command, connection
from albion_models.lidar.lidar import LIDAR_NODATA
from albion_models.solar_pv import tables
from albion_models.solar_pv.outdated_lidar.outdated_lidar_check import HeightAggregator


def check_toids_lidar(pg_uri: str, job_id: int, toids: List[str], write_test_data: bool = True):
    for toid in toids:
        check_toid_lidar(pg_uri, job_id, toid, write_test_data)


def check_toid_lidar(pg_uri: str, job_id: int, toid: str, write_test_data: bool):
    ha = HeightAggregator(debug=True)
    with connection(pg_uri, cursor_factory=DictCursor) as pg_conn:
        pixels = _load_building_pixels(pg_conn, job_id, toid)
        for pixel in pixels:
            ha.aggregate_row(pixel)

    reason = ha.exclusion_reason()
    if reason:
        print(f"toid {toid} excluded. Reason {reason}\n")
    else:
        print(f"toid {toid} not excluded.\n")
    if write_test_data:
        _write_test_data(toid, pixels)


def _write_test_data(toid, building):
    """
    Write out a test data CSV that can be used for unit tests.
    See test_oudated_lidar_check.py
    """
    ransac_test_data_dir = join(paths.TEST_DATA, "outdated_lidar")
    csv = join(ransac_test_data_dir, f"{toid}.csv")
    with open(csv, 'w') as f:
        f.write(
            "pixel_id,elevation,toid,within_building,without_building,height,base_roof_height\n")
        for p in building:
            f.write(
                f"{p['pixel_id']},{p['elevation']},{p['toid']},{p['within_building']},"
                f"{p['without_building']},{p['height']},{p['base_roof_height']}\n")


def _load_building_pixels(pg_conn, job_id: int, toid: str):
    return sql_command(
        pg_conn,
        """
        SELECT
            h.pixel_id,
            h.elevation,
            b.toid,
            ST_Contains(b.geom_27700, h.en) AS within_building,
            h.toid IS NULL AS without_building,
            hh.height,
            hh.rel_h2 AS base_roof_height
            -- hh.rel_hmax AS max_roof_height,
            -- hh.abs_hmin AS ground_height
        FROM {buildings} b
        LEFT JOIN mastermap.height hh ON b.toid = hh.toid
        LEFT JOIN {lidar_pixels} h 
            -- ON ST_Contains(ST_Buffer(ST_ExteriorRing(b.geom_27700), 5), h.en)
            ON ST_Contains(ST_Buffer(b.geom_27700, 5), h.en)
        WHERE h.elevation != %(lidar_nodata)s
        AND b.toid = %(toid)s
        """,
        {
            "toid": toid,
            "lidar_nodata": LIDAR_NODATA,
        },
        lidar_pixels=Identifier(tables.schema(job_id), tables.LIDAR_PIXEL_TABLE),
        buildings=Identifier(tables.schema(job_id), tables.BUILDINGS_TABLE),
        result_extractor=lambda rows: rows)


if __name__ == "__main__":
    check_toids_lidar(
        "postgresql://albion_webapp:ydBbE3JCnJ4@localhost:5432/albion?application_name=blah",
        1617,
        [
            "osgb5000005134753286",
            "osgb5000005134753280",
            "osgb5000005134753270",
            "osgb5000005134753276",
            "osgb5000005152026792",
            "osgb5000005152026801",
            "osgb5000005235848378",
            "osgb5000005134753282",
            "osgb5000005135275129",
            "osgb1000020005762",
            # should be allowed:
            "osgb1000019929148",
            "osgb1000043085584",
            "osgb1000019927618",
            "osgb1000020002707",
            "osgb1000020002198",  # Not working - should keep
            "osgb1000043085181",  # Not working - should keep
            "osgb1000020002780",
        ],
        write_test_data=True,
    )
