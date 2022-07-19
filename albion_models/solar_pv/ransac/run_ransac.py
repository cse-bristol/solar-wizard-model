import logging
import os
import time
from collections import defaultdict

import math
from typing import List
import multiprocessing as mp

from albion_models.db_funcs import connect, count
from albion_models.solar_pv import tables

import psycopg2.extras
from psycopg2.sql import SQL, Identifier
import numpy as np

from albion_models.solar_pv.ransac.ransac import RANSACRegressorForLIDAR, _aspect, _slope


def _get_cpu_count():
    """Use 3/4s of available CPUs for RANSAC plane detection"""
    return int(len(os.sched_getaffinity(0)) * 0.75)


def run_ransac(pg_uri: str,
               job_id: int,
               resolution_metres: float,
               workers: int = _get_cpu_count(),
               building_page_size: int = 10) -> None:

    if count(pg_uri, tables.schema(job_id), tables.ROOF_PLANE_TABLE) > 0:
        logging.info("Not detecting roof planes, already detected.")
        return

    building_count = _building_count(pg_uri, job_id)
    segments = math.ceil(building_count / building_page_size)
    logging.info(f"{building_count} buildings, in {segments} batches to process")
    start_time = time.time()

    with mp.Pool(workers) as pool:
        wrapped_iterable = ((pg_uri, job_id, seg, building_page_size, resolution_metres)
                            for seg in range(0, segments))
        for res in pool.starmap(_handle_building_page, wrapped_iterable):
            pass

    _mark_buildings_with_no_planes(pg_uri, job_id)
    logging.info(f"RANSAC for {building_count} roofs took {round(time.time() - start_time, 2)} s.")


def _handle_building_page(pg_uri: str, job_id: int, page: int, page_size: int, resolution_metres: float):
    rows = _load(pg_uri, job_id, page, page_size)
    by_toid = defaultdict(list)
    for row in rows:
        by_toid[row['toid']].append(row)

    planes = []
    for toid, building in by_toid.items():
        found = _ransac_building(building, toid, resolution_metres)
        if len(found) > 0:
            planes.extend(found)
        elif len(building) > 1000:
            # Retry with relaxed constraints around group checks and with a higher
            # `max_trials` for larger buildings where we care more:
            found = _ransac_building(building, toid, resolution_metres, max_trials=3000, include_group_checks=False)
            planes.extend(found)

    _save_planes(pg_uri, job_id, planes)
    print(f"Page {page} of buildings complete")


def _ransac_building(pixels_in_building: List[dict],
                     toid: str,
                     resolution_metres: float,
                     max_trials: int = 1000,
                     include_group_checks: bool = True) -> List[dict]:
    xyz = np.array([[pixel["easting"], pixel["northing"], pixel["elevation"]] for pixel in pixels_in_building])
    aspect = np.array([pixel["aspect"] for pixel in pixels_in_building])
    pixel_ids = np.array([pixel["pixel_id"] for pixel in pixels_in_building])

    planes = []
    min_points_per_plane = 8
    total_points_in_building = len(aspect)

    while np.count_nonzero(xyz) // 3 > min_points_per_plane:
        XY = xyz[:, :2]
        Z = xyz[:, 2]
        try:
            ransac = RANSACRegressorForLIDAR(residual_threshold=0.25,
                                             max_trials=max_trials,
                                             max_slope=75,
                                             min_slope=0,
                                             min_points_per_plane=min_points_per_plane,
                                             resolution_metres=resolution_metres)
            ransac.fit(XY, Z,
                       aspect=aspect,
                       total_points_in_building=total_points_in_building,
                       include_group_checks=include_group_checks)
            inlier_mask = ransac.inlier_mask_
            outlier_mask = np.logical_not(inlier_mask)
            a, b = ransac.estimator_.coef_
            d = ransac.estimator_.intercept_

            planes.append({
                "toid": toid,
                "x_coef": a,
                "y_coef": b,
                "intercept": d,
                "slope": _slope(a, b),
                "aspect": _aspect(a, b),
                "inliers": pixel_ids[inlier_mask],
                "sd": ransac.sd,
            })

            xyz = xyz[outlier_mask]
            aspect = aspect[outlier_mask]
            pixel_ids = pixel_ids[outlier_mask]
        except ValueError:
            break

    return planes


def _load(pg_uri: str, job_id: int, page: int, page_size: int):
    """
    Load LIDAR pixel data for RANSAC processing. page_size is number of
    buildings rather than pixels to prevent splitting a building's pixels across
    pages.
    """
    pg_conn = connect(pg_uri, cursor_factory=psycopg2.extras.DictCursor)
    try:
        with pg_conn.cursor() as cursor:
            cursor.execute(SQL("""
                WITH building_page AS (
                    SELECT b.toid, b.geom_27700
                    FROM
                        {buildings} b
                        LEFT JOIN {building_exclusion_reasons} ber ON b.toid = ber.toid
                    WHERE ber.exclusion_reason IS NULL
                    ORDER BY b.toid
                    OFFSET %(offset)s LIMIT %(limit)s
                )
                SELECT h.pixel_id, h.easting, h.northing, h.elevation, h.aspect, b.toid
                FROM building_page b
                LEFT JOIN {lidar_pixels} h ON h.toid = b.toid
                WHERE h.elevation != -9999
                ORDER BY b.toid;
                """).format(
                lidar_pixels=Identifier(tables.schema(job_id), tables.LIDAR_PIXEL_TABLE),
                buildings=Identifier(tables.schema(job_id), tables.BUILDINGS_TABLE),
                building_exclusion_reasons=Identifier(tables.schema(job_id), tables.BUILDING_EXCLUSION_REASONS_TABLE)
            ), {
                "offset": page * page_size,
                "limit": page_size,
            })
            pg_conn.commit()
            return cursor.fetchall()
    finally:
        pg_conn.close()


def _building_count(pg_uri: str, job_id: int):
    pg_conn = connect(pg_uri, cursor_factory=psycopg2.extras.DictCursor)
    try:
        with pg_conn.cursor() as cursor:
            cursor.execute(SQL("SELECT COUNT(*) FROM {buildings};").format(
                buildings=Identifier(tables.schema(job_id), tables.BUILDINGS_TABLE),
            ))
            pg_conn.commit()
            return cursor.fetchone()[0]
    finally:
        pg_conn.close()


def _save_planes(pg_uri: str, job_id: int, planes: List[dict]):
    if len(planes) == 0:
        return

    pg_conn = connect(pg_uri)
    plane_inliers = []
    for plane in planes:
        plane_inliers.append(plane['inliers'])
        del plane['inliers']

    try:
        with pg_conn.cursor() as cursor:
            plane_ids = psycopg2.extras.execute_values(cursor, SQL("""
                INSERT INTO {roof_planes} (toid, x_coef, y_coef, intercept, slope, aspect, sd)
                VALUES %s
                RETURNING roof_plane_id;
            """).format(
                roof_planes=Identifier(tables.schema(job_id), tables.ROOF_PLANE_TABLE),
            ), argslist=planes, fetch=True,
                template="(%(toid)s, %(x_coef)s, %(y_coef)s, %(intercept)s, %(slope)s, %(aspect)s, %(sd)s)")

            pixel_plane_data = []
            for i in range(0, len(plane_ids)):
                plane_id = plane_ids[i][0]
                for pixel_id in plane_inliers[i]:
                    pixel_plane_data.append((int(pixel_id), plane_id))

            psycopg2.extras.execute_values(cursor, SQL("""
                UPDATE {pixel_horizons}
                SET roof_plane_id = data.roof_plane_id
                FROM (VALUES %s) AS data (pixel_id, roof_plane_id)
                WHERE {pixel_horizons}.pixel_id = data.pixel_id;
            """).format(
                pixel_horizons=Identifier(tables.schema(job_id), tables.LIDAR_PIXEL_TABLE),
            ), argslist=pixel_plane_data)
            pg_conn.commit()
    finally:
        pg_conn.close()


def _mark_buildings_with_no_planes(pg_uri: str, job_id: int):
    pg_conn = connect(pg_uri)
    try:
        with pg_conn.cursor() as cursor:
            # TODO this is also picking up things with no LiDAR coverage - need
            #  to change it so that it checks for at least one pixel whose value is not -9999.
            #  Update:
            #  I think this is resolved now (as previously nodata pixels were not being loaded
            #  in to the db - now they are), due to not using SAGA any more. Need to test!
            cursor.execute(SQL("""
                UPDATE {building_exclusion_reasons} ber
                SET exclusion_reason = 'NO_ROOF_PLANES_DETECTED'
                WHERE
                    NOT EXISTS (SELECT FROM {roof_planes} rp WHERE rp.toid = ber.toid)
                    AND ber.exclusion_reason IS NULL
            """).format(
                roof_planes=Identifier(tables.schema(job_id), tables.ROOF_PLANE_TABLE),
                building_exclusion_reasons=Identifier(tables.schema(job_id),
                                                      tables.BUILDING_EXCLUSION_REASONS_TABLE),
            ))
            pg_conn.commit()
    finally:
        pg_conn.close()

# if __name__ == '__main__':
#     _handle_building_page(
#         "postgresql://albion_webapp:ydBbE3JCnJ4@localhost:5432/albion",
#         31,
#         page_size=10,
#         page=10,
#         resolution_metres=0.5)
