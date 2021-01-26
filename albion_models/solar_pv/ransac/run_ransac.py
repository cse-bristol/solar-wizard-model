import logging
import os
import time
from collections import defaultdict

import math
from typing import List
import multiprocessing as mp

from albion_models.db_funcs import connect
from albion_models.solar_pv import tables

import psycopg2.extras
from psycopg2.sql import SQL, Identifier
import numpy as np

from albion_models.solar_pv.ransac.ransac import RANSACRegressorForLIDAR, _aspect, _slope


def _get_cpu_count():
    """Use 3/4s of available CPUs for RANSAC plane detection"""
    return int(len(os.sched_getaffinity(0)) * 0.75)


def run_ransac(pg_uri: str, job_id: int,
               workers: int = _get_cpu_count(),
               building_page_size: int = 10) -> None:
    building_count = _building_count(pg_uri, job_id)
    segments = math.ceil(building_count / building_page_size)
    logging.info(f"{building_count} buildings, in {segments} batches to process")
    start_time = time.time()

    with mp.Pool(workers) as pool:
        wrapped_iterable = ((pg_uri, job_id, seg, building_page_size)
                            for seg in range(0, segments))
        for res in pool.starmap(_handle_building_page, wrapped_iterable):
            pass

    logging.info(f"RANSAC for {building_count} roofs took {round(time.time() - start_time, 2)} s.")


def _handle_building_page(pg_uri: str, job_id: int, page: int, page_size: int):
    rows = _load(pg_uri, job_id, page, page_size)
    by_toid = defaultdict(list)
    for row in rows:
        by_toid[row['toid']].append(row)

    planes = []
    for toid, building in by_toid.items():
        planes.extend(_ransac_building(building, toid))

    _save_planes(pg_uri, job_id, planes)
    print(f"Page {page} of buildings complete")


def _ransac_building(pixels_in_building: List[dict], toid: str) -> List[dict]:
    xyz = np.array([[pixel["easting"], pixel["northing"], pixel["elevation"]] for pixel in pixels_in_building])
    aspect = np.array([pixel["aspect"] for pixel in pixels_in_building])
    pixel_ids = np.array([pixel["pixel_id"] for pixel in pixels_in_building])

    planes = []
    min_points_per_plane = 8

    while np.count_nonzero(xyz) // 3 > min_points_per_plane:
        XY = xyz[:, :2]
        Z = xyz[:, 2]
        try:
            ransac = RANSACRegressorForLIDAR(residual_threshold=0.25,
                                             max_trials=1000,
                                             max_slope=75,
                                             min_slope=0,
                                             min_points_per_plane=min_points_per_plane)
            ransac.fit(XY, Z, aspect=aspect)
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
                    SELECT b.toid, b.geom_27700 FROM {buildings} b ORDER BY b.toid
                    OFFSET %(offset)s LIMIT %(limit)s
                )
                SELECT h.pixel_id, h.easting, h.northing, h.elevation, h.aspect, b.toid 
                FROM building_page b 
                LEFT JOIN {pixel_horizons} h
                ON ST_Contains(b.geom_27700, h.en)
                ORDER BY b.toid;
                """).format(
                pixel_horizons=Identifier(tables.schema(job_id), tables.PIXEL_HORIZON_TABLE),
                buildings=Identifier(tables.schema(job_id), tables.BUILDINGS_TABLE),
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
                INSERT INTO {roof_planes} (toid, x_coef, y_coef, intercept, slope, aspect)
                VALUES %s
                RETURNING roof_plane_id;
            """).format(
                roof_planes=Identifier(tables.schema(job_id), tables.ROOF_PLANE_TABLE),
            ), argslist=planes, fetch=True,
               template="(%(toid)s, %(x_coef)s, %(y_coef)s, %(intercept)s, %(slope)s, %(aspect)s)")

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
                pixel_horizons=Identifier(tables.schema(job_id), tables.PIXEL_HORIZON_TABLE),
            ), argslist=pixel_plane_data)
            pg_conn.commit()
    finally:
        pg_conn.close()
