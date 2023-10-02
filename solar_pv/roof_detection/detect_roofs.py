import json

# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import logging
from contextlib import contextmanager

import time
import math
import traceback
from typing import List, Dict
import multiprocessing as mp

from psycopg2.extras import DictCursor, execute_values, Json
from psycopg2.sql import SQL, Identifier
import numpy as np
from shapely import wkt

from solar_pv.db_funcs import count, sql_command, connect, connection
from solar_pv.postgis import pixels_for_buildings
from solar_pv import tables
from solar_pv.constants import RANSAC_LARGE_BUILDING, \
    RANSAC_LARGE_MAX_TRIALS, RANSAC_SMALL_MAX_TRIALS, RANSAC_SMALL_BUILDING, \
    RANSAC_MEDIUM_MAX_TRIALS, ROOFDET_MAX_MAE
from solar_pv.roof_detection.detect_messy_roofs import detect_messy_roofs
from solar_pv.roof_detection.detsac import DETSACRegressorForLIDAR
from solar_pv.roof_detection.merge_adjacent import merge_adjacent
from solar_pv.roof_detection.premade_planes import create_planes
from solar_pv.roof_detection.ransac import RANSACRegressorForLIDAR
from solar_pv.roof_polygons.roof_polygons import create_roof_polygons
from solar_pv.datatypes import RoofDetBuilding, RoofPlane, RoofPolygon
from solar_pv.util import get_cpu_count

_SEMAPHORE_TIMEOUT_S = 120
_SEMAPHORE_MAX_CONN = 50
_PG_CONN_SEMAPHORE = mp.Semaphore(_SEMAPHORE_MAX_CONN)


def _roof_det_cpu_count():
    """Use 3/4s of available CPUs for roof plane detection"""
    return int(get_cpu_count() * 0.75)


def detect_roofs(pg_uri: str,
                 job_id: int,
                 max_roof_slope_degrees: int,
                 min_roof_area_m: int,
                 min_roof_degrees_from_north: int,
                 flat_roof_degrees: int,
                 min_dist_to_edge_m: float,
                 resolution_metres: float,
                 workers: int = _roof_det_cpu_count(),
                 building_page_size: int = 50) -> None:

    if count(pg_uri, tables.schema(job_id), tables.ROOF_POLYGON_TABLE) > 0:
        logging.info("Not detecting roof planes, already detected.")
        return

    building_count = _building_count(pg_uri, job_id)
    segments = math.ceil(building_count / building_page_size)
    workers = min(segments, workers)
    logging.info(f"{building_count} buildings, in {segments} batches to process")
    logging.info(f"Using {workers} processes for roof plane detection")
    start_time = time.time()

    params = {
        "max_roof_slope_degrees": max_roof_slope_degrees,
        "min_roof_area_m": min_roof_area_m,
        "min_roof_degrees_from_north": min_roof_degrees_from_north,
        "flat_roof_degrees": flat_roof_degrees,
        "min_dist_to_edge_m": min_dist_to_edge_m,
        "resolution_metres": resolution_metres,
    }

    with mp.Pool(workers) as pool:
        wrapped_iterable = ((pg_uri, job_id, seg, building_page_size, params)
                            for seg in range(0, segments))
        res = pool.starmap_async(_handle_building_page, wrapped_iterable, chunksize=1)
        # Hacky way to poll for failures in workers, doesn't seem to be a nicer way
        # of doing this:
        while not res.ready():
            if not res._success:
                pool.terminate()
                pool.join()
                raise ValueError('Cancelling roof plane detection due to failure in worker')
            time.sleep(1)

    _mark_buildings_with_no_planes(pg_uri, job_id)
    logging.info(f"roof plane detection for {building_count} roofs took {round(time.time() - start_time, 2)} s.")


def _handle_building_page(pg_uri: str, job_id: int, page: int, page_size: int, params: dict):
    start_time = time.time()
    buildings = _load(pg_uri, job_id, page, page_size)

    polygons = []
    for toid, building in buildings.items():
        try:
            t0 = time.time()
            found = _detect_building_roof_planes(building, toid, params['resolution_metres'])
            t1 = time.time()
            if t1 - t0 > 1200:
                print(f"very slow plane detection: {toid} took {round(t1 - t0, 2)} s")
                # _print_test_data(building)
        except Exception as e:
            print(f"Exception during roof plane detection for TOID {toid}:")
            traceback.print_exception(e)
            _print_test_data(building)
            raise e

        if len(found) > 0:
            polygons.extend(create_roof_polygons(toid, building['polygon'], found, **params))

    try:
        _save_planes(pg_uri, job_id, polygons)
    except Exception as e:
        print(f"Exception when saving roof planes:")
        traceback.print_exception(e)
        raise e

    page_time = round(time.time() - start_time, 2)
    if page_time > 120:
        print(f"slow page: {page} of {page_size} took {page_time} s.")


def _detect_building_roof_planes(building: RoofDetBuilding,
                                 toid: str,
                                 resolution_metres: float,
                                 debug: bool = False) -> List[RoofPlane]:
    pixels_in_building = building['pixels']
    polygon = building['polygon']
    max_ground_height = building['max_ground_height']

    xyz = np.array([[pixel["x"], pixel["y"], pixel["elevation"]] for pixel in pixels_in_building])
    aspect = np.array([pixel["aspect"] for pixel in pixels_in_building])
    slope = np.array([pixel["slope"] for pixel in pixels_in_building])
    z = xyz[:, 2]
    mask = z > max_ground_height if max_ground_height else np.ones(aspect.shape)

    min_points_per_plane = min(8, int(8 / resolution_metres))  # 8 for 2m, 8 for 1m, 16 for 0.5m
    total_points_in_building = len(aspect)
    premade_planes = create_planes(xyz, aspect, slope, resolution_metres)
    skip_planes = set()
    xy = xyz[:, :2]
    z = xyz[:, 2]

    labels_nodata = -1
    labels = np.full(z.shape, labels_nodata, dtype=int)
    planes: Dict[int, RoofPlane] = {}

    plane_idx = 0
    while np.count_nonzero(mask) > min_points_per_plane:
        detsac = DETSACRegressorForLIDAR(residual_threshold=0.25,
                                         flat_roof_residual_threshold=0.1,
                                         max_slope=75,
                                         min_slope=0,
                                         min_points_per_plane=min_points_per_plane,
                                         resolution_metres=resolution_metres)
        detsac.fit(xy, z,
                   aspect=aspect,
                   mask=mask,
                   polygon=polygon,
                   premade_planes=premade_planes,
                   skip_planes=skip_planes,
                   total_points_in_building=total_points_in_building,
                   debug=debug)

        if detsac.finished:
            break

        if detsac.success:
            inlier_mask = detsac.inlier_mask_

            # don't keep bad planes - their inliers become candidates for being
            # merged into other planes (and the planes will still have been added to
            # skip_planes, so we don't retry them)
            if detsac.plane_properties["score"] < ROOFDET_MAX_MAE:
                planes[plane_idx] = detsac.plane_properties
                planes[plane_idx]["toid"] = toid
                labels[inlier_mask] = plane_idx
                plane_idx += 1
                mask[inlier_mask] = 0

    total_pixels = len(aspect)
    if total_pixels > RANSAC_LARGE_BUILDING / resolution_metres:
        max_trials = RANSAC_LARGE_MAX_TRIALS
    elif total_pixels < RANSAC_SMALL_BUILDING / resolution_metres:
        max_trials = RANSAC_SMALL_MAX_TRIALS
    else:
        max_trials = RANSAC_MEDIUM_MAX_TRIALS

    # We only fall back to RANSAC if a decent fraction of pixels are left to find:
    pixels_required_for_ransac = max(min_points_per_plane * 5, total_points_in_building // 4)

    while np.count_nonzero(mask) > pixels_required_for_ransac:
        ransac = RANSACRegressorForLIDAR(residual_threshold=0.25,
                                         flat_roof_residual_threshold=0.1,
                                         max_trials=max_trials,
                                         max_slope=75,
                                         min_slope=0,
                                         min_points_per_plane=min_points_per_plane,
                                         resolution_metres=resolution_metres)
        ransac.fit(xy, z,
                   aspect=aspect,
                   mask=mask,
                   polygon=polygon,
                   skip_planes=skip_planes,
                   total_points_in_building=total_points_in_building,
                   debug=debug)

        if ransac.finished:
            break

        if ransac.success:
            inlier_mask = ransac.inlier_mask_

            # don't keep bad planes - their inliers become candidates for being
            # merged into other planes (and the planes will still have been added to
            # skip_planes, so we don't retry them)
            if ransac.plane_properties["score"] < ROOFDET_MAX_MAE:
                planes[plane_idx] = ransac.plane_properties
                planes[plane_idx]["toid"] = toid
                labels[inlier_mask] = plane_idx
                plane_idx += 1
                mask[inlier_mask] = 0

    if len(planes) == 0:
        return []

    if debug:
        print("Finished detecting roofs")

    # label all the outlying pixels with individual IDs:
    outliers = np.count_nonzero(labels[mask == 1])
    labels[mask == 1] = range(plane_idx + 1, outliers + plane_idx + 1)

    merged_planes, new_labels = merge_adjacent(xy, z, labels, planes, resolution_metres, labels_nodata, debug=debug)

    if debug:
        print("Merged planes and outliers")

    non_messy_planes = detect_messy_roofs(merged_planes, new_labels, xy, resolution_metres, debug=debug)

    if debug:
        print("Finished detecting mess")

    return non_messy_planes


def _load(pg_uri: str,
          job_id: int,
          page: int,
          page_size: int,
          toids: List[str] = None,
          force_load: bool = False) -> Dict[str, RoofDetBuilding]:
    """
    Load LIDAR pixel data for roof plane detection. page_size is number of
    buildings rather than pixels to prevent splitting a building's pixels across
    pages.
    """
    with semaphore_connection(pg_uri, cursor_factory=DictCursor) as pg_conn:
        elevation_table = f"{tables.schema(job_id)}.{tables.ELEVATION}"
        aspect_table = f"{tables.schema(job_id)}.{tables.ASPECT}"
        slope_table = f"{tables.schema(job_id)}.{tables.SLOPE}"
        by_toid = pixels_for_buildings(pg_conn, job_id, page, page_size, [elevation_table, aspect_table, slope_table], toids, force_load=force_load)
        buildings = _load_building_polygons(pg_conn, job_id, list(by_toid.keys()))
        loaded = {}
        for building in buildings:
            toid = building["toid"]
            pixels = by_toid[toid]
            loaded[toid] = {}
            loaded[toid]["toid"] = toid
            loaded[toid]["polygon"] = wkt.loads(building["polygon"])
            loaded[toid]["pixels"] = pixels
            loaded[toid]["min_ground_height"] = building["min_ground_height"]
            loaded[toid]["max_ground_height"] = building["max_ground_height"]
        return loaded


def _load_building_polygons(pg_conn, job_id, toids: List[str]) -> List[dict]:
    buildings = sql_command(
            pg_conn,
            """
            SELECT toid, ST_AsText(geom_27700) AS polygon, min_ground_height, max_ground_height
            FROM {buildings}
            WHERE toid = ANY( %(toids)s )""",
            {"toids": toids},
            buildings=Identifier(tables.schema(job_id), tables.BUILDINGS_TABLE),
            result_extractor=lambda rows: rows)

    return buildings


def _building_count(pg_uri: str, job_id: int):
    with connection(pg_uri, cursor_factory=DictCursor) as pg_conn:
        return sql_command(
            pg_conn,
            "SELECT COUNT(*) FROM {buildings};",
            buildings=Identifier(tables.schema(job_id), tables.BUILDINGS_TABLE),
            result_extractor=lambda rows: rows[0][0])


def _save_planes(pg_uri: str, job_id: int, planes: List[RoofPolygon]):
    if len(planes) == 0:
        return

    for plane in planes:
        # TODO maybe don't have to save inliers_xy as it's only needed for dev_roof_polygons?
        plane['inliers_xy'] = plane['inliers_xy'].tolist()
        plane['meta'] = Json({
            "sd": plane["sd"],
            "score": plane["score"],
            "aspect_circ_mean": plane["aspect_circ_mean"],
            "aspect_circ_sd": plane["aspect_circ_sd"],
            "thinness_ratio": plane["thinness_ratio"],
            "cv_hull_ratio": plane["cv_hull_ratio"],
            "plane_type": plane["plane_type"],
            "aspect_raw": plane["aspect_raw"],
            "r2": plane["r2"],
            "mae": plane["mae"],
            "mse": plane["mse"],
            "rmse": plane["rmse"],
            "msle": plane["msle"],
            "mape": plane["mape"],
        })
        plane['roof_geom_27700'] = plane['roof_geom_27700'].wkt
        plane['roof_geom_raw_27700'] = plane['roof_geom_raw_27700'].wkt

    with semaphore_connection(pg_uri) as pg_conn, pg_conn.cursor() as cursor:
        execute_values(cursor, SQL("""
            INSERT INTO {roof_polygons} (
                toid, 
                roof_geom_27700, 
                roof_geom_raw_27700, 
                x_coef, 
                y_coef, 
                intercept, 
                slope, 
                aspect,
                is_flat, 
                usable, 
                inliers_xy,
                meta
            ) VALUES %s;
        """).format(
            roof_polygons=Identifier(tables.schema(job_id), tables.ROOF_POLYGON_TABLE),
        ), argslist=planes,
           template="""(%(toid)s, 
                        %(roof_geom_27700)s, 
                        %(roof_geom_raw_27700)s, 
                        %(x_coef)s,
                        %(y_coef)s, 
                        %(intercept)s, 
                        %(slope)s, 
                        %(aspect)s, 
                        %(is_flat)s, 
                        %(usable)s, 
                        %(inliers_xy)s,
                        %(meta)s )""")

        pg_conn.commit()


def _mark_buildings_with_no_planes(pg_uri: str, job_id: int):
    with connection(pg_uri) as pg_conn:
        sql_command(
            pg_conn,
            """
            UPDATE {buildings} b
            SET exclusion_reason = 'NO_ROOF_PLANES_DETECTED'
            WHERE
                NOT EXISTS (SELECT FROM {roof_polygons} rp WHERE rp.toid = b.toid)
                AND b.exclusion_reason IS NULL
            """,
            roof_polygons=Identifier(tables.schema(job_id), tables.ROOF_POLYGON_TABLE),
            buildings=Identifier(tables.schema(job_id), tables.BUILDINGS_TABLE))


def _print_test_data(building: RoofDetBuilding):
    """Print data for building in the format that the RANSAC tests expect"""
    print(json.dumps(building, default=str))


@contextmanager
def semaphore_connection(pg_uri: str, **kwargs):
    _PG_CONN_SEMAPHORE.acquire(timeout=_SEMAPHORE_TIMEOUT_S)
    try:
        pg_conn = connect(pg_uri, **kwargs)
        try:
            yield pg_conn
        finally:
            pg_conn.close()
    finally:
        _PG_CONN_SEMAPHORE.release()
