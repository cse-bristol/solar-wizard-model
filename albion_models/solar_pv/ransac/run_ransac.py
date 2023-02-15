import logging
import time
from collections import defaultdict

import math
from typing import List, Dict
import multiprocessing as mp

from albion_models.db_funcs import count, connection, sql_command
from albion_models.lidar.lidar import LIDAR_NODATA
from albion_models.postgis import pixels_for_buildings
from albion_models.solar_pv import tables

from psycopg2.extras import DictCursor, execute_values
from psycopg2.sql import SQL, Identifier, Literal
import numpy as np

from albion_models.solar_pv.constants import RANSAC_LARGE_BUILDING, \
    RANSAC_BASE_MAX_TRIALS, RANSAC_ABS_MAX_TRIALS
from albion_models.solar_pv.ransac.ransac import RANSACRegressorForLIDAR, _aspect, \
    _slope, RANSACValueError
from albion_models.solar_pv.roof_polygons.roof_polygons import create_roof_polygons
from albion_models.util import get_cpu_count


def _ransac_cpu_count():
    """Use 3/4s of available CPUs for RANSAC plane detection"""
    return int(get_cpu_count() * 0.75)


def run_ransac(pg_uri: str,
               job_id: int,
               max_roof_slope_degrees: int,
               min_roof_area_m: int,
               min_roof_degrees_from_north: int,
               flat_roof_degrees: int,
               large_building_threshold: float,
               min_dist_to_edge_m: float,
               min_dist_to_edge_large_m: float,
               resolution_metres: float,
               panel_width_m: float,
               panel_height_m: float,
               workers: int = _ransac_cpu_count(),
               building_page_size: int = 50) -> None:

    if count(pg_uri, tables.schema(job_id), tables.ROOF_POLYGON_TABLE) > 0:
        logging.info("Not detecting roof planes, already detected.")
        return

    building_count = _building_count(pg_uri, job_id)
    segments = math.ceil(building_count / building_page_size)
    workers = min(segments, workers)
    logging.info(f"{building_count} buildings, in {segments} batches to process")
    logging.info(f"Using {workers} processes for RANSAC")
    start_time = time.time()

    params = {
        "max_roof_slope_degrees": max_roof_slope_degrees,
        "min_roof_area_m": min_roof_area_m,
        "min_roof_degrees_from_north": min_roof_degrees_from_north,
        "flat_roof_degrees": flat_roof_degrees,
        "large_building_threshold": large_building_threshold,
        "min_dist_to_edge_m": min_dist_to_edge_m,
        "min_dist_to_edge_large_m": min_dist_to_edge_large_m,
        "resolution_metres": resolution_metres,
        "panel_width_m": panel_width_m,
        "panel_height_m": panel_height_m,
    }
    with mp.Pool(workers) as pool:
        wrapped_iterable = ((pg_uri, job_id, seg, building_page_size, params)
                            for seg in range(0, segments))
        for res in pool.starmap(_handle_building_page, wrapped_iterable):
            pass

    _mark_buildings_with_no_planes(pg_uri, job_id)
    logging.info(f"RANSAC for {building_count} roofs took {round(time.time() - start_time, 2)} s.")


def _handle_building_page(pg_uri: str, job_id: int, page: int, page_size: int, params: dict):
    start_time = time.time()
    by_toid = _load(pg_uri, job_id, page, page_size)

    planes = []
    for toid, building in by_toid.items():
        found = _ransac_building(building, toid, params['resolution_metres'])
        if len(found) > 0:
            planes.extend(found)

    planes = create_roof_polygons(pg_uri, job_id, planes, **params)
    _save_planes(pg_uri, job_id, planes)
    print(f"Page {page} of {page_size} buildings complete, took {round(time.time() - start_time, 2)} s.")


def _ransac_building(pixels_in_building: List[dict],
                     toid: str,
                     resolution_metres: float,
                     debug: bool = False) -> List[dict]:
    xyz = np.array([[pixel["x"], pixel["y"], pixel["elevation"]] for pixel in pixels_in_building])
    aspect = np.array([pixel["aspect"] for pixel in pixels_in_building])

    if len(pixels_in_building) > RANSAC_LARGE_BUILDING / resolution_metres:
        max_trials = min(RANSAC_BASE_MAX_TRIALS + len(pixels_in_building) / resolution_metres,
                         RANSAC_ABS_MAX_TRIALS)
        # Disables checks that forbid planes that cover multiple discontinuous groups
        # of pixels, as large buildings often have separate roof areas that are on the
        # same plane. Only the largest group will be used each time anyway, so this
        # won't cause problems and all discontinuous groups should be picked up
        # eventually.
        include_group_checks = False
    else:
        max_trials = RANSAC_BASE_MAX_TRIALS
        include_group_checks = True

    planes = []
    min_points_per_plane = min(8, int(8 / resolution_metres))  # 8 for 2m, 8 for 1m, 16 for 0.5m
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
                       include_group_checks=include_group_checks,
                       debug=debug)
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
                "inliers_xy": XY[inlier_mask],
                "sd": ransac.sd,
                "aspect_circ_mean": ransac.plane_properties["aspect_circ_mean"],
                "aspect_circ_sd": ransac.plane_properties["aspect_circ_sd"],
            })

            xyz = xyz[outlier_mask]
            aspect = aspect[outlier_mask]
        except RANSACValueError as e:
            if debug:
                print("No plane found - received RANSACValueError:")
                print(e)
                print("")
            break

    return planes


def _load(pg_uri: str, job_id: int, page: int, page_size: int, toids: List[str] = None) -> Dict[str, List[dict]]:
    """
    Load LIDAR pixel data for RANSAC processing. page_size is number of
    buildings rather than pixels to prevent splitting a building's pixels across
    pages.
    """
    with connection(pg_uri, cursor_factory=DictCursor) as pg_conn:
        elevation_table = f"{tables.schema(job_id)}.{tables.ELEVATION}"
        aspect_table = f"{tables.schema(job_id)}.{tables.ASPECT}"
        by_toid = pixels_for_buildings(pg_conn, job_id, page, page_size, [elevation_table, aspect_table], toids)
        return by_toid


def _building_count(pg_uri: str, job_id: int):
    with connection(pg_uri, cursor_factory=DictCursor) as pg_conn:
        return sql_command(
            pg_conn,
            "SELECT COUNT(*) FROM {buildings};",
            buildings=Identifier(tables.schema(job_id), tables.BUILDINGS_TABLE),
            result_extractor=lambda rows: rows[0][0])


def _save_planes(pg_uri: str, job_id: int, planes: List[dict]):
    if len(planes) == 0:
        return

    for plane in planes:
        plane['inliers_xy'] = plane['inliers_xy'].tolist()

    with connection(pg_uri) as pg_conn, pg_conn.cursor() as cursor:
        execute_values(cursor, SQL("""
            INSERT INTO {roof_polygons} (
                toid, 
                roof_geom_27700, 
                x_coef, 
                y_coef, 
                intercept, 
                slope, 
                aspect, 
                sd, 
                is_flat, 
                usable, 
                easting, 
                northing, 
                raw_footprint, 
                raw_area, 
                archetype,
                inliers_xy
            ) VALUES %s;
        """).format(
            roof_polygons=Identifier(tables.schema(job_id), tables.ROOF_POLYGON_TABLE),
        ), argslist=planes,
           template="""(%(toid)s, 
                        %(roof_geom_27700)s, 
                        %(x_coef)s,
                        %(y_coef)s, 
                        %(intercept)s, 
                        %(slope)s, 
                        %(aspect)s, 
                        %(sd)s, 
                        %(is_flat)s, 
                        %(usable)s, 
                        %(easting)s, 
                        %(northing)s, 
                        %(raw_footprint)s, 
                        %(raw_area)s, 
                        %(archetype)s,
                        %(inliers_xy)s )""")

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
