import itertools
# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import logging
import time
import math
import traceback
from typing import List, Dict, TypedDict
import multiprocessing as mp

from psycopg2.extras import DictCursor, execute_values
from psycopg2.sql import SQL, Identifier, Literal
import numpy as np
from shapely import wkt, ops
from shapely.geometry import Polygon, LineString
from sklearn.linear_model import LinearRegression

from solar_pv.db_funcs import count, connection, sql_command
from solar_pv.postgis import pixels_for_buildings
from solar_pv import tables
from solar_pv.constants import RANSAC_LARGE_BUILDING, \
    RANSAC_LARGE_MAX_TRIALS, RANSAC_SMALL_MAX_TRIALS, FLAT_ROOF_DEGREES_THRESHOLD, \
    RANSAC_SMALL_BUILDING, RANSAC_MEDIUM_MAX_TRIALS
from solar_pv.ransac.detsac import DETSACRegressorForLIDAR
from solar_pv.ransac.premade_planes import create_planes, create_planes_2
from solar_pv.ransac.ransac import RANSACRegressorForLIDAR, _aspect, \
    _slope, RANSACValueError
from solar_pv.roof_polygons.roof_polygons import create_roof_polygons
from solar_pv.util import get_cpu_count


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
        res = pool.starmap_async(_handle_building_page, wrapped_iterable)
        # Hacky way to poll for failures in workers, doesn't seem to be a nicer way
        # of doing this:
        while not res.ready():
            if not res._success:
                pool.terminate()
                pool.join()
                raise ValueError('Cancelling RANSAC due to failure in worker')
            time.sleep(1)

    _mark_buildings_with_no_planes(pg_uri, job_id)
    logging.info(f"RANSAC for {building_count} roofs took {round(time.time() - start_time, 2)} s.")


def _handle_building_page(pg_uri: str, job_id: int, page: int, page_size: int, params: dict):
    start_time = time.time()
    buildings = _load(pg_uri, job_id, page, page_size)

    planes = []
    for toid, building in buildings.items():
        try:
            found = _ransac_building(building['pixels'], toid, params['resolution_metres'], building['polygon'])
            if len(found) > 0:
                planes.extend(found)
        except Exception as e:
            print(f"Exception during RANSAC for TOID {toid}:")
            traceback.print_exception(e)
            _print_test_data(building['pixels'])
            raise e

    planes = create_roof_polygons(pg_uri, job_id, planes, **params)
    _save_planes(pg_uri, job_id, planes)
    print(f"Page {page} of {page_size} buildings complete, took {round(time.time() - start_time, 2)} s.")


def _ransac_building(pixels_in_building: List[dict],
                     toid: str,
                     resolution_metres: float,
                     polygon: Polygon = None,
                     debug: bool = False) -> List[dict]:
    xyz = np.array([[pixel["x"], pixel["y"], pixel["elevation"]] for pixel in pixels_in_building])
    aspect = np.array([pixel["aspect"] for pixel in pixels_in_building])
    mask = np.ones(aspect.shape)

    if len(pixels_in_building) > RANSAC_LARGE_BUILDING / resolution_metres:
        max_trials = RANSAC_LARGE_MAX_TRIALS
        # Disables checks that forbid planes that cover multiple discontinuous groups
        # of pixels, as large buildings often have separate roof areas that are on the
        # same plane. Only the largest group will be used each time anyway, so this
        # won't cause problems and all discontinuous groups should be picked up
        # eventually.
        include_group_checks = False
    elif len(pixels_in_building) < RANSAC_SMALL_BUILDING / resolution_metres:
        max_trials = RANSAC_SMALL_MAX_TRIALS
        include_group_checks = True
    else:
        max_trials = RANSAC_MEDIUM_MAX_TRIALS
        include_group_checks = True

    planes = _do_ransac_building(toid, xyz, aspect, mask, polygon, resolution_metres, max_trials, include_group_checks,
                                 sample_residual_thresholds=[2.0, 0.25], debug=debug)

    return planes


def _do_ransac_building(toid: str,
                        xyz,
                        aspect,
                        mask,
                        polygon: Polygon,
                        resolution_metres: float,
                        max_trials: int,
                        include_group_checks: bool,
                        sample_residual_thresholds: List[float],
                        debug: bool):
    planes = []
    min_points_per_plane = min(8, int(8 / resolution_metres))  # 8 for 2m, 8 for 1m, 16 for 0.5m
    total_points_in_building = len(aspect)
    premade_planes = create_planes_2(xyz, aspect, polygon, resolution_metres)
    # premade_planes = create_planes(pixels_in_building, polygon)

    while np.count_nonzero(mask) > min_points_per_plane:
        XY = xyz[:, :2]
        Z = xyz[:, 2]
        try:
            ransac = DETSACRegressorForLIDAR(residual_threshold=0.25,
                                             sample_residual_thresholds=sample_residual_thresholds,
                                             flat_roof_residual_threshold=0.1,
                                             max_trials=max_trials,
                                             max_slope=75,
                                             min_slope=0,
                                             flat_roof_threshold_degrees=FLAT_ROOF_DEGREES_THRESHOLD,
                                             min_points_per_plane=min_points_per_plane,
                                             resolution_metres=resolution_metres)
            ransac.fit(XY, Z,
                       aspect=aspect,
                       mask=mask,
                       premade_planes=premade_planes,
                       total_points_in_building=total_points_in_building,
                       include_group_checks=include_group_checks,
                       debug=debug)
            inlier_mask = ransac.inlier_mask_
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
                "score": ransac.plane_properties["score"],
                "aspect_circ_mean": ransac.plane_properties["aspect_circ_mean"],
                "aspect_circ_sd": ransac.plane_properties["aspect_circ_sd"],
                "thinness_ratio": ransac.plane_properties["thinness_ratio"],
                "cv_hull_ratio": ransac.plane_properties["cv_hull_ratio"],
                "plane_type": ransac.plane_properties["plane_type"],
            })

            mask[inlier_mask] = 0
        except RANSACValueError as e:
            if debug:
                print("No plane found - received RANSACValueError:")
                print(e)
                print("")
            break
    return planes


class BuildingData(TypedDict):
    toid: str
    pixels: List[dict]
    polygon: Polygon


def _load(pg_uri: str, job_id: int, page: int, page_size: int, toids: List[str] = None, force_load: bool = False) -> Dict[str, BuildingData]:
    """
    Load LIDAR pixel data for RANSAC processing. page_size is number of
    buildings rather than pixels to prevent splitting a building's pixels across
    pages.
    """
    with connection(pg_uri, cursor_factory=DictCursor) as pg_conn:
        elevation_table = f"{tables.schema(job_id)}.{tables.ELEVATION}"
        aspect_table = f"{tables.schema(job_id)}.{tables.ASPECT}"
        by_toid = pixels_for_buildings(pg_conn, job_id, page, page_size, [elevation_table, aspect_table], toids, force_load=force_load)
        buildings = _load_building_polygons(pg_conn, job_id, list(by_toid.keys()))
        loaded = {}
        for building in buildings:
            toid = building["toid"]
            pixels = by_toid[toid]
            loaded[toid] = {}
            loaded[toid]["toid"] = toid
            loaded[toid]["polygon"] = wkt.loads(building["polygon"])
            loaded[toid]["pixels"] = pixels
        return loaded


def _load_building_polygons(pg_conn, job_id, toids: List[str]) -> List[dict]:
    buildings = sql_command(
            pg_conn,
            """
            SELECT toid, ST_AsText(geom_27700) AS polygon 
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


def _print_test_data(building: List[dict]):
    """Print data for building in the format that the RANSAC tests expect"""
    print("pixel_id,x,y,elevation,aspect\n")
    for pixel in building:
        print(f"{pixel['pixel_id']},{pixel['x']},{pixel['y']},{pixel['elevation']},{pixel['aspect']}")
