import json
import logging
import os
import time
from os.path import join
from typing import List

import psycopg2.extras
from psycopg2.sql import Identifier
import numpy as np
from shapely import geometry, wkt
from shapely.geometry import Polygon

from albion_models import paths
from albion_models.db_funcs import connection, sql_command
from albion_models.solar_pv import tables
from albion_models.solar_pv.roof_polygons.roof_polygons import _building_geoms, \
    _create_roof_polygons, _to_test_data

_MAX_ROOF_SLOPE_DEGREES = 70
_MIN_ROOF_AREA_M = 8
_MIN_ROOF_DEGREES_FROM_NORTH = 45
_FLAT_ROOF_DEGREES = 10
_LARGE_BUILDING_THRESHOLD = 200
_MIN_DIST_TO_EDGE_M = 0.4
_MIN_DIST_TO_EDGE_LARGE_M = 1
_PANEL_W_M = 0.99
_PANEL_H_M = 1.64


def make_job_roof_polygons(pg_uri: str, job_id: int,
                           resolution_metres: float, out_dir: str,
                           write_test_data: bool = True):
    logging.basicConfig(level=logging.DEBUG,
                        format='[%(asctime)s] %(levelname)s: %(message)s')

    with connection(pg_uri, cursor_factory=psycopg2.extras.DictCursor) as pg_conn:
        toids = sql_command(
            pg_conn,
            "SELECT toid FROM {buildings}",
            buildings=Identifier(tables.schema(job_id), tables.BUILDINGS_TABLE),
            result_extractor=lambda rows: [row[0] for row in rows])
        t0 = time.time()
        logging.info(f"TOIDS: {len(toids)}")

        building_geoms = _building_geoms(pg_uri, job_id, toids)
        all_planes = []
        for toid in toids:
            planes = _load_toid_planes(pg_uri, job_id, toid)
            polygons = _create_roof_polygons(building_geoms,
                                             planes,
                                             max_roof_slope_degrees=_MAX_ROOF_SLOPE_DEGREES,
                                             min_roof_area_m=_MIN_ROOF_AREA_M,
                                             min_roof_degrees_from_north=_MIN_ROOF_DEGREES_FROM_NORTH,
                                             flat_roof_degrees=_FLAT_ROOF_DEGREES,
                                             large_building_threshold=_LARGE_BUILDING_THRESHOLD,
                                             min_dist_to_edge_m=_MIN_DIST_TO_EDGE_M,
                                             min_dist_to_edge_large_m=_MIN_DIST_TO_EDGE_LARGE_M,
                                             panel_width_m=_PANEL_W_M,
                                             panel_height_m=_PANEL_H_M,
                                             resolution_metres=resolution_metres)
            logging.info(f"Created {len(polygons)} planes for toid {toid}")
            all_planes.extend(polygons)

        logging.info(f"found {len(all_planes)} planes, took {round(time.time() - t0, 2)}s")

        if write_test_data:
            t = int(time.time())
            _write_outputs(f"{job_id}_planes_{t}", all_planes, out_dir)


def make_roof_polygons_all(pg_uri: str, job_id: int, toids: List[str],
                           resolution_metres: float, out_dir: str,
                           write_test_data: bool = True):
    for toid in toids:
        make_roof_polygons(pg_uri, job_id, toid, resolution_metres, out_dir, write_test_data)


def make_roof_polygons(pg_uri: str, job_id: int, toid: str,
                       resolution_metres: float, out_dir: str,
                       write_test_data: bool = True):
    logging.basicConfig(level=logging.DEBUG,
                        format='[%(asctime)s] %(levelname)s: %(message)s')
    os.makedirs(out_dir, exist_ok=True)

    planes = _load_toid_planes(pg_uri, job_id, toid)
    building_geoms = _building_geoms(pg_uri, job_id, [toid])

    if write_test_data:
        _write_test_data(toid, planes, building_geoms[toid], out_dir)

    _create_roof_polygons(building_geoms,
                          planes,
                          max_roof_slope_degrees=_MAX_ROOF_SLOPE_DEGREES,
                          min_roof_area_m=_MIN_ROOF_AREA_M,
                          min_roof_degrees_from_north=_MIN_ROOF_DEGREES_FROM_NORTH,
                          flat_roof_degrees=_FLAT_ROOF_DEGREES,
                          large_building_threshold=_LARGE_BUILDING_THRESHOLD,
                          min_dist_to_edge_m=_MIN_DIST_TO_EDGE_M,
                          min_dist_to_edge_large_m=_MIN_DIST_TO_EDGE_LARGE_M,
                          panel_width_m=_PANEL_W_M,
                          panel_height_m=_PANEL_H_M,
                          resolution_metres=resolution_metres)

    if write_test_data:
        _write_outputs(toid, planes, out_dir, building_geoms[toid])


def _write_test_data(toid: str, planes: List[dict], building_geom: Polygon, out_dir: str):
    jsonfile = join(out_dir, f"{toid}.json")

    with open(jsonfile, 'w') as f:
        json.dump(_to_test_data(toid, planes, building_geom), f, sort_keys=True)


def _write_outputs(name: str, planes: List[dict], out_dir: str, building_geom: Polygon = None):
    geojson_features = []
    for plane in planes:
        if building_geom:
            plane['building_geom'] = building_geom.wkt
        geojson_geom = geometry.mapping(wkt.loads(plane['roof_geom_27700']))
        del plane['roof_geom_27700']
        del plane['inliers_xy']
        geojson_feature = {
          "type": "Feature",
          "geometry": geojson_geom,
          "properties": plane
        }
        geojson_features.append(geojson_feature)

    geojson = {
        "type": "FeatureCollection",
        "crs": {"type": "name", "properties": {"name": "urn:ogc:def:crs:EPSG::27700"}},
        "features": geojson_features
    }
    with open(join(out_dir, f"{name}.geojson"), 'w') as f:
        json.dump(geojson, f)


def _load_toid_planes(pg_uri: str, job_id: int, toid: str):
    """
    Load LIDAR pixel data for RANSAC processing for a specific TOID.
    """
    with connection(pg_uri, cursor_factory=psycopg2.extras.DictCursor) as pg_conn:
        planes = sql_command(
            pg_conn,
            """
            SELECT toid, roof_plane_id, slope, aspect, inliers_xy
            FROM {roof_polygons} 
            WHERE toid = %(toid)s
            ORDER BY roof_plane_id
            """,
            {"toid": toid},
            roof_polygons=Identifier(tables.schema(job_id), tables.ROOF_POLYGON_TABLE),
            result_extractor=lambda res: [dict(row) for row in res])

        for plane in planes:
            plane['inliers_xy'] = np.array(plane['inliers_xy'])
        return planes


if __name__ == "__main__":
    roof_polys_dir = join(paths.TEST_DATA, "roof_polygons")
    # make_roof_polygons_all(
    #     "postgresql://albion_webapp:ydBbE3JCnJ4@localhost:5432/albion?application_name=blah",
    #     1621,
    #     [
    #         # "osgb1000021445362",
    #         "osgb1000021445086",
    #         "osgb1000021445097",
    #     ],
    #     1.0,
    #     roof_polys_dir)

    # make_job_roof_polygons(
    #     "postgresql://albion_webapp:ydBbE3JCnJ4@localhost:5432/albion?application_name=blah",
    #     1648,
    #     1.0,
    #     "/home/neil/data/albion-models/roof-polys")

    make_roof_polygons(
        "postgresql://albion_webapp:ydBbE3JCnJ4@localhost:5432/albion?application_name=blah",
        1648,
        "osgb1000014916349",
        1.0,
        "/home/neil/data/albion-models/roof-polys")
