# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
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

from solar_pv import paths
from solar_pv.db_funcs import connection, sql_command
from solar_pv import tables
from solar_pv.roof_polygons.roof_polygons_2 import _building_geoms, \
    _create_roof_polygons, _to_test_data

_MAX_ROOF_SLOPE_DEGREES = 70
_MIN_ROOF_AREA_M = 8
_MIN_ROOF_DEGREES_FROM_NORTH = 0
_FLAT_ROOF_DEGREES = 10
_LARGE_BUILDING_THRESHOLD = 200
_MIN_DIST_TO_EDGE_M = 0.1
_MIN_DIST_TO_EDGE_LARGE_M = 0.1


def make_job_roof_polygons(pg_uri: str, job_id: int,
                           resolution_metres: float, out_dir: str,
                           toids: List[str] = None,
                           make_planes: bool = False,
                           write_test_data: bool = True):
    logging.basicConfig(level=logging.DEBUG,
                        format='[%(asctime)s] %(levelname)s: %(message)s')

    with connection(pg_uri, cursor_factory=psycopg2.extras.DictCursor) as pg_conn:
        if toids is None:
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
            if make_planes:
                planes = _make_roof_planes(pg_uri, job_id, toid, resolution_metres)
            else:
                planes = _load_toid_planes(pg_uri, job_id, toid)

            logging.info(f"TOID: {toid}")
            polygons = _create_roof_polygons(building_geoms,
                                             planes,
                                             max_roof_slope_degrees=_MAX_ROOF_SLOPE_DEGREES,
                                             min_roof_area_m=_MIN_ROOF_AREA_M,
                                             min_roof_degrees_from_north=_MIN_ROOF_DEGREES_FROM_NORTH,
                                             flat_roof_degrees=_FLAT_ROOF_DEGREES,
                                             large_building_threshold=_LARGE_BUILDING_THRESHOLD,
                                             min_dist_to_edge_m=_MIN_DIST_TO_EDGE_M,
                                             min_dist_to_edge_large_m=_MIN_DIST_TO_EDGE_LARGE_M,
                                             resolution_metres=resolution_metres)
            logging.info(f"Created {len(polygons)} planes for toid {toid}")
            all_planes.extend(polygons)

        logging.info(f"found {len(all_planes)} planes, took {round(time.time() - t0, 2)}s")

        if write_test_data:
            t = int(time.time())
            _write_outputs(f"{job_id}_planes_{t}", all_planes, out_dir)


def make_roof_polygons_all(pg_uri: str, job_id: int, toids: List[str],
                           resolution_metres: float, out_dir: str,
                           make_planes: bool = False,
                           write_test_data: bool = True):
    for toid in toids:
        make_roof_polygons(pg_uri, job_id, toid, resolution_metres, out_dir, make_planes, write_test_data)


def make_roof_polygons(pg_uri: str, job_id: int, toid: str,
                       resolution_metres: float, out_dir: str,
                       make_planes: bool = False,
                       write_test_data: bool = True):
    logging.basicConfig(level=logging.DEBUG,
                        format='[%(asctime)s] %(levelname)s: %(message)s')
    os.makedirs(out_dir, exist_ok=True)

    if make_planes:
        planes = _make_roof_planes(pg_uri, job_id, toid, resolution_metres)
    else:
        planes = _load_toid_planes(pg_uri, job_id, toid)
    building_geoms = _building_geoms(pg_uri, job_id, [toid])

    if write_test_data:
        _write_test_data(toid, planes, building_geoms[toid], out_dir)

    planes = _create_roof_polygons(building_geoms,
                                   planes,
                                   max_roof_slope_degrees=_MAX_ROOF_SLOPE_DEGREES,
                                   min_roof_area_m=_MIN_ROOF_AREA_M,
                                   min_roof_degrees_from_north=_MIN_ROOF_DEGREES_FROM_NORTH,
                                   flat_roof_degrees=_FLAT_ROOF_DEGREES,
                                   large_building_threshold=_LARGE_BUILDING_THRESHOLD,
                                   min_dist_to_edge_m=_MIN_DIST_TO_EDGE_M,
                                   min_dist_to_edge_large_m=_MIN_DIST_TO_EDGE_LARGE_M,
                                   resolution_metres=resolution_metres)

    if write_test_data:
        _write_outputs(toid, planes, out_dir, building_geoms[toid])


def _make_roof_planes(pg_uri: str, job_id: int, toid: str, resolution_metres: float):
    from solar_pv.roof_detection.detect_roofs import _detect_building_roof_planes, _load
    by_toid = _load(pg_uri, job_id, page=0, page_size=1000, toids=[toid], force_load=True)
    building = by_toid[toid]
    planes = _detect_building_roof_planes(building, toid, resolution_metres, debug=True)
    return planes


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
    fname = join(out_dir, f"{name}.geojson")
    with open(fname, 'w') as f:
        json.dump(geojson, f)
    print(f"Wrote debug data to {fname}")


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
    #     os.getenv("PGW_URI"),
    #     1621,
    #     [
    #         # "osgb1000021445362",
    #         "osgb1000021445086",
    #         "osgb1000021445097",
    #     ],
    #     1.0,
    #     roof_polys_dir)

    # make_job_roof_polygons(
    #     os.getenv("PGW_URI"),
    #     1649,
    #     1.0,
    #     f"{os.getenv('DEV_DATA_DIR')}/roof-polys")

    # make_roof_polygons(
    #     os.getenv("PGW_URI"),
    #     1648,
    #     "osgb1000014916349",
    #     1.0,
    #     f"{os.getenv('DEV_DATA_DIR')}/roof-polys")

    # make_job_roof_polygons(
    #     os.getenv("PGW_URI"),
    #     1657,
    #     1.0,
    #     f"{os.getenv('DEV_DATA_DIR')}/roof-polys",
    #     make_planes=True)

    # make_job_roof_polygons(
    #     os.getenv("PGW_URI"),
    #     1659,
    #     1.0,
    #     f"{os.getenv('DEV_DATA_DIR')}/roof-polys",
    #     toids=[
    #         "osgb5000005116861453",
    #         "osgb5000005116861461",
    #         "osgb1000014994628",
    #         "osgb1000014994636",
    #         "osgb1000014994648",
    #         "osgb1000014994630",
    #         "osgb1000014994634",
    #         "osgb1000014994631",
    #         "osgb1000014994632",
    #         "osgb1000014994629",
    #         "osgb1000014994635",
    #         "osgb1000014994633",
    #         "osgb1000014994626",
    #         "osgb1000014994627",
    #         "osgb1000014994624",
    #         "osgb1000014994625",
    #         "osgb1000014994654",
    #         "osgb1000014994658",
    #         "osgb1000014994649",
    #         "osgb1000014994652",
    #         "osgb1000014994646",
    #         "osgb1000014994653",
    #         "osgb1000014994641",
    #         "osgb1000014994651",
    #         "osgb1000014994639",
    #         "osgb1000014994644",
    #         "osgb1000014994637",
    #         "osgb1000014994650",
    #         "osgb1000014994655",
    #         "osgb1000014994657",
    #         "osgb1000014994660",
    #         "osgb1000014994656",
    #         "osgb1000014994647",
    #         "osgb1000014994643",
    #         "osgb1000014994642",
    #         "osgb1000014994645",
    #         "osgb1000014994659",
    #         "osgb1000014994638",
    #         "osgb1000014994640",
    #         "osgb1000014995257",
    #         "osgb5000005116861456",
    #         "osgb1000014995257",
    #
    #         "osgb1000014994950",
    #         "osgb1000014994952",
    #         "osgb1000014994947",
    #         "osgb1000014994949",
    #         "osgb1000014994951",
    #         "osgb1000014994948",
    #
    #         "osgb1000014998052",
    #
    #         "osgb1000014994877",
    #         "osgb1000014995098",
    #         "osgb1000014994794",
    #         "osgb1000014995098",
    #         "osgb1000014998049",
    #         "osgb1000014998048",
    #     ],
    #     make_planes=True)

    make_job_roof_polygons(
            os.getenv("PGW_URI"),
            1660,
            1.0,
            f"{os.getenv('DEV_DATA_DIR')}/roof-polys",
            [
                "osgb5000005110302956",
                "osgb1000014963168",

                # messy:
                "osgb1000002529080353",
                "osgb1000002529080355",
                "osgb1000002529080354",
            ],
            make_planes=True)
