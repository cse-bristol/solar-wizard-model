# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import json
import logging
import math
from os.path import join
from typing import List, Optional

import time
from shapely import ops
from shapely.geometry import MultiPoint, MultiPolygon, CAP_STYLE, JOIN_STYLE

from solar_pv import paths

from osgeo import ogr, gdal

from solar_pv.geos import square, to_geojson_dict, de_zigzag, simplify_by_angle
from solar_pv.lidar.lidar import LIDAR_NODATA
from solar_pv.roof_detection.detect_roofs import _detect_building_roof_planes, _load


def detect_toid_roofs(pg_uri: str, job_id: int, toids: Optional[List[str]], resolution_metres: float, out_dir: str, write_test_data: bool = True):
    logging.basicConfig(level=logging.INFO,
                        format='[%(asctime)s] %(levelname)s: %(message)s')
    os.makedirs(out_dir, exist_ok=True)

    by_toid = _load(pg_uri, job_id, page=0, page_size=1000, toids=toids, force_load=True)
    all_planes = []
    for toid, building in by_toid.items():
        print(f"\nTOID: {toid}\n")
        planes = _detect_building_roof_planes(building, toid, resolution_metres, debug=True)
        all_planes.extend(planes)

        if len(planes) > 0:
            print("\nROOFDET: all planes:")
            for plane in planes:
                print(f'type {plane["plane_type"]} toid {plane["toid"]} slope {plane["slope"]} aspect {plane["aspect"]} sd {plane["sd"]} inliers {len(plane["inliers_xy"])}')
        else:
            print("No planes to write, not creating geoJSON")
        if write_test_data:
            _write_test_data(toid, building)

    if len(all_planes) > 0:
        _write_planes(toids, job_id, resolution_metres, out_dir, all_planes)


def _write_planes(toids: Optional[List[str]], job_id: int, resolution_metres: float, out_dir: str, planes):
    if toids is None:
        filename = f"job_{job_id}"
    elif len(toids) == 1:
        filename = f"job_{job_id}_{toids[0]}"
    else:
        filename = f"job_{job_id}_toids"
    t = int(time.time())
    geojson_out = join(out_dir, f"{filename}-{t}.geojson")

    feature_coll = {"type": "FeatureCollection",
                    "crs": {"type": "name",
                            "properties": {"name": "urn:ogc:def:crs:EPSG::27700"}},
                    "features": []}
    for plane in planes:
        halfr = resolution_metres / 2
        r = resolution_metres
        pixels = [square(xy[0] - halfr, xy[1] - halfr, r) for xy in plane['inliers_xy']]
        geom = ops.unary_union(pixels)

        # plane['aspect'] = plane['aspect_adjusted']
        # geom = _initial_polygon(plane, resolution_metres)
        # geom = _grid_polygon(plane, resolution_metres)
        # geom = simplify_by_angle(geom)
        # geom = de_zigzag(geom)
        # geom = geom.buffer(math.sqrt(resolution_metres / 2) / 2, cap_style=CAP_STYLE.square, join_style=JOIN_STYLE.mitre, resolution=1)

        geojson = to_geojson_dict(geom)
        plane['inliers'] = len(plane['inliers_xy'])
        del plane['inliers_xy']
        feature_coll['features'].append({"type": "Feature",
                                         "geometry": geojson,
                                         "properties": plane})

    with open(geojson_out, 'w') as f:
        json.dump(feature_coll, f, default=str)

    print(f"Wrote debug data to file {geojson_out}")


def _write_test_data(toid, building):
    test_data_dir = join(paths.TEST_DATA, "roof_detection")
    testfile = join(test_data_dir, f"{toid}.json")
    with open(testfile, 'w') as f:
        json.dump(building, f, default=str)
    print(f"Wrote test data to {testfile}")


def thinness_ratio_experiments():
    import numpy as np

    def tr(width, height):
        perimeter = width * 2 + height * 2
        area = width * height
        _tr = (4 * np.pi * area) / (perimeter * perimeter)
        print(f"area: {area} ({width} x {height}) TR: {_tr}")
        return _tr

    def trs(area):
        for width in range(1, area):
            height = area / width
            if height < width:
                break
            tr(width, height)

    # Question: for each area, there will be a given threshold that makes sense - what is it?
    # trs(10)  # 0.55
    # trs(20)  # 0.55
    # trs(30)  # 0.5
    # trs(40)  # 0.45
    # trs(50)  # 0.4
    # trs(100)  # 0.25
    # trs(200)  # 0.25
    # trs(300)  # 0.25
    # trs(400)  # 0.2
    # trs(500)  # 0.2
    # trs(750)  # 0.15
    # trs(1000)  # 0.10
    # trs(2000)  # 0.10
    # trs(3000)  # 0.10
    # trs(5000)  # 0.07


if __name__ == "__main__":
    import os
    # thinness_ratio_experiments()

    # detect_toid_roofs(
    #     os.getenv("PGW_URI"),
    #     1659,
    #     [
    #         # "osgb1000014994638",
    #         # "osgb1000014994637",
    #         # "osgb1000014994948",
    #         # "osgb1000014994624",
    #         # "osgb1000014994950",
    #         # "osgb1000014994951",
    #
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
    #         "osgb1000014998049",
    #         "osgb1000014998048",
    #     ],
    #     1.0,
    #     f"{os.getenv('DEV_DATA_DIR')}/ransac",
    #     write_test_data=False)

    # detect_toid_roofs(
    #     os.getenv("PGW_URI"),
    #     1649,
    #     ["osgb1000014994631"],
    #     # None,
    #     1.0,
    #     f"{os.getenv('DEV_DATA_DIR')}/ransac",
    #     write_test_data=False)

    # detect_toid_roofs(
    #     os.getenv("PGW_URI"),
    #     1661,
    #     [
    #         "osgb1000021672464",
    #         "osgb1000000337215292",
    #         "osgb1000021681586",
    #         "osgb1000021672474",
    #         "osgb1000021672476",
    #         "osgb1000021672457",
    #         "osgb1000021672466",
    #         "osgb1000000337226766",
    #     ],
    #     1.0,
    #     f"{os.getenv('DEV_DATA_DIR')}/ransac",
    #     write_test_data=False)

    # detect_toid_roofs(
    #     os.getenv("PGW_URI"),
    #     1660,
    #     [
    #         # "osgb5000005110302956",
    #         # "osgb1000014963168",
    #
    #         # messy:
    #         "osgb1000002529080353",
    #         "osgb1000002529080355",
    #         "osgb1000002529080354",
    #     ],
    #     # None,
    #     1.0,
    #     f"{os.getenv('DEV_DATA_DIR')}/ransac",
    #     write_test_data=False)

    detect_toid_roofs(
        os.getenv("PGW_URI"),
        1663,
        [
            "osgb1000000054783152",
            # "osgb1000036903249",
        ],
        # None,
        1.0,
        f"{os.getenv('DEV_DATA_DIR')}/ransac",
        write_test_data=False)
