# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import json
from os.path import join
from typing import Tuple, List

import numpy as np
from shapely import wkt, Polygon
from shapely.geometry import JOIN_STYLE

from solar_pv import paths
from solar_pv.roof_detection.detect_roofs import _detect_building_roof_planes
from solar_pv.roof_detection.test_detect_roofs import _load_data
from solar_pv.datatypes import RoofPolygon
from solar_pv.roof_polygons.roof_polygons import _create_roof_polygons
from solar_pv.test_utils.test_funcs import ParameterisedTestCase

_METHODS = ("legacy", "mincut")


def _load_test_data(building_id: str):
    roof_polys_dir = join(paths.TEST_DATA, "roof_polygons")
    with open(join(roof_polys_dir, f"{building_id}.json")) as f:
        data = json.load(f)
        planes = data['planes']
        for plane in planes:
            plane['inliers_xy'] = np.array(plane['inliers_xy'])
            # fix up old test data...
            if 'is_flat' not in plane:
                plane['is_flat'] = plane['slope'] <= 5
        building_geom = wkt.loads(data['building_geom'])
    return planes, building_geom


def _create_polygons_using_test_data(building_id: str,
                                     max_roof_slope_degrees: int = 80,
                                     min_roof_area_m: int = 8,
                                     min_roof_degrees_from_north: int = 45,
                                     flat_roof_degrees: int = 10,
                                     min_dist_to_edge_m: float = 0.3,
                                     method: str = "legacy") -> Tuple[List[RoofPolygon], Polygon]:
    planes, building_geom = _load_test_data(building_id)
    planes = _create_roof_polygons(
        building_geom,
        planes,
        max_roof_slope_degrees=max_roof_slope_degrees,
        min_roof_area_m=min_roof_area_m,
        min_roof_degrees_from_north=min_roof_degrees_from_north,
        flat_roof_degrees=flat_roof_degrees,
        min_dist_to_edge_m=min_dist_to_edge_m,
        resolution_metres=1.0,
        method=method,
        debug=True)

    return planes, building_geom


def _create_polygons_from_lidar(building_id: str, min_dist_to_edge_m: float = 0.3):
    """Detect planes from a roof detection fixture, then make polygons, both by min-cut."""
    building = _load_data(join(paths.TEST_DATA, "roof_detection", f"{building_id}.json"))
    np.random.seed(42)
    planes = _detect_building_roof_planes(building, building_id, 1.0, method="mincut")
    polygons = _create_roof_polygons(
        building['polygon'],
        planes,
        max_roof_slope_degrees=80,
        min_roof_area_m=8,
        min_roof_degrees_from_north=45,
        flat_roof_degrees=10,
        min_dist_to_edge_m=min_dist_to_edge_m,
        resolution_metres=1.0,
        method="mincut")
    return polygons, building['polygon']


class RoofPolygonTest(ParameterisedTestCase):

    def test_mincut_from_lidar(self):
        def _do_test(building_id: str):
            min_dist_to_edge_m = 0.3
            polygons, building_geom = _create_polygons_from_lidar(building_id, min_dist_to_edge_m)
            assert len(polygons) > 0
            shrunk = building_geom.buffer(-min_dist_to_edge_m, join_style=JOIN_STYLE.mitre)
            for i, p1 in enumerate(polygons):
                crossover = p1['roof_geom_27700'].difference(shrunk).area
                assert crossover < 0.1, f"plane {i} overlaps -ve buffered building by {crossover} m2"
                for j, p2 in enumerate(polygons):
                    if i != j:
                        crossover = p1['roof_geom_27700'].intersection(p2['roof_geom_27700']).area
                        assert crossover == 0, f"plane {i} overlaps {j} by {crossover} m2"

        self.parameterised_test([
            (building_id, None)
            for building_id in ("0004", "0005", "0007", "0015", "0020", "0022", "0026", "0032")
        ], _do_test)

    def test_roof_polygons_do_not_overlap(self):
        def _do_test(building_id: str, method: str):
            planes, _ = _create_polygons_using_test_data(building_id, method=method)
            for i, p1 in enumerate(planes):
                for j, p2 in enumerate(planes):
                    poly1 = p1['roof_geom_27700']
                    poly2 = p2['roof_geom_27700']
                    if i != j:
                        crossover = poly1.intersection(poly2).area
                        assert crossover == 0, f"{method}: plane {i} overlaps {j} by {crossover} m2"

        self.parameterised_test([
            ("0001", "legacy", None),
            ("0005", "legacy", None),
        ] + [
            (building_id, "mincut", None)
            for building_id in ("0001", "0005", "0011", "0015", "0016")
        ], _do_test)

    def test_roof_polygons_stay_within_building(self):
        def _do_test(building_id: str, method: str):
            min_dist_to_edge_m = 0.55
            planes, building_geom = _create_polygons_using_test_data(
                building_id, min_dist_to_edge_m=min_dist_to_edge_m, method=method)
            building_geom = building_geom.buffer(-min_dist_to_edge_m, join_style=JOIN_STYLE.mitre)
            for i, p in enumerate(planes):
                poly = p['roof_geom_27700']
                crossover = poly.difference(building_geom).area
                assert crossover < 0.1, f"{method}: plane {i} overlaps -ve buffered building by {crossover} m2"

        self.parameterised_test([
            (building_id, method, None)
            for building_id in ("0001", "0005", "0011", "0015")
            for method in _METHODS
        ], _do_test)

    def test_failing_roof_polygons(self):
        def _do_test(building_id: str, min_dist_to_edge_m: float, method: str):
            _create_polygons_using_test_data(building_id, min_dist_to_edge_m=min_dist_to_edge_m, method=method)

        self.parameterised_test([
            (building_id, min_dist_to_edge_m, method, None)
            for building_id, min_dist_to_edge_m in (
                ("0008", 0.55),
                ("0004", 0.55),
                ("0006", 0.55),
                ("0003", 0.55),
                ("0009", 0.4),
                ("0011", 0.1),
                ("0010", 0.1),
                ("0013", 0.1),
                ("0012", 0.1),
                ("0014", 0.1),
                ("0015", 0.1),
                ("0016", 0.1),
                ("0017", 0.1),
                ("0018", 0.1),
            )
            for method in _METHODS
        ], _do_test)

    def test_merge(self):
        # "mincut" leaves merging planes to roof detection
        planes, _ = _create_polygons_using_test_data("0007", method="legacy")
        assert len(planes) == 2
