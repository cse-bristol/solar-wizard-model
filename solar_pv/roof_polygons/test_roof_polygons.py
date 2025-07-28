# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import json
from os.path import join
from typing import Tuple, List

import numpy as np
from shapely import wkt, Polygon

from solar_pv import paths
from solar_pv.datatypes import RoofPolygon
from solar_pv.roof_polygons.roof_polygons import _create_roof_polygons
from solar_pv.test_utils.test_funcs import ParameterisedTestCase

def _load_test_data(toid: str):
    roof_polys_dir = join(paths.TEST_DATA, "roof_polygons")
    with open(join(roof_polys_dir, f"{toid}.json")) as f:
        data = json.load(f)
        planes = data['planes']
        for plane in planes:
            plane['inliers_xy'] = np.array(plane['inliers_xy'])
            # fix up old test data...
            if 'is_flat' not in plane:
                plane['is_flat'] = plane['slope'] <= 5
        building_geom = wkt.loads(data['building_geom'])
    return planes, building_geom


def _create_polygons_using_test_data(toid: str,
                                     max_roof_slope_degrees: int = 80,
                                     min_roof_area_m: int = 8,
                                     min_roof_degrees_from_north: int = 45,
                                     flat_roof_degrees: int = 10,
                                     min_dist_to_edge_m: float = 0.3) -> Tuple[List[RoofPolygon], Polygon]:
    planes, building_geom = _load_test_data(toid)
    planes = _create_roof_polygons(
        building_geom,
        planes,
        max_roof_slope_degrees=max_roof_slope_degrees,
        min_roof_area_m=min_roof_area_m,
        min_roof_degrees_from_north=min_roof_degrees_from_north,
        flat_roof_degrees=flat_roof_degrees,
        min_dist_to_edge_m=min_dist_to_edge_m,
        resolution_metres=1.0,
        debug=True)

    return planes, building_geom


class RoofPolygonTest(ParameterisedTestCase):

    def test_roof_polygons_do_not_overlap(self):
        def _do_test(toid: str):
            planes, _ = _create_polygons_using_test_data(toid)
            for p1 in planes:
                for p2 in planes:
                    poly1 = p1['roof_geom_27700']
                    poly2 = p2['roof_geom_27700']
                    if p1['roof_plane_id'] != p2['roof_plane_id']:
                        crossover = poly1.intersection(poly2).area
                        assert crossover == 0, f"{p1['roof_plane_id']} overlaps {p2['roof_plane_id']} by {crossover} m2"

        self.parameterised_test([
            ("0001", None),
            ("0005", None),
        ], _do_test)

    def test_roof_polygons_stay_within_building(self):
        def _do_test(toid: str):
            min_dist_to_edge_m = 0.55
            planes, building_geom = _create_polygons_using_test_data(toid, min_dist_to_edge_m=min_dist_to_edge_m)
            building_geom = building_geom.buffer(-min_dist_to_edge_m)
            for p in planes:
                poly = p['roof_geom_27700']
                crossover = poly.difference(building_geom).area
                assert crossover < 0.1, f"{p['roof_plane_id']} overlaps  -ve buffered building by {crossover} m2"

        self.parameterised_test([
            ("0001", None),
            ("0005", None),
        ], _do_test)

    def test_failing_roof_polygons(self):
        def _do_test(toid: str, min_dist_to_edge_m: float = 0.55):
            _create_polygons_using_test_data(toid, min_dist_to_edge_m=min_dist_to_edge_m)

        self.parameterised_test([
            ("0008", None),
            ("0004", None),
            ("0006", None),
            ("0003", None),
            ("0009", 0.4, None),
            ("0011", 0.1, None),
            ("0010", 0.1, None),
            ("0013", 0.1, None),
            ("0012", 0.1, None),
            ("0014", 0.1, None),
            ("0015", 0.1, None),
            ("0016", 0.1, None),
            ("0017", 0.1, None),
            ("0018", 0.1, None),
        ], _do_test)

    def test_merge(self):
        planes, _ = _create_polygons_using_test_data("0007")
        assert len(planes) == 2
