# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import csv
import json
import unittest
from os.path import join
from typing import List

import numpy as np
from shapely import wkt, box

from solar_pv.paths import TEST_DATA
from solar_pv.roof_detection.detect_roofs import _detect_building_roof_planes, _create_adaptive_batches, _assign_and_merge, \
    _set_skew, _trim_plane_coefficients
from solar_pv.roof_detection.merge_adjacent import set_plane_fit
from solar_pv.datatypes import RoofDetBuilding
from solar_pv.test_utils.test_funcs import ParameterisedTestCase

_ROOFDET_DATA = join(TEST_DATA, "roof_detection")

_SEED = 42


def _load_data(filename: str) -> RoofDetBuilding:
    with open(filename) as f:
        building = json.load(f)
        building['polygon'] = wkt.loads(building['polygon'])
    return building


def _roofdet(building_id: str, res: float, method: str = "legacy"):
    np.random.seed(_SEED)
    filename = f"{building_id}.json" if not building_id.endswith(".json") else building_id
    planes = _detect_building_roof_planes(_load_data(join(_ROOFDET_DATA, filename)), filename, res,
                                          debug=False, method=method)
    return sorted([plane['aspect'] for plane in planes])


# With "mincut", an 8-pixel flat fragment at the bottom edge of the M-shaped roof is
# kept as its own plane (it is too small to be usable once it's a polygon).
# The Cotham school roofs are messy in aerial imagery, but only partly in the LiDAR: in
# 0029, a small flat roof is found to be clean.
# In 0026, the 149-facing slope is two planes about 0.24m apart in height, which one plane
# fits poorly (MAE 0.14m).
_MINCUT_EXPECTED = {
    "0026": [59, 59, 149, 149, 239, 329],
    "0032": [82, 84, 168, 264, 264, 264],
    "0029": [149],
    "0028": [],
}


_ROOF_DETECTION_CASES = [
    # Tricky Totterdown terraces:
    ("0013", 1.0, [54, 234]),
    ("0008", 1.0, [54, 233]),
    ("0016", 1.0, [60, 240]),
    ("0003", 1.0, [57, 239]),
    ("0035", 1.0, [58, 238]),
    ("0021", 1.0, [56, 236]),
    ("0019", 1.0, [56, 236]),
    ("0031", 1.0, [55, 236]),
    ("0012", 1.0, [54, 236]),
    ("0008", 1.0, [54, 233]),
    ("0030", 1.0, [57, 234]),
    ("0034", 1.0, [54, 234]),
    ("0025", 1.0, [54, 234]),
    ("0027", 1.0, [54, 234]),
    ("0006", 1.0, [59, 236]),
    ("0018", 1.0, [58, 237]),
    ("0009", 1.0, [58, 237]),
    ("0011", 1.0, [61, 236]),

    # Irregular buildings:
    ("0032", 1.0, [82, 84, 168, 264]),
    # ("0005", 1.0, [148, 148, 148, 148]),  # Totterdown Mosque - currently fails due to dome

    # Messy roofs - should find nothing
    # ("0015", 1.0, []),  # Totterdown pub - very messy non-flat roof - currently fails due to mess
    # Cotham school - messy flat roofs. Imagery says 0029 is [149] and 0028 is [] (as mincut
    # finds), but legacy's merge_adjacent absorbs messy pixels into planes before the mess
    # check sees them.
    ("0029", 1.0, [149, 321]),
    ("0028", 1.0, [162, 254]),
    ("0033", 1.0, []),  # Cotham school - messy flat roof

    # Flat roofs:
    ("0023", 1.0, [138]),
    ("0017", 1.0, [141, 226]),

    # warehouses etc:
    ("0022", 1.0, [10, 46, 155, 190, 226, 335]),  # Motorbike shop
    ("0020", 1.0, [144, 144, 324, 324]),

    # Various tricky buildings in Croyde
    ("0004", 1.0, [166, 166, 167, 257, 346, 347, 347]),
    ("0001", 1.0, [154, 335]),
    ("0010", 1.0, [59, 238]),
    ("0024", 1.0, [73, 253]),
    ("0002", 1.0, [71, 251, 251]),
    ("0007", 1.0, [0, 90, 90, 180, 270]),
    ("0014", 1.0, [85, 265]),
    ("0026", 1.0, [59, 59, 149, 239, 329]),

    # Only has 1 pixel of lidar coverage...:
    ("0036", 1.0, []),

    # one plane that covers the entire roof and building is cardinally-aligned, so
    # no pixels outside building:
    ("0037", 1.0, [182]),

    # Below 0 height:
    ("0038", 1.0, [207]),
    # Totally round:
    ("0039", 1.0, []),

    # see premade_planes.py, L49-50
    ("0040", 1.0, []),
    ("0040", 1.0, []),

]


class RoofDetTestCase(ParameterisedTestCase):

    def test_roof_detection(self):
        self.parameterised_test(_ROOF_DETECTION_CASES, _roofdet)

    def test_roof_detection_mincut(self):
        self.parameterised_test(
            [(building_id, res, _MINCUT_EXPECTED.get(building_id, expected))
             for building_id, res, expected in _ROOF_DETECTION_CASES],
            lambda building_id, res: _roofdet(building_id, res, method="mincut"))

    def test_create_adaptive_batches(self):
        """Test that adaptive batching creates appropriate batch sizes based on building areas"""
        
        batches = _create_adaptive_batches([])
        self.assertEqual(batches, [])

        single_building = [("test_building", 100)]
        batches = _create_adaptive_batches(single_building)
        self.assertEqual(len(batches), 1)
        self.assertEqual(batches[0], ["test_building"])

        buildings_with_areas = [
            ("very_large_1", 30000),
            ("very_large_2", 25000),
            ("large_1", 3000),     
            ("large_2", 2800),      
            ("large_3", 2600),      
            ("medium_1", 300),     
            ("medium_2", 200),     
            ("medium_3", 150),     
            ("small_1", 80),       
            ("small_2", 70),       
            ("very_small_1", 30),  
            ("very_small_2", 20),  
            ("very_small_3", 10),  
        ]

        batches = _create_adaptive_batches(buildings_with_areas)
        
        expected_batches = [
            ["very_large_1"],
            ["very_large_2"],
            ["large_1", "large_2", "large_3"],
            ["medium_1", "medium_2", "medium_3", "small_1", "small_2", "very_small_1", "very_small_2", "very_small_3"],
        ]
        
        self.assertEqual(batches, expected_batches)


class AssignAndMergeTest(unittest.TestCase):

    def test_no_planes_left_after_assignment(self):
        xs, ys = np.meshgrid(np.arange(10) + 0.5, np.arange(10) + 0.5)
        xyz = np.column_stack([xs.ravel(), ys.ravel(), np.full(100, 10.0)])
        aspect = np.zeros(100)
        # too few pixels to be refitted, so the plane is dropped:
        labels = np.full(100, -1)
        labels[:2] = 0
        planes = {0: {'is_flat': True}}

        found = _assign_and_merge(xyz, aspect, labels, planes, np.ones(100, dtype=bool), 1.0, -1)

        self.assertEqual(found, [])


class SetSkewTest(unittest.TestCase):

    def test_skew_over_pixels_in_polygon(self):
        # a roof tilted 5 degrees more than its stored plane, beside pixels far off it that
        # are outside its polygon:
        xs, ys = np.meshgrid(np.arange(30) + 0.5, np.arange(10) + 0.5)
        xs, ys = xs.ravel(), ys.ravel()
        tilted_x_coef = np.tan(np.arctan(0.6) + np.radians(5))
        zs = np.where(xs < 20, xs * tilted_x_coef + 5, 100.0)
        pixels = [{"x": x, "y": y, "elevation": z} for x, y, z in zip(xs, ys, zs)]
        polygon = {"roof_geom_27700": box(0, 0, 20, 10), "x_coef": 0.6, "y_coef": 0.0, "intercept": 5.0}

        _set_skew([polygon], pixels, resolution_metres=1.0)

        self.assertEqual(polygon["skew"]["pixels"], 200)
        self.assertAlmostEqual(polygon["skew"]["tilt_err_deg"], 5, delta=0.1)


class TrimPlaneCoefficientsTest(unittest.TestCase):

    def test_ignores_mixed_eave_pixels(self):
        # a 6-pixel-deep 30 degree roof, whose eave row is mixed with the gutter and wall
        # below, 0.6m under the roof:
        xs, ys = np.meshgrid(np.arange(10) + 0.5, np.arange(6) + 0.5)
        xs, ys = xs.ravel(), ys.ravel()
        y_coef = np.tan(np.radians(30))
        zs = ys * y_coef + 5 - np.where(ys < 1, 0.6, 0.0)
        xyz = np.column_stack([xs, ys, zs])
        plane = {}
        set_plane_fit(plane, xyz[:, :2], zs, np.zeros(len(zs)), 1.0)
        self.assertGreater(abs(plane['slope'] - 30), 3)

        _trim_plane_coefficients(plane, xyz)

        self.assertAlmostEqual(plane['slope'], 30, delta=0.01)
        self.assertAlmostEqual(plane['y_coef'], y_coef, delta=1e-6)
        self.assertEqual(len(plane['inliers_xy']), 60)

    def test_keeps_least_squares_fit_if_most_pixels_are_off_it(self):
        xs, ys = np.meshgrid(np.arange(10) + 0.5, np.arange(6) + 0.5)
        xs, ys = xs.ravel(), ys.ravel()
        # ridges and furrows, which no plane fits:
        zs = np.where(ys % 2 < 1, 5.0, 6.0)
        xyz = np.column_stack([xs, ys, zs])
        plane = {}
        set_plane_fit(plane, xyz[:, :2], zs, np.zeros(len(zs)), 1.0)
        before = dict(plane)

        _trim_plane_coefficients(plane, xyz)

        self.assertEqual(plane['y_coef'], before['y_coef'])
