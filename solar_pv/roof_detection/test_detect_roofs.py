# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import csv
import json
import unittest
from os.path import join
from typing import List

import numpy as np
from shapely import wkt, box, LineString, STRtree

from solar_pv.paths import TEST_DATA
from solar_pv.roof_detection.detect_roofs import _detect_building_roof_planes, _create_adaptive_batches, _assign_and_merge, \
    _set_skew, _trim_plane_coefficients, _facing_share, _implausible, _set_footprint_aspect_gap, _finalise, \
    _snapped_aspect
from solar_pv.roof_detection.merge_adjacent import set_plane_fit
from solar_pv.roof_detection.plane_moves import Plane
from solar_pv.roof_detection.segment_planes import local_fits
from solar_pv.roof_detection.ransac import _FaceIndex
from solar_pv.roof_detection import rejections
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
# 0029, a small flat roof is found to be clean. Imagery says 0029 is [149], but mincut
# also keeps 141 and 321.
# In 0026, the 149-facing slope is two planes about 0.24m apart in height, which one plane
# fits poorly (MAE 0.14m).
_MINCUT_EXPECTED = {
    "0026": [59, 59, 149, 149, 239, 329],
    "0032": [82, 84, 168, 264, 264],
    "0029": [141, 149, 321],
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
    # Cotham school - messy flat roofs. Imagery says 0029 is [149] and 0028 is [], but
    # legacy's merge_adjacent absorbs messy pixels into planes before the mess check sees them.
    ("0029", 1.0, [141, 149, 321]),
    ("0028", 1.0, [63, 164, 254]),
    ("0033", 1.0, []),  # Cotham school - messy flat roof

    # Flat roofs:
    ("0023", 1.0, [138]),
    ("0017", 1.0, [141, 226, 226]),

    # warehouses etc:
    ("0022", 1.0, [10, 46, 100, 155, 190, 226, 335]),  # Motorbike shop
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


class FacingShareTest(unittest.TestCase):

    def _gable(self):
        # 30 degree faces either side of a ridge along y = 6, each 6 pixels deep:
        xs, ys = np.meshgrid(np.arange(10) + 0.5, np.arange(12) + 0.5)
        xs, ys = xs.ravel(), ys.ravel()
        zs = 10 - np.abs(ys - 6) * np.tan(np.radians(30))
        xyz = np.column_stack([xs, ys, zs])
        normals, rms = local_fits(xyz, 1.0)
        south = ys < 6
        plane = {}
        set_plane_fit(plane, xyz[south, :2], zs[south], np.zeros(np.count_nonzero(south)), 1.0)
        return plane, normals[rms <= 0.1], south[rms <= 0.1]

    def test_pixels_of_the_plane_face_it(self):
        plane, normals, south = self._gable()
        self.assertEqual(_facing_share(plane, normals[south]), 1.0)

    def test_pixels_of_another_face_do_not(self):
        plane, normals, south = self._gable()
        self.assertEqual(_facing_share(plane, normals[~south]), 0.0)

    def test_too_few_locally_planar_pixels_to_say(self):
        plane, normals, south = self._gable()
        self.assertEqual(_facing_share(plane, normals[~south][:2]), 1.0)


def _south_face(rotation_degrees: float, slope_degrees: float = 30, pixel_aspect: float = 180.0):
    """A roof facing south, on a footprint rotated anticlockwise by `rotation_degrees` from
    it, and its pixels' local fits. `pixel_aspect` is every pixel's aspect from the aspect
    raster."""
    from shapely import affinity
    xs, ys = np.meshgrid(np.arange(10) + 0.5, np.arange(6) + 0.5)
    xs, ys = xs.ravel(), ys.ravel()
    zs = ys * np.tan(np.radians(slope_degrees)) + 5
    xyz = np.column_stack([xs, ys, zs])
    plane = {}
    set_plane_fit(plane, xyz[:, :2], zs, np.full(len(zs), pixel_aspect), 1.0)
    footprint = affinity.rotate(box(0, 0, 10, 6), rotation_degrees, origin=(5, 3))
    normals, rms = local_fits(xyz, 1.0)
    return plane, footprint, normals[rms <= 0.1]


class FootprintAspectTest(unittest.TestCase):

    def test_plane_aligned_with_footprint_is_snapped(self):
        plane, footprint, normals = _south_face(5)
        self.assertIsNone(_implausible(plane, _FaceIndex.of(footprint), normals))
        self.assertEqual(plane['aspect'], 175)

    def test_plane_off_footprint_keeps_its_own_aspect_if_its_pixels_corroborate_it(self):
        plane, footprint, normals = _south_face(30)
        self.assertIsNone(_implausible(plane, _FaceIndex.of(footprint), normals))
        self.assertAlmostEqual(plane['aspect'], 180, delta=0.01)

    def test_plane_off_footprint_rejected_if_nothing_corroborates_it(self):
        plane, footprint, normals = _south_face(30)
        self.assertEqual(_implausible(plane, _FaceIndex.of(footprint), normals[:2]), rejections.NO_CLOSE_ASPECT)

    def test_shallow_plane_off_footprint_rejected(self):
        plane, footprint, normals = _south_face(30, slope_degrees=8)
        self.assertEqual(_implausible(plane, _FaceIndex.of(footprint), normals), rejections.NO_CLOSE_ASPECT)

    def test_plane_aligned_only_by_its_pixels_mean_aspect_keeps_its_own(self):
        # its pixels' aspects follow the footprint (which is at 60 degrees, and so 150), so
        # it's kept even though it's too shallow to be kept off the footprint:
        plane, footprint, normals = _south_face(30, slope_degrees=8, pixel_aspect=150.0)
        self.assertIsNone(_implausible(plane, _FaceIndex.of(footprint), normals))
        self.assertAlmostEqual(plane['aspect'], 180, delta=0.01)

    def test_snaps_to_a_further_face_near_its_own_aspect_before_one_near_its_pixels_mean_aspect(self):
        plane, _, _ = _south_face(0, pixel_aspect=150.0)
        # within 1m of its pixels, a face at 60 degrees (so at right angles to 150); 2.5m
        # from them, one at 90 (so at right angles to 180):
        faces = [LineString([(-0.5, -0.5), (-0.5 + 2 * np.sin(np.radians(60)), -0.5 + 2 * np.cos(np.radians(60)))]),
                 LineString([(2, -2), (8, -2)])]
        self.assertEqual(_snapped_aspect(plane, _FaceIndex(faces, STRtree(faces))), (180, True))

    def test_footprint_aspect_gap(self):
        plane, footprint, _ = _south_face(30)
        plane['is_flat'] = False
        _set_footprint_aspect_gap([plane], footprint)
        self.assertAlmostEqual(plane['footprint_aspect_gap'], 30, delta=1)


class SegmentReproposalTest(unittest.TestCase):

    def test_no_plane_is_removed_twice(self):
        # Proposing from the pixels of a plane just removed grows much the same plane, to be
        # removed again. These buildings each have planes removed while refining:
        for building_id in ("0015", "0032"):
            with self.subTest(building_id=building_id):
                rejected = []
                _detect_building_roof_planes(_load_data(join(_ROOFDET_DATA, f"{building_id}.json")), building_id,
                                             1.0, method="segment", rejected=rejected)
                removed = [set(map(tuple, np.round(r['inliers_xy'], 3).tolist())) for r in rejected
                           if r['rejection_reason'] in (rejections.UNPROFITABLE, rejections.TOO_FEW_PIXELS)]
                self.assertGreater(len(removed), 0)
                for i, pixels in enumerate(removed):
                    for earlier in removed[:i]:
                        self.assertLess(len(pixels & earlier), len(pixels) / 2)


class FinaliseTest(unittest.TestCase):

    def _finalise(self, xs, ys, zs, footprint) -> dict:
        xyz = np.column_stack([xs.ravel(), ys.ravel(), zs.ravel()])
        # proposed with another aspect, which isn't kept:
        planes = {0: Plane(coef=(0.0, 0.0, 0.0), is_flat=True, plane_type="segmented", plane_id="segmented_0",
                           xy=xyz[:, :2], proposed={'building_id': "0", 'aspect': 63})}
        return _finalise(xyz, np.full(len(xyz), 180.0), np.zeros(len(xyz), dtype=int), planes,
                         _FaceIndex.of(footprint), 1.0)[0]

    def test_pitched_plane_with_no_face_close_keeps_its_own_aspect(self):
        from shapely import affinity
        xs, ys = np.meshgrid(np.arange(10) + 0.5, np.arange(6) + 0.5)
        plane = self._finalise(xs, ys, ys * np.tan(np.radians(30)) + 5,
                               affinity.rotate(box(0, 0, 10, 6), 30, origin=(5, 3)))
        self.assertAlmostEqual(plane['aspect'], 180, delta=0.01)

    def test_flat_plane_with_no_face_near_faces_south(self):
        # in the middle of a large footprint, more than 10m from all its faces:
        xs, ys = np.meshgrid(np.arange(15, 25) + 0.5, np.arange(15, 25) + 0.5)
        plane = self._finalise(xs, ys, np.full(xs.shape, 10.0), box(0, 0, 40, 40))
        self.assertTrue(plane['is_flat'])
        self.assertEqual(plane['aspect'], 180)
