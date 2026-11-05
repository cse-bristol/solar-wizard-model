# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import csv
import json
import unittest
from os.path import join
from typing import List

import numpy as np
from shapely import wkt, box, LineString

from solar_pv.paths import TEST_DATA
from solar_pv.roof_detection.detect_roofs import _detect_building_roof_planes, _create_adaptive_batches, \
    _set_skew, _facing_share, _implausible, _set_footprint_aspect_gap, _finalise, _snapped_aspect, _fit
from solar_pv.geos import slope_deg
from solar_pv.roof_detection.footprint import FaceIndex
from solar_pv.roof_detection.plane_metrics import set_plane_fit
from solar_pv.roof_detection.plane_moves import Plane
from solar_pv.roof_detection.segment_planes import local_fits
from solar_pv.roof_detection import rejections
from solar_pv.datatypes import RoofDetBuilding
from solar_pv.test_utils.test_funcs import ParameterisedTestCase

_ROOFDET_DATA = join(TEST_DATA, "roof_detection")


def _load_data(filename: str) -> RoofDetBuilding:
    with open(filename) as f:
        building = json.load(f)
        building['polygon'] = wkt.loads(building['polygon'])
    return building


def _roofdet(building_id: str, res: float):
    filename = f"{building_id}.json" if not building_id.endswith(".json") else building_id
    planes = _detect_building_roof_planes(_load_data(join(_ROOFDET_DATA, filename)), filename, res)
    return sorted([round(plane['aspect']) for plane in planes])


# What roof detection finds, as sorted aspects. These aren't ground truth: they're what it
# found when the cases were last reviewed, so a change to them needs checking against the
# LiDAR and imagery, not just accepting.
_ROOF_DETECTION_CASES = [
    # Tricky Totterdown terraces:
    ("0013", 1.0, [54, 234]),
    ("0008", 1.0, [54, 233]),
    ("0016", 1.0, [61, 240]),
    ("0003", 1.0, [57, 239]),
    ("0035", 1.0, [56, 238]),
    ("0021", 1.0, [57, 236]),
    ("0019", 1.0, [56, 236]),
    ("0031", 1.0, [57, 237]),
    ("0012", 1.0, [54, 236]),
    ("0030", 1.0, [54, 233]),
    ("0034", 1.0, [54, 234]),
    ("0025", 1.0, [52, 234]),
    ("0027", 1.0, [53, 231]),
    ("0006", 1.0, [59, 236, 236]),
    ("0018", 1.0, [56, 173, 237]),
    ("0009", 1.0, [58, 237]),
    ("0011", 1.0, [61, 236]),

    # Irregular buildings:
    ("0032", 1.0, [62, 84, 84, 168, 258, 264]),
    # ("0005", 1.0, [148, 148, 148, 148]),  # Totterdown Mosque - currently fails due to dome

    # Messy roofs - should find nothing
    # ("0015", 1.0, []),  # Totterdown pub - very messy non-flat roof - currently fails due to mess
    # Cotham school - messy flat roofs. Imagery says 0029 is [149], and 0028 and 0033 are
    # [], but small planes on them that the mess checks don't reject are kept.
    ("0029", 1.0, [61, 141, 151, 151, 235, 320]),
    ("0028", 1.0, [152, 156, 161, 167, 242]),
    ("0033", 1.0, [159, 167, 262, 336]),

    # Flat roofs:
    ("0023", 1.0, [138]),
    ("0017", 1.0, [141]),

    # warehouses etc:
    ("0022", 1.0, [10, 46, 155, 190, 226, 335]),  # Motorbike shop
    ("0020", 1.0, [144, 144, 324, 324]),

    # Various tricky buildings in Croyde
    ("0004", 1.0, [77, 166, 166, 167, 257, 346, 347, 347]),
    ("0001", 1.0, [155, 155, 334]),
    ("0010", 1.0, [58, 59, 238, 239]),
    ("0024", 1.0, [53, 73, 164, 253]),
    ("0002", 1.0, [71, 251, 251]),
    ("0007", 1.0, [0, 90, 90, 180, 215, 270]),
    ("0014", 1.0, [85, 175, 265, 355, 355]),
    ("0026", 1.0, [59, 149, 239, 239, 329]),

    # Only has 1 pixel of lidar coverage...:
    ("0036", 1.0, []),

    # one plane that covers the entire roof and building is cardinally-aligned, so
    # no pixels outside building:
    ("0037", 1.0, [182]),

    # Below 0 height:
    ("0038", 1.0, [207]),
    # Totally round:
    ("0039", 1.0, [60, 191, 243, 270, 316]),

    # Large and very messy: most regions fail the gates
    ("0040", 1.0, [3, 138, 183, 183, 183, 183, 183, 183, 183, 183, 183, 273, 313]),
]


class RoofDetTestCase(ParameterisedTestCase):

    def test_roof_detection(self):
        self.parameterised_test(_ROOF_DETECTION_CASES, _roofdet)

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
        self.assertIsNone(_facing_share(plane, normals[~south][:2]))


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
        self.assertIsNone(_implausible(plane, FaceIndex.of(footprint), normals))
        self.assertEqual(plane['aspect'], 175)

    def test_plane_off_footprint_keeps_its_own_aspect_if_its_pixels_corroborate_it(self):
        plane, footprint, normals = _south_face(30)
        self.assertIsNone(_implausible(plane, FaceIndex.of(footprint), normals))
        self.assertAlmostEqual(plane['aspect'], 180, delta=0.01)

    def test_plane_off_footprint_rejected_if_nothing_corroborates_it(self):
        plane, footprint, normals = _south_face(30)
        self.assertEqual(_implausible(plane, FaceIndex.of(footprint), normals[:2]), rejections.NO_CLOSE_ASPECT)

    def test_shallow_plane_off_footprint_rejected(self):
        plane, footprint, normals = _south_face(30, slope_degrees=8)
        self.assertEqual(_implausible(plane, FaceIndex.of(footprint), normals), rejections.NO_CLOSE_ASPECT)

    def test_plane_aligned_only_by_its_pixels_mean_aspect_keeps_its_own(self):
        # its pixels' aspects follow the footprint (which is at 60 degrees, and so 150), so
        # it's kept even though it's too shallow to be kept off the footprint:
        plane, footprint, normals = _south_face(30, slope_degrees=8, pixel_aspect=150.0)
        self.assertIsNone(_implausible(plane, FaceIndex.of(footprint), normals))
        self.assertAlmostEqual(plane['aspect'], 180, delta=0.01)

    def test_snaps_to_a_further_face_near_its_own_aspect_before_one_near_its_pixels_mean_aspect(self):
        plane, _, _ = _south_face(0, pixel_aspect=150.0)
        # within 1m of its pixels, a face at 60 degrees (so at right angles to 150); 2.5m
        # from them, one at 90 (so at right angles to 180):
        faces = [LineString([(-0.5, -0.5), (-0.5 + 2 * np.sin(np.radians(60)), -0.5 + 2 * np.cos(np.radians(60)))]),
                 LineString([(2, -2), (8, -2)])]
        self.assertEqual(_snapped_aspect(plane, FaceIndex.of_faces(faces)), (180, True))

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
                                             1.0, rejected=rejected)
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
        normals, rms = local_fits(xyz, 1.0)
        reliable_normals = np.where((rms <= 0.1)[:, None], normals, np.nan)
        return _finalise(xyz, np.full(len(xyz), 180.0), np.zeros(len(xyz), dtype=int), planes,
                         FaceIndex.of(footprint), 1.0, reliable_normals)[0]

    def test_pitched_plane_with_no_face_close_keeps_its_own_aspect(self):
        from shapely import affinity
        xs, ys = np.meshgrid(np.arange(10) + 0.5, np.arange(6) + 0.5)
        plane = self._finalise(xs, ys, ys * np.tan(np.radians(30)) + 5,
                               affinity.rotate(box(0, 0, 10, 6), 30, origin=(5, 3)))
        self.assertAlmostEqual(plane['aspect'], 180, delta=0.01)

    def test_orientation_leaves_out_mixed_pixels_within_the_residual_scale(self):
        # a 30 degree roof 6 pixels deep, whose eave row is mixed with the gutter below, 0.3m
        # under the roof: within the pitched residual scale of the least-squares plane it
        # tilts, so refining keeps it
        xs, ys = np.meshgrid(np.arange(10) + 0.5, np.arange(6) + 0.5)
        zs = ys * np.tan(np.radians(30)) + 5 - np.where(ys < 1, 0.3, 0.0)
        xyz = np.column_stack([xs.ravel(), ys.ravel(), zs.ravel()])
        self.assertGreater(abs(slope_deg(*_fit(xyz)[0][:2]) - 30), 0.5)

        plane = self._finalise(xs, ys, zs, box(0, 0, 10, 6))

        self.assertAlmostEqual(plane['slope'], 30, delta=0.01)
        self.assertEqual(len(plane['inliers_xy']), 60)
        # the fit metrics are of that plane, not the least-squares one:
        self.assertAlmostEqual(plane['mae'], 0.3 * 10 / 60, delta=1e-9)
        self.assertEqual(plane['score'], plane['mae'])
        # the eave row's pixels aren't locally planar, and the others face the plane's way:
        self.assertEqual(plane['facing_share'], 1.0)

    def test_flat_plane_with_no_face_near_faces_south(self):
        # in the middle of a large footprint, more than 10m from all its faces:
        xs, ys = np.meshgrid(np.arange(15, 25) + 0.5, np.arange(15, 25) + 0.5)
        plane = self._finalise(xs, ys, np.full(xs.shape, 10.0), box(0, 0, 40, 40))
        self.assertTrue(plane['is_flat'])
        self.assertEqual(plane['aspect'], 180)
