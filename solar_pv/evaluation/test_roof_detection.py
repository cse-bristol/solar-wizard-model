# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import unittest

import numpy as np
from shapely import box

from solar_pv.evaluation.roof_detection import _agreement, _label, _coef_from_slope_aspect, _pixel_weighted_quantile, \
    oversplit_pairs
from solar_pv.roof_detection.assign_pixels import grid_index, _neighbour_pairs
from solar_pv.geos import slope_deg, aspect_deg


def _plane(geom, x_coef, y_coef, intercept, is_flat=False):
    return {"geom": geom, "coef": (x_coef, y_coef, intercept), "is_flat": is_flat}


class RoofDetectionEvalTest(unittest.TestCase):

    def test_label_takes_containing_polygon_and_best_fit_where_they_overlap(self):
        xyz = np.array([[0.5, 0.5, 1.0], [1.5, 0.5, 1.0], [2.5, 0.5, 2.0], [5.5, 0.5, 1.0]])
        planes = [_plane(box(0, 0, 3, 1), 0, 0, 1.0, True),   # z = 1
                  _plane(box(2, 0, 3, 1), 0, 0, 2.0, True)]   # z = 2, overlapping the 3rd pixel
        labels, residual = _label(xyz, planes)
        self.assertEqual(labels.tolist(), [0, 0, 1, -1])
        self.assertEqual(residual[:3].tolist(), [0.0, 0.0, 0.0])
        self.assertTrue(np.isnan(residual[3]))

    def test_agreement_matches_planes_by_overlap(self):
        a = np.array([0, 0, 0, 1, 1, -1])
        b = np.array([5, 5, 7, 6, 6, -1])   # same planes, renumbered, one pixel differing
        mask = np.ones(len(a), bool)
        self.assertAlmostEqual(_agreement(a, b, mask), 5 / 6)
        self.assertEqual(_agreement(a, a, mask), 1.0)
        self.assertEqual(_agreement(a, np.full(len(a), -1), mask), 1 / 6)

    def test_oversplit_pairs_counts_one_face_split_but_not_a_ridge(self):
        xs, ys = np.meshgrid(np.arange(10) + 0.5, np.arange(6) + 0.5)
        x, y = xs.ravel(), ys.ravel()
        halves = (x > 5).astype(int)
        lo, hi = _neighbour_pairs(grid_index(np.column_stack([x, y]), 1.0)[0])
        planes = [{"is_flat": False}, {"is_flat": False}]

        one_face = np.column_stack([x, y, 0.5 * y])
        self.assertEqual(len(oversplit_pairs(one_face, halves, planes, lo, hi)), 1)
        ridge = np.column_stack([x, y, 5 - 0.6 * np.abs(x - 5)])
        self.assertEqual(oversplit_pairs(ridge, halves, planes, lo, hi), [])

    def test_oversplit_pairs_does_not_count_a_small_plane_at_another_level(self):
        # a 40x10m flat roof, with a 2x2m flat plant top 0.3m above it: one plane through both
        # misses only 1% of their pixels, but all the plant top's
        xs, ys = np.meshgrid(np.arange(40) + 0.5, np.arange(10) + 0.5)
        x, y = xs.ravel(), ys.ravel()
        plant = (x > 19) & (x < 21) & (y > 4) & (y < 6)
        labels = plant.astype(int)
        lo, hi = _neighbour_pairs(grid_index(np.column_stack([x, y]), 1.0)[0])
        planes = [{"is_flat": True}, {"is_flat": True}]

        xyz = np.column_stack([x, y, np.where(plant, 10.3, 10.0)])
        self.assertEqual(oversplit_pairs(xyz, labels, planes, lo, hi), [])

    def test_coef_from_slope_aspect_inverts_slope_and_aspect(self):
        for a, b in [(0.6, 0.0), (-0.2, 0.5), (0.1, -0.4)]:
            x, y = _coef_from_slope_aspect(slope_deg(a, b), aspect_deg(a, b))
            self.assertAlmostEqual(x, a)
            self.assertAlmostEqual(y, b)

    def test_pixel_weighted_quantile(self):
        v = np.array([1.0, 2.0, 3.0])
        self.assertEqual(_pixel_weighted_quantile(v, np.array([1.0, 1.0, 10.0]), 0.5), 3.0)
        self.assertEqual(_pixel_weighted_quantile(v, np.array([10.0, 1.0, 1.0]), 0.5), 1.0)


if __name__ == '__main__':
    unittest.main()
