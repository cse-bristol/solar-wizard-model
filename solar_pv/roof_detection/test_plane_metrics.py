# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import unittest

import numpy as np

from solar_pv.roof_detection.plane_metrics import min_thinness_ratio, set_plane_fit


def _grid(x0: int, x1: int, y0: int, y1: int) -> np.ndarray:
    xs, ys = np.meshgrid(np.arange(x0, x1) + 0.5, np.arange(y0, y1) + 0.5)
    return np.column_stack([xs.ravel(), ys.ravel()])


class SetPlaneFitTest(unittest.TestCase):

    def test_solid_rectangle(self):
        xy = _grid(0, 10, 0, 6)
        z = xy[:, 1] * np.tan(np.radians(30)) + 5
        plane = {}

        set_plane_fit(plane, xy, z, np.full(len(z), 180.0), 1.0)

        self.assertAlmostEqual(plane['slope'], 30, delta=1e-6)
        self.assertAlmostEqual(plane['aspect_raw'], 180, delta=1e-6)
        self.assertFalse(plane['is_flat'])
        self.assertAlmostEqual(plane['mae'], 0, delta=1e-9)
        self.assertEqual(plane['cv_hull_ratio'], 1.0)
        self.assertAlmostEqual(plane['aspect_circ_mean'], 180, delta=1e-6)
        self.assertAlmostEqual(plane['aspect_circ_sd'], 0, delta=1e-6)

    def test_concave_shape_is_smaller_than_its_hull(self):
        xy = _grid(0, 10, 0, 10)
        # a U, open to the north:
        xy = xy[(xy[:, 0] < 3) | (xy[:, 0] > 7) | (xy[:, 1] < 3)]
        plane = {}

        set_plane_fit(plane, xy, np.full(len(xy), 10.0), np.full(len(xy), 180.0), 1.0)

        self.assertTrue(plane['is_flat'])
        self.assertLess(plane['cv_hull_ratio'], 0.8)

    def test_sliver_is_thinner_than_a_square(self):
        def thinness(xy):
            plane = {}
            set_plane_fit(plane, xy, np.full(len(xy), 10.0), np.full(len(xy), 180.0), 1.0)
            return plane['thinness_ratio']

        self.assertLess(thinness(_grid(0, 50, 0, 2)), thinness(_grid(0, 10, 0, 10)))


class MinThinnessRatioTest(unittest.TestCase):

    def test_representative_sizes(self):
        self.assertEqual(min_thinness_ratio(10), 0.45)
        self.assertEqual(min_thinness_ratio(50), 0.4)
        self.assertEqual(min_thinness_ratio(100), 0.24)
        self.assertEqual(min_thinness_ratio(400), 0.2)
        self.assertEqual(min_thinness_ratio(600), 0.15)
        self.assertEqual(min_thinness_ratio(900), 0.10)
        self.assertEqual(min_thinness_ratio(5000), 0.07)

    def test_boundary_between_bands(self):
        self.assertEqual(min_thinness_ratio(50), 0.4)
        self.assertEqual(min_thinness_ratio(51), 0.24)

    def test_larger_planes_never_require_a_higher_ratio(self):
        ratios = [min_thinness_ratio(a) for a in range(0, 4000, 10)]
        self.assertEqual(ratios, sorted(ratios, reverse=True))
