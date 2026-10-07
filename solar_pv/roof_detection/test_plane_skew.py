# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import unittest

import numpy as np

from solar_pv.geos import slope_deg, aspect_deg
from solar_pv.roof_detection.plane_skew import plane_skew


def _roof(x_coef: float, noise: float = 0.03, step_share: float = 0.0, seed: int = 0):
    """A 20x10m roof z = x_coef * x + 5, with the first `step_share` of pixels raised 1.5m."""
    rng = np.random.default_rng(seed)
    x, y = np.meshgrid(np.arange(20.0), np.arange(10.0))
    x, y = x.ravel(), y.ravel()
    z = x_coef * x + 5 + rng.normal(0, noise, len(x))
    z[:int(step_share * len(x))] += 1.5
    return np.column_stack([x, y, z])


def _tilted(x_coef: float, degrees: float) -> float:
    return np.tan(np.arctan(x_coef) + np.radians(degrees))


class PlaneSkewTest(unittest.TestCase):

    def test_matching_plane_has_no_skew(self):
        skew = plane_skew(_roof(0.6), (0.6, 0.0, 5.0), 1.0)
        self.assertLess(skew["tilt_err_deg"], 0.2)
        self.assertLess(abs(skew["offset_m"]), 0.01)

    def test_recovers_tilt_despite_pixels_off_the_plane(self):
        for step_share in (0.0, 0.1, 0.2):
            stored = _tilted(0.6, 3)
            skew = plane_skew(_roof(0.6, step_share=step_share), (stored, 0.0, 5.0), 1.0)
            self.assertAlmostEqual(skew["tilt_err_deg"], 3, delta=0.2, msg=step_share)
            self.assertAlmostEqual(skew["outlier_share"], step_share, delta=0.03)
            self.assertGreater(skew["tilt_z"], 10)

    def test_offset_edge_row_of_small_plane_isnt_a_significant_tilt(self):
        # a plane 4 pixels deep whose eave row is 10cm low (gutter/wall in the pixels), with
        # the stored plane fitted without it: fitting two rows exactly would give a tilt
        # that looked certain
        rng = np.random.default_rng(0)
        x, y = np.meshgrid(np.arange(4.0), np.arange(8.0))
        x, y = x.ravel(), y.ravel()
        z = 0.7 * x + 5 + rng.normal(0, 0.02, len(x))
        z[x == 0] -= 0.1
        skew = plane_skew(np.column_stack([x, y, z]), (0.7, 0.0, 5.0), 1.0)
        self.assertEqual(skew["bands"], 4)
        self.assertLess(skew["tilt_z"], 3)

    def test_no_significance_below_min_bands(self):
        x, y = np.meshgrid(np.arange(2.0), np.arange(10.0))
        xyz = np.column_stack([x.ravel(), y.ravel(), 0.6 * x.ravel() + 5])
        skew = plane_skew(xyz, (0.5, 0.0, 5.0), 1.0)
        self.assertEqual(skew["bands"], 2)
        self.assertIsNone(skew["tilt_z"])

    def test_bands_are_pixels(self):
        # the same 4-pixel-deep plane at 50cm: 4 bands, not 2
        x, y = np.meshgrid(np.arange(4.0) * 0.5, np.arange(10.0) * 0.5)
        noise = np.random.default_rng(0).normal(0, 0.02, x.size)
        xyz = np.column_stack([x.ravel(), y.ravel(), 0.6 * x.ravel() + 5 + noise])
        skew = plane_skew(xyz, (0.6, 0.0, 5.0), 0.5)
        self.assertEqual(skew["bands"], 4)
        self.assertIsNotNone(skew["tilt_z"])

    def test_contamination(self):
        skew = plane_skew(_roof(0.6, step_share=0.1), (0.6, 0.0, 5.0), 1.0)
        self.assertLess(skew["tilt_err_deg"], 0.2)
        self.assertGreater(skew["contamination_deg"], 2)

    def test_implied_orientation(self):
        skew = plane_skew(_roof(0.6), (_tilted(0.6, 5), 0.0, 5.0), 1.0)
        self.assertAlmostEqual(skew["implied_slope"], slope_deg(0.6, 0), delta=0.2)
        self.assertAlmostEqual(skew["implied_aspect"], aspect_deg(0.6, 0), delta=0.5)

    def test_too_few_or_too_narrow(self):
        self.assertIsNone(plane_skew(_roof(0.6)[:5], (0.6, 0.0, 5.0), 1.0))
        strip = _roof(0.6)[:20]  # one row: no extent in y
        self.assertIsNone(plane_skew(strip, (0.6, 0.0, 5.0), 1.0))


if __name__ == '__main__':
    unittest.main()
