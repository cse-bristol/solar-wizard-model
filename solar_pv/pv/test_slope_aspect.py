# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import unittest

import numpy as np

from solar_pv.pv import slope_aspect as sa


class ApplyCorrectionTest(unittest.TestCase):

    def test_flat_roof_default_without_override(self):
        # flat roof (slope below threshold) -> pitch=flat_roof_degrees, aspect=180 (S compass):
        slope = np.array([2.0, 30.0])          # first is flat, second pitched
        aspect_compass = np.array([90.0, 90.0])   # both nominally east
        sa_slope, sa_aspect = sa.apply_correction(
            slope, aspect_compass, flat_roof_degrees=10.0, flat_roof_threshold=5.0)
        np.testing.assert_allclose(sa_slope, [10.0, 30.0])
        np.testing.assert_allclose(sa_aspect, [180.0, 90.0])  # flat -> south, pitched keeps east

    def test_override_takes_precedence_where_present(self):
        slope = np.array([30.0, 30.0])
        aspect_compass = np.array([180.0, 180.0])
        # a usable plane covers pixel 0 (compass 90 = east), not pixel 1 (NaN):
        aspect_override = np.array([90.0, np.nan])
        slope_override = np.array([25.0, np.nan])
        sa_slope, sa_aspect = sa.apply_correction(
            slope, aspect_compass, 10.0, 5.0,
            aspect_override_compass_deg=aspect_override, slope_override_deg=slope_override)
        # pixel 0 uses the override (east); pixel 1 the base south:
        np.testing.assert_allclose(sa_aspect, [90.0, 180.0])
        np.testing.assert_allclose(sa_slope, [25.0, 30.0])

    def test_override_path_skips_flat_default(self):
        # with an override raster present, a flat pixel not covered by a plane keeps its base
        # aspect (no south default) - matching _apply_slope_aspect_correction's has-override branch:
        slope = np.array([2.0])
        aspect_compass = np.array([90.0])       # east
        _, sa_aspect = sa.apply_correction(
            slope, aspect_compass, 10.0, 5.0,
            aspect_override_compass_deg=np.array([np.nan]), slope_override_deg=np.array([np.nan]))
        np.testing.assert_allclose(sa_aspect, [90.0])  # base east, not south


if __name__ == "__main__":
    unittest.main()
