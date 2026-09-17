# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import unittest

import numpy as np

from solar_pv.pv import slope_aspect as sa


class ApplyCorrectionTest(unittest.TestCase):

    def test_override_takes_precedence_where_present(self):
        slope = np.array([30.0, 30.0])
        aspect_compass = np.array([180.0, 180.0])
        # a usable plane covers pixel 0 (compass 90 = east), not pixel 1 (NaN):
        aspect_override = np.array([90.0, np.nan])
        slope_override = np.array([25.0, np.nan])
        sa_slope, sa_aspect = sa.apply_correction(
            slope, aspect_compass,
            aspect_override_compass_deg=aspect_override, slope_override_deg=slope_override)
        # pixel 0 uses the override (east); pixel 1 the base south:
        np.testing.assert_allclose(sa_aspect, [90.0, 180.0])
        np.testing.assert_allclose(sa_slope, [25.0, 30.0])

    def test_uncovered_pixel_keeps_base_slope_and_aspect(self):
        # a pixel no usable plane covers (override NaN) keeps its base slope/aspect unchanged -
        # the flat-roof pitch is baked into the override upstream, not applied here:
        slope = np.array([2.0])
        aspect_compass = np.array([90.0])       # east
        sa_slope, sa_aspect = sa.apply_correction(
            slope, aspect_compass,
            aspect_override_compass_deg=np.array([np.nan]), slope_override_deg=np.array([np.nan]))
        np.testing.assert_allclose(sa_aspect, [90.0])  # base east, not south
        np.testing.assert_allclose(sa_slope, [2.0])    # base slope, no flat-roof default


if __name__ == "__main__":
    unittest.main()
