# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import unittest

import numpy as np

from solar_pv.constants import FLAT_ROOF_ROW_SPACING_SUN_ELEVATION_DEGREES as SUN
from solar_pv.evaluation import mounting_study as ms
from solar_pv.pv.aggregate_pixel_results import flat_roof_ground_cover_ratio


class MountingStudyTest(unittest.TestCase):

    def test_rows_on_a_flat_roof_spaced_as_the_model_spaces_them(self):
        self.assertAlmostEqual(1 / ms.row_pitch(10, 0), flat_roof_ground_cover_ratio(10))

    def test_row_in_front_reaches_the_spacing_sun_at_the_foot_of_the_panel(self):
        for fall in (-10, -5, 0, 5):
            pitch = ms.row_pitch(10, fall)
            self.assertAlmostEqual(float(ms.row_horizon(10, fall, pitch, 0, np.array([0.0]))[0]), SUN)

    def test_rows_rise_less_further_up_the_panel_and_to_the_side(self):
        pitch = ms.row_pitch(10, 0)
        h = ms.row_horizon(10, 0, pitch, 0.5, np.array([0.0, 60.0, 90.0, 180.0]))
        self.assertLess(h[0], SUN)
        self.assertLess(h[1], h[0])
        self.assertAlmostEqual(h[2], 0)
        self.assertEqual(h[3], 0)

    def test_rows_rising_roof_spaced_further_apart(self):
        self.assertGreater(ms.row_pitch(10, -5), ms.row_pitch(10, 0))
        self.assertLess(ms.row_pitch(10, 5), ms.row_pitch(10, 0))

    def test_no_rows_where_roof_falls_faster_than_tilt_or_rises_faster_than_sun(self):
        self.assertIsNone(ms.row_pitch(10, 12))
        self.assertIsNone(ms.row_pitch(10, -SUN))

    def test_fall_towards(self):
        self.assertAlmostEqual(ms.fall_towards(5, 0, 180), -5)
        self.assertAlmostEqual(ms.fall_towards(5, 180, 180), 5)
        self.assertAlmostEqual(ms.fall_towards(5, 90, 180), 0)

    def test_nearest_south_face(self):
        self.assertEqual(ms.nearest_south_face(0), 180)
        self.assertEqual(ms.nearest_south_face(30), 210)
        self.assertEqual(ms.nearest_south_face(100), 190)
        self.assertEqual(ms.nearest_south_face(225), 225)

    def test_panels_tilted_at_tilt_where_roof_only_falls_the_way_they_face(self):
        for roof_aspect in (0, 180):
            slope, aspect = ms.panel_orientation(5, roof_aspect, 180, 10)
            self.assertAlmostEqual(slope, 10)
            self.assertAlmostEqual(aspect, 180)
        self.assertEqual(ms.panel_orientation(2, 0, 180, 10), (10, 180))

    def test_panels_follow_a_roof_falling_across_them(self):
        slope, aspect = ms.panel_orientation(15, 45, 135, 10)
        # about the 15° roof and the 10° lift combined at right angles:
        self.assertAlmostEqual(slope, 18, delta=0.5)
        self.assertGreater(aspect, 45)
        self.assertLess(aspect, 135)

    def test_east_west_pairs(self):
        self.assertAlmostEqual(ms.ew_panel_area(0, 0), 1)
        self.assertLess(ms.ew_panel_area(10, 0.3), ms.ew_panel_area(10, 0.1))
        self.assertEqual(float(ms.ew_horizon(10, 0.3, 1, np.array([0.0]))[0]), 0)
        self.assertGreater(float(ms.ew_horizon(10, 0.3, 0, np.array([0.0]))[0]), 0)


if __name__ == '__main__':
    unittest.main()
