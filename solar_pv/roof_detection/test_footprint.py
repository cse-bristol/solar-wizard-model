# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import unittest

from solar_pv.roof_detection.footprint import closest_azimuth


class ClosestAzimuthTest(unittest.TestCase):

    def test_returns_nearest_within_threshold(self):
        self.assertEqual(closest_azimuth([10, 100, 200], 105, thresh=10), 100)

    def test_returns_none_when_nothing_within_threshold(self):
        self.assertIsNone(closest_azimuth([10, 100, 200], 105, thresh=3))

    def test_wraps_around_360(self):
        # 355 and 5 are 10 degrees apart across the 0/360 boundary:
        self.assertEqual(closest_azimuth([355], 5, thresh=15), 355)
        self.assertIsNone(closest_azimuth([355], 5, thresh=5))
