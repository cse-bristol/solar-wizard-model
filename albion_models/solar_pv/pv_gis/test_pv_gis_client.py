import unittest
from typing import List

from albion_models.solar_pv.pv_gis.pv_gis_client import _easting_northing_to_lon_lat, _rad_to_deg, _PI


class PvGisClientTestCase(unittest.TestCase):

    def _parameterised_test(self, mapping: List[tuple], fn):
        for tup in mapping:
            expected = tup[-1]
            actual = fn(*tup[:-1])
            if isinstance(expected, float):
                assert abs(expected - actual) < 0.0000001, f"\nExpected: {expected}\nActual  : {actual}"
            else:
                assert expected == actual, f"\nExpected: {expected}\nActual  : {actual}"

    def test_easting_northing_to_lon_lat(self):
        self._parameterised_test([
            (249081, 75139, (-4.13191806007282, 50.55647199018528)),
        ], _easting_northing_to_lon_lat)

    def test_rad_to_deg(self):
        self._parameterised_test([
            (0, 0),
            ("0", 0),
            (str(_PI), 180.0),
            (str(_PI * 2), 360.0),
            (str(_PI / 2), 90.0),
        ], _rad_to_deg)
