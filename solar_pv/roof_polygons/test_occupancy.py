# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import unittest

from shapely import wkt
from shapely.geometry import Point

from solar_pv.roof_polygons.occupancy import _rotate_valid

# A self-intersecting sliver beside another polygon, from a building in the roof detection dev
# sample, that make_valid(method='structure') fails on (GEOS 3.13):
_STRUCTURE_FAILS = wkt.loads(
    "MULTIPOLYGON (((357352.2507759008 172769.97745369116, 357352.66293540905 172769.74898994507, "
    "357351.7638808048 172768.12705250445, 357350.7867181465 172768.7501420239, "
    "357353.12698055955 172770.71197129984, 357353.4983066173 172771.02325191826, "
    "357352.2507759008 172769.97745369116)), "
    "((357365.24345565174 172818.2281659944, 357363.49421623745 172819.1977852349, "
    "357363.0094066172 172818.32316552775, 357359.5109277887 172820.26240400874, "
    "357360.4805470292 172822.01164342303, 357359.605927322 172822.49645304328, "
    "357360.09073694225 172823.37107275042, 357359.21611723513 172823.85588237067, "
    "357360.6705460959 172826.47974149208, 357363.2944052173 172825.02531263133, "
    "357363.77921483753 172825.89993233848, 357368.1523133732 172823.47588423724, "
    "357365.24345565174 172818.2281659944)))")


class RotateValidTest(unittest.TestCase):

    def test_valid_where_make_valid_structure_fails(self):
        valid = _rotate_valid(_STRUCTURE_FAILS, 0, Point(0, 0))
        self.assertTrue(valid.is_valid)
        self.assertAlmostEqual(valid.area, _STRUCTURE_FAILS.area, delta=0.1)
