# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import json
from os.path import join

from solar_pv.outdated_lidar.perimeter_gradient import HeightAggregator
from solar_pv.paths import TEST_DATA
from solar_pv.outdated_lidar.outdated_lidar_check import _check_building
from solar_pv.test_utils.test_funcs import ParameterisedTestCase

_PIXEL_DATA = join(TEST_DATA, "outdated_lidar")


def _load_data(filename: str) -> dict:
    with open(filename) as f:
        return json.load(f)


def _check(filename: str):
    building = _load_data(join(_PIXEL_DATA, filename))
    return _check_building(building, resolution_metres=1.0)[0]

def _check_gh(filename: str):
    building = _load_data(join(_PIXEL_DATA, filename))
    return _check_building(building, resolution_metres=1.0)[1:]


def _height(filename: str):
    building = _load_data(join(_PIXEL_DATA, filename))
    height = HeightAggregator(building['pixels']).height()
    return round(height, 2) if height else height


class OutdatedLidarTestCase(ParameterisedTestCase):

    def test_lidar_checker(self):
        self.parameterised_test([
            ('0021.json', 'NO_LIDAR_COVERAGE'),
            ('0005.json', 'OUTDATED_LIDAR_COVERAGE'),
            ('0011.json', 'OUTDATED_LIDAR_COVERAGE'),
            ('0006.json', 'OUTDATED_LIDAR_COVERAGE'),
            ('0004.json', 'OUTDATED_LIDAR_COVERAGE'),
            ('0020.json', 'OUTDATED_LIDAR_COVERAGE'),
            ('0002.json', 'OUTDATED_LIDAR_COVERAGE'),
            ('0003.json', 'OUTDATED_LIDAR_COVERAGE'),
            ('0008.json', 'OUTDATED_LIDAR_COVERAGE'),
            ('0017.json', 'OUTDATED_LIDAR_COVERAGE'),
            ('0010.json', 'OUTDATED_LIDAR_COVERAGE'),
            ('0019.json', None),
            ('0001.json', None),
            ('0018.json', None),
            ('0015.json', None),
            ('0013.json', None),
            ('0007.json', None),
            ('0014.json', None),
            ("0009.json", 'OUTDATED_LIDAR_COVERAGE'),
            ("0012.json", 'OUTDATED_LIDAR_COVERAGE'),
            ("0016.json", 'OUTDATED_LIDAR_COVERAGE'),
            ("0023.json", 'OUTDATED_LIDAR_COVERAGE'),
            ("0022.json", None),
            ("0025.json", None),
            ("0024.json", None),
            # Was throwing an exception in perimeter_gradient.py:
            ("0027.json", None),
            # Was throwing an exception in perimeter_gradient.py:
            ("0026.json", None),
            # Has a pixel within, but none without. If there are no pixels in the moat
            # that aren't inside another building, we currently assume LiDAR is ok...
            ("no_without.json", None),
        ], _check)

    def test_height(self):
        self.parameterised_test([
            ("0026.json", 3.18),
            # Has a pixel within, but none without. If there are no pixels in the moat
            # that aren't inside another building, we can't know the height...
            ("no_without.json", None),
        ], _height)

    def test_ground_height(self):
        self.parameterised_test([
            ('0021.json', (None, None)),
            ('0005.json', (None, None)),
            ('0019.json', (129.8, 130.5)),
            ("0027.json", (134.7, 135.8)),
            ("0026.json", (6.0, 6.0)),
            # Has a pixel within, but none without:
            ("no_without.json", (None, None)),
        ], _check_gh)
