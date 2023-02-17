# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import json
from os.path import join
from typing import List

from solar_model.paths import TEST_DATA
from solar_model.solar_pv.outdated_lidar.outdated_lidar_check import _check_building
from solar_model.test_utils.test_funcs import ParameterisedTestCase

_PIXEL_DATA = join(TEST_DATA, "outdated_lidar")


def _load_data(filename: str) -> List[dict]:
    with open(filename) as f:
        return json.load(f)


def _check(filename: str):
    building = _load_data(join(_PIXEL_DATA, filename))
    return _check_building(building, resolution_metres=1.0)


class OutdatedLidarTestCase(ParameterisedTestCase):

    def test_lidar_checker(self):
        self.parameterised_test([
            ('osgb5000005219846721.json', 'NO_LIDAR_COVERAGE'),
            ('osgb5000005134753276.json', 'OUTDATED_LIDAR_COVERAGE'),
            ('osgb5000005152026792.json', 'OUTDATED_LIDAR_COVERAGE'),
            ('osgb5000005152026801.json', 'OUTDATED_LIDAR_COVERAGE'),
            ('osgb5000005235848378.json', 'OUTDATED_LIDAR_COVERAGE'),
            ('osgb5000005134753282.json', 'OUTDATED_LIDAR_COVERAGE'),
            ('osgb5000005135275129.json', 'OUTDATED_LIDAR_COVERAGE'),
            ('osgb5000005134753286.json', 'OUTDATED_LIDAR_COVERAGE'),
            ('osgb5000005134753270.json', 'OUTDATED_LIDAR_COVERAGE'),
            ('osgb5000005134753280.json', 'OUTDATED_LIDAR_COVERAGE'),
            ('osgb1000020005762.json', 'OUTDATED_LIDAR_COVERAGE'),
            ('osgb1000019929148.json', None),
            ('osgb1000043085584.json', None),
            ('osgb1000019927618.json', None),
            ('osgb1000020002707.json', None),
            ('osgb1000020002198.json', None),
            ('osgb1000043085181.json', None),
            ('osgb1000020002780.json', None),
            ("osgb5000005262593487.json", 'OUTDATED_LIDAR_COVERAGE'),
            ("osgb5000005262593494.json", 'OUTDATED_LIDAR_COVERAGE'),
            ("osgb5000005262592293.json", 'OUTDATED_LIDAR_COVERAGE'),
            ("osgb1000002085437860.json", 'OUTDATED_LIDAR_COVERAGE'),
            ("osgb1000021445343.json", None),
            ("osgb5000005150981943.json", None),
            ("osgb1000021445346.json", None),
            # Was throwing an exception in perimeter_gradient.py:
            ("osgb1000034157635.json", None),
            # Was throwing an exception in perimeter_gradient.py:
            ("osgb1000034135706.json", None),
        ], _check)
