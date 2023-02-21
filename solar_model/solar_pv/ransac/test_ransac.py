# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import csv
import unittest
from os.path import join
from typing import List

from solar_model.paths import TEST_DATA
from solar_model.solar_pv.ransac.run_ransac import _ransac_building

_RANSAC_DATA = join(TEST_DATA, "ransac")


def _load_data(filename: str) -> List[dict]:
    with open(filename) as f:
        return [{k: float(v) if k != 'pixel_id' else v for k, v in row.items()}
                for row in csv.DictReader(f)]


def _ransac(filename: str, res: float):
    return len(_ransac_building(_load_data(join(_RANSAC_DATA, filename)), filename, res, debug=False))


class RansacTestCase(unittest.TestCase):

    def parameterised_test(self, mapping: List[tuple], fn):
        for tup in mapping:
            with self.subTest():
                expected = tup[-1]
                inputs = tup[:-1]
                actual = fn(*inputs)
                if isinstance(expected, int):
                    assert expected == actual, f"\nExpected: {expected}\nActual  : {actual}\nInputs : {inputs}"
                else:
                    assert actual in expected, f"\nExpected: {expected}\nActual  : {actual}\nInputs : {inputs}"

    def test_ransac(self):
        self.parameterised_test([
            ('end_terrace.csv', 1.0, (3, 4)),
            ('all_one_plane.csv', 1.0, 1),
            ('osgb1000020002724.csv', 1.0, 3),
            ('osgb5000005156974578.csv', 1.0, (3, 4)),
            ('osgb1000020002610.csv', 1.0, 2),
            # Occasional flat roof detected where after final fit-to-plane, no points
            # are within the min distance:
            ('osgb1000011999905.csv', 1.0, (3, 4, 5, 6)),
        ], _ransac)
