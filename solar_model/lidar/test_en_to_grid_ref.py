# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
from solar_model.lidar.en_to_grid_ref import en_to_grid_ref, round_down_to, \
    is_in_range
from solar_model.test_utils.test_funcs import ParameterisedTestCase


class GridRefTest(ParameterisedTestCase):

    def test_en_to_grid_ref(self):
        self.parameterised_test([
            # Test mapping of 1st letter:
            (0, 0, 5000, 'SV00sw'),
            (500000, 0, 5000, 'TV00sw'),
            (0, 500000, 5000, 'NV00sw'),
            (500000, 500000, 5000, 'OV00sw'),
            (0, 1000000, 5000, 'HV00sw'),

            # Test mapping of 2nd letter:
            (0, 400000, 5000, 'SA00sw'),
            (100000, 400000, 5000, 'SB00sw'),
            (200000, 400000, 5000, 'SC00sw'),
            (300000, 400000, 5000, 'SD00sw'),
            (400000, 400000, 5000, 'SE00sw'),
            (0, 300000, 5000, 'SF00sw'),
            (100000, 300000, 5000, 'SG00sw'),
            (200000, 300000, 5000, 'SH00sw'),
            (300000, 300000, 5000, 'SJ00sw'),
            (400000, 300000, 5000, 'SK00sw'),
            (0, 200000, 5000, 'SL00sw'),
            (100000, 200000, 5000, 'SM00sw'),
            (200000, 200000, 5000, 'SN00sw'),
            (300000, 200000, 5000, 'SO00sw'),
            (400000, 200000, 5000, 'SP00sw'),
            (0, 100000, 5000, 'SQ00sw'),
            (100000, 100000, 5000, 'SR00sw'),
            (200000, 100000, 5000, 'SS00sw'),
            (300000, 100000, 5000, 'ST00sw'),
            (400000, 100000, 5000, 'SU00sw'),
            (0, 0, 5000, 'SV00sw'),
            (100000, 0, 5000, 'SW00sw'),
            (200000, 0, 5000, 'SX00sw'),
            (300000, 0, 5000, 'SY00sw'),
            (400000, 0, 5000, 'SZ00sw'),

            # Rest
            (50000, 40000, 5000, 'SV54sw'),
            (90000, 90000, 5000, 'SV99sw'),
            (5000, 0, 5000, 'SV00se'),
            (5000, 5000, 5000, 'SV00ne'),
            (0, 5000, 5000, 'SV00nw'),
            (460726, 212585, 5000, 'SP61sw'),

            # Other cell sizes:
            (0, 0, 10000, 'SV00'),
            (0, 0, 100000, 'SV'),
            (0, 0, 500000, 'S'),
            (456, 123, 500000, 'S'),
        ], en_to_grid_ref)

    def test_round_down_to(self):
        self.parameterised_test([
            (1_304_560, 500_000, 1_000_000),
            (399_560, 100_000, 300_000),
            (399_560, 10_000, 390_000),
            (399_560, 5_000, 395_000),
        ], round_down_to)

    def test_is_in_range(self):
        self.parameterised_test([
            (-1, -1, False),
            (0, 0, True),
            (0, 1_500_000, False),
            (0, 1_499_999, True),
            (999_999, 999_999, True),
            (1_000_000, 1_000_000, False),
        ], is_in_range)
