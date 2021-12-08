from albion_models.lidar.en_to_lidar_zip_id import en_to_lidar_zip_id, _round_down_to
from albion_models.test.test_funcs import ParameterisedTestCase


class GridRefTest(ParameterisedTestCase):

    def test_en_to_lidar_zip_id(self):
        self.parameterised_test([
            # Test mapping of 1st letter:
            (0, 0, 'SV00sw'),
            (500000, 0, 'TV00sw'),
            (0, 500000, 'NV00sw'),
            (500000, 500000, 'OV00sw'),
            (0, 1000000, 'HV00sw'),

            # Test mapping of 2nd letter:
            (0, 400000, 'SA00sw'),
            (100000, 400000, 'SB00sw'),
            (200000, 400000, 'SC00sw'),
            (300000, 400000, 'SD00sw'),
            (400000, 400000, 'SE00sw'),
            (0, 300000, 'SF00sw'),
            (100000, 300000, 'SG00sw'),
            (200000, 300000, 'SH00sw'),
            (300000, 300000, 'SJ00sw'),
            (400000, 300000, 'SK00sw'),
            (0, 200000, 'SL00sw'),
            (100000, 200000, 'SM00sw'),
            (200000, 200000, 'SN00sw'),
            (300000, 200000, 'SO00sw'),
            (400000, 200000, 'SP00sw'),
            (0, 100000, 'SQ00sw'),
            (100000, 100000, 'SR00sw'),
            (200000, 100000, 'SS00sw'),
            (300000, 100000, 'ST00sw'),
            (400000, 100000, 'SU00sw'),
            (0, 0, 'SV00sw'),
            (100000, 0, 'SW00sw'),
            (200000, 0, 'SX00sw'),
            (300000, 0, 'SY00sw'),
            (400000, 0, 'SZ00sw'),

            # Rest
            (50000, 40000, 'SV54sw'),
            (90000, 90000, 'SV99sw'),
            (5000, 0, 'SV00se'),
            (5000, 5000, 'SV00ne'),
            (0, 5000, 'SV00nw'),
            (460726, 212585, 'SP61sw'),
        ], en_to_lidar_zip_id)

    def test_round_down_to(self):
        self.parameterised_test([
            (1_304_560, 500_000, 1_000_000),
            (399_560, 100_000, 300_000),
            (399_560, 10_000, 390_000),
            (399_560, 5_000, 395_000),
        ], _round_down_to)
