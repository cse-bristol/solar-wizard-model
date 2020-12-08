import unittest
from typing import List

from albion_models.solar_pv.pv_gis.flatten import flatten


class FlattenTestCase(unittest.TestCase):

    def _parameterised_test(self, mapping: List[tuple], fn):
        for tup in mapping:
            expected = tup[-1]
            actual = fn(*tup[:-1])
            assert expected == actual, f"\nExpected: {expected}\nActual  : {actual}"

    def test_flatten(self):
        self._parameterised_test([
            (None, {}),
            ({"test": "a"}, {"test": "a"}),
            ({"test": "a"}, {"test": "a"}),
            ({"test": "a", "t": [2, 3]}, {"test": "a", "t_0": 2, "t_1": 3}),
            ({
                'a': 1,
                'b': [2, 22, 222, 2222],
                'c': {'d': 4, 'e': [{'f': 6, 'g': 7}, {'h': 7, 'i': 8}]}
            }, {'a': 1, 'b_0': 2, 'b_1': 22, 'b_2': 222, 'b_3': 2222, 'c_d': 4, 'c_e_0_f': 6, 'c_e_0_g': 7, 'c_e_1_h': 7, 'c_e_1_i': 8}),
        ], flatten)
