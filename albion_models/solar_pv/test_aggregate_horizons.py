import unittest
from typing import List

from albion_models.solar_pv.aggregate_horizons import _avg_southerly_horizon_rads


class PolygonizeTestCase(unittest.TestCase):

    def test_avg_southerly_horizon_rads(self):
        self._parameterised_test([
            (4, '(h.horizon_slice_2) / 1'),
            (6, '(h.horizon_slice_2 + h.horizon_slice_3 + h.horizon_slice_4) / 3'),
            (8, '(h.horizon_slice_3 + h.horizon_slice_4 + h.horizon_slice_5) / 3'),
            (11, '(h.horizon_slice_4 + h.horizon_slice_5 + h.horizon_slice_6 + h.horizon_slice_7) / 4'),
            (16, '(h.horizon_slice_5 + h.horizon_slice_6 + h.horizon_slice_7 + h.horizon_slice_8 + h.horizon_slice_9 + h.horizon_slice_10 + h.horizon_slice_11) / 7'),
        ], _avg_southerly_horizon_rads)

    def _parameterised_test(self, mapping: List[tuple], fn):
        for tup in mapping:
            expected = tup[-1]
            actual = fn(*tup[:-1])
            assert expected == actual, f"\n{tup[:-1]}\nExpected: {expected}\nActual  : {actual}"
