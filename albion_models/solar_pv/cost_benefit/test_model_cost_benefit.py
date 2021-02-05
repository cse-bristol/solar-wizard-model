import unittest
from typing import List

from albion_models.solar_pv.cost_benefit.model_cost_benefit import _irr, _npv


def _parameterised_test(mapping: List[tuple], fn, delta: float = 0.0001):
    for tup in mapping:
        expected = tup[-1]
        actual = fn(*tup[:-1])
        if isinstance(expected, float):
            assert abs(
                expected - actual) < delta, f"\nExpected: {expected}\nActual  : {actual}"
        else:
            assert expected == actual, f"\nExpected: {expected}\nActual  : {actual}"


class CostBenefitTestCase(unittest.TestCase):
    """See file testdata/npv_irr.ods for source of values"""
    def test_npv(self):
        _parameterised_test([
            (5, 0.035, 100, 1, 25, 12.8763093867687),
            (5, 0.035, 1700.34, 1, 350, -120.071668585238),
            (5, 0.035, 2000, 1, 1000, 2515.05237547075),
        ], _npv)

    def test_irr(self):
        _parameterised_test([
            (5, 100, 1, 25, 0.079308261160529),
            (5, 1700.34, 1, 350, 0.009673229205254),
            (5, 2000, 1, 1000, 0.410414965009418),
        ], _irr)
