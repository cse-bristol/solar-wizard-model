# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import unittest

import numpy as np

from solar_pv.pv import run_pv


class MonthlyWhAndAnnualTest(unittest.TestCase):

    def test_no_corrections_sums_by_days_in_month(self):
        # constant 100 Wh/day every month -> yearly = sum(100 * days) * 0.001.
        hpv = [np.full((2, 2), 100.0) for _ in range(12)]
        monthly_wh, kwh_year = run_pv.monthly_wh_and_annual(hpv)
        # monthly Wh unchanged (no wind/spectral):
        for wh in monthly_wh:
            np.testing.assert_allclose(wh, 100.0)
        total_days = sum(nd for _, _, _, nd in run_pv.MONTHLY_STEPS)  # 365
        np.testing.assert_allclose(kwh_year, 100.0 * total_days * 0.001)

    def test_wind_and_spectral_multiply(self):
        hpv = [np.full((1, 1), 200.0) for _ in range(12)]
        wind = [np.full((1, 1), 1.05) for _ in range(12)]
        spectral = [np.full((1, 1), 0.98) for _ in range(12)]
        monthly_wh, _ = run_pv.monthly_wh_and_annual(hpv, wind, spectral)
        np.testing.assert_allclose(monthly_wh[0], 200.0 * 1.05 * 0.98)

    def test_correction_gaps_default_to_one(self):
        hpv = [np.full((1, 3), 100.0) for _ in range(12)]
        # wind present but with a NaN gap, spectral absent (None) for all months:
        wind = [np.array([[1.1, np.nan, 0.9]]) for _ in range(12)]
        monthly_wh, _ = run_pv.monthly_wh_and_annual(hpv, wind, None)
        np.testing.assert_allclose(monthly_wh[0], [[110.0, 100.0, 90.0]])

    def test_twelve_months_required(self):
        self.assertEqual(len(run_pv.MONTHLY_STEPS), 12)
        self.assertEqual(sum(nd for _, _, _, nd in run_pv.MONTHLY_STEPS), 365)


if __name__ == "__main__":
    unittest.main()
