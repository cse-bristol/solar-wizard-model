# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import unittest

import numpy as np

from solar_pv.pv import run_pv
from solar_pv.pv.met_data import MonthMet

CSI_COEFFS = [1.000436, -0.017237, -0.040465, -0.004702, 0.000149, 0.000170, 0.000005, 0.035]


class _StubMet:
    """A MetData stand-in returning constant, gap-free met on the given grid (no tar / GDAL),
    so the compute_pv_fields wiring can be exercised without fixtures."""

    def __init__(self, shape):
        self._shape = shape

    def for_month(self, month: int) -> MonthMet:
        ones = np.ones(self._shape)
        return MonthMet(
            linke=3.0 * ones, cbh=1.0 * ones, cdh=1.0 * ones,
            temps8=np.full(self._shape + (8,), 10.0),
            wind=None, spectral=None)


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


class FieldArraysTest(unittest.TestCase):

    def test_keys_and_order_match_raster_tables(self):
        kwh = np.zeros((2, 2))
        monthly = [np.full((2, 2), i) for i in range(12)]
        horizon = np.zeros((2, 2, 8))
        fa = run_pv.field_arrays(kwh, monthly, horizon)
        keys = list(fa)
        self.assertEqual(keys[0], "kwh_year")
        self.assertEqual(keys[1:13], [f"month_{i:02d}_wh" for i in range(1, 13)])
        self.assertEqual(keys[13:], [f"horizon_{d:02d}" for d in range(8)])
        # aggregate splits pixel_fields as [0]=year, [1:13]=months, [13:]=horizon:
        self.assertEqual(len(keys), 1 + 12 + 8)

    def test_values_are_float32(self):
        fa = run_pv.field_arrays(np.zeros(3), [np.zeros(3)] * 12, np.zeros((3, 8)))
        self.assertTrue(all(a.dtype == np.float32 for a in fa.values()))


class ComputePvFieldsTest(unittest.TestCase):
    """Wiring of the whole-grid assembly (lat/lon + met + compute_pv + PixelFields packaging),
    with a stub met. Not an accuracy check — the golden tests cover the numerics."""

    def setUp(self):
        # a 4x3 grid somewhere in Great Britain (EPSG:27700), 1 m cells, north-up:
        self.shape = (4, 3)
        self.gt = (330000.0, 1.0, 0.0, 550000.0, 0.0, -1.0)
        self.n_dir = 8
        self.step = 360.0 / self.n_dir
        self.slope = np.full(self.shape, 30.0)
        self.aspect = np.full(self.shape, 180.0)  # compass south-facing
        self.elev = np.full(self.shape, 50.0)
        self.horizon = np.zeros(self.shape + (self.n_dir,))

    def test_returns_pixel_fields(self):
        pf = run_pv.compute_pv_fields(
            self.slope, self.aspect, self.elev, self.horizon, self.gt,
            _StubMet(self.shape), CSI_COEFFS, self.step)
        self.assertEqual(list(pf.values)[0], "kwh_year")
        self.assertEqual(len(pf.values), 1 + 12 + self.n_dir)
        n = self.shape[0] * self.shape[1]   # all pixels valid (all inputs finite)
        self.assertEqual(pf.rows.size, n)
        for arr in pf.values.values():
            self.assertEqual(arr.shape, (n,))
        # a south-facing 30-degree roof with clear sky generates a positive yearly total:
        self.assertTrue(np.all(pf.values["kwh_year"] > 0))

    def test_valid_mask_restricts_to_masked_pixels(self):
        valid = np.zeros(self.shape, dtype=bool)
        valid[1, 1] = True
        pf = run_pv.compute_pv_fields(
            self.slope, self.aspect, self.elev, self.horizon, self.gt,
            _StubMet(self.shape), CSI_COEFFS, self.step, valid=valid)
        self.assertEqual(pf.rows.size, 1)
        self.assertEqual((pf.rows[0], pf.cols[0]), (1, 1))
        self.assertTrue(np.isfinite(pf.values["kwh_year"][0]))


if __name__ == "__main__":
    unittest.main()
