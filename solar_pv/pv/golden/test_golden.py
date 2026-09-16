# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
"""
Phase 0 golden validation (docs/pv-grass-removal-plan.md).

Two kinds of test:

1. Fixture sanity (runs now): the committed validation-area inputs load and the cached
   PVGIS API ground truth is internally consistent. Guards the harness itself.

2. Port acceptance gate (skipped until the port + frozen goldens exist): the native-Python
   PV port must match the frozen GRASS/PVMAPS golden rasters to a tight tolerance. This is
   the real pass/fail gate for the project.
"""
import unittest

from solar_pv.pv.golden import compare, horizon_check, rpv_check
from solar_pv.pv.golden.fixtures import AREAS, GOLDEN_ROOT

# The tight target agreed 2026-09-15 (yearly kWh, port vs frozen PVMAPS golden):
MAX_ABS_PC_YEAR = 2.0
# Monthly Wh vary more; near-zero winter months make percentages noisy, so pair the
# percentage bound with a small floor and lean on absolute stats during bring-up.
MAX_ABS_PC_MONTH = 5.0

# Phase 1 horizon tolerances (port vs frozen r.horizonmask goldens, on a shared grid).
# Observed worst across areas: mean 0.04 deg, p99 0.00 deg, 0.13% of pixels > 5 deg (the
# only differences are near-obstruction diagonal nearest-neighbour ties). Thresholds sit
# well above that to catch regressions without flapping.
HORIZON_MAX_MEAN_DEG = 0.10
HORIZON_MAX_P99_DEG = 0.50
HORIZON_MAX_PCT_OVER_5DEG = 0.40


class FixtureSanityTest(unittest.TestCase):
    def test_areas_have_inputs(self):
        for area in AREAS:
            with self.subTest(area=area.name):
                self.assertTrue(area.has_inputs(),
                                f"{area.name}: missing inputs under {area.input_dir}")

    def test_api_ground_truth_is_consistent(self):
        found_any = False
        for area in AREAS:
            gt = area.load_api_ground_truth()
            if gt is None:
                continue
            found_any = True
            points, api_results = gt
            with self.subTest(area=area.name):
                self.assertEqual(len(points), len(api_results))
                for e_day, e_year in api_results:
                    self.assertEqual(len(e_day), 12)
                    self.assertGreater(e_year, 0.0)
                    self.assertAlmostEqual(sum(e_day) * 30.4, e_year, delta=e_year,
                                           msg=f"{area.name}: monthly totals implausible vs yearly")
        self.assertTrue(found_any, "no area had a cached PVGIS API ground truth")


class HorizonPortTest(unittest.TestCase):
    """Phase 1 acceptance gate: the native-Python horizon port reproduces r.horizonmask."""

    def test_horizon_matches_golden(self):
        checked = 0
        for area in AREAS:
            if not area.has_goldens():
                continue
            checked += 1
            stats = horizon_check.check_area(area)
            with self.subTest(area=area.name):
                self.assertLess(stats.mean_deg, HORIZON_MAX_MEAN_DEG, str(stats))
                self.assertLess(stats.p99_deg, HORIZON_MAX_P99_DEG, str(stats))
                self.assertLess(stats.pct_over_5deg, HORIZON_MAX_PCT_OVER_5DEG, str(stats))
        self.assertGreater(checked, 0, "no areas had frozen horizon goldens to check")


class RpvCorePortTest(unittest.TestCase):
    """Phase 2: the r.pv port reproduces GRASS r.pv on identical inputs (per representative
    day), for every area with a frozen r.pv reference. The isolated maths match to ~machine
    precision bar a few shadow-boundary nearest-neighbour ties."""

    def test_rpv_matches_reference(self):
        checked = 0
        for area in AREAS:
            if not rpv_check.has_rpv_reference(area):
                continue
            for day, month in rpv_check.MONTHLY_DAYS:
                checked += 1
                stats = rpv_check.check_day(area, day, month)
                with self.subTest(area=area.name, day=day):
                    self.assertLess(stats.mean_pc, 0.05, str(stats))
                    self.assertLess(stats.p99_pc, 0.1, str(stats))
                    self.assertGreaterEqual(stats.pct_within_2, 99.5, str(stats))
        self.assertGreater(checked, 0, "no area had a frozen r.pv reference to check")


class AnnualPortTest(unittest.TestCase):
    """Phase 2 end-to-end: the full port (r.pv over 12 months + met sampled from
    pvgis_data_uk.tar + wind/spectral + annual sum) reproduces the kwh_year golden, given the
    GRASS-adjusted slope/aspect + horizon. Isolates everything except slope/aspect derivation
    (which production supplies via GDAL, as it does today)."""

    def test_kwh_year_matches_golden(self):
        if not rpv_check.has_met_tar():
            self.skipTest("pvgis_data_uk.tar not present")
        checked = 0
        for area in AREAS:
            if not rpv_check.has_rpv_reference(area):
                continue
            checked += 1
            stats = rpv_check.check_annual(area)
            with self.subTest(area=area.name):
                self.assertLess(stats.mean_pc, MAX_ABS_PC_YEAR, str(stats))
                self.assertGreaterEqual(stats.pct_within_2, 99.5, str(stats))
        self.assertGreater(checked, 0, "no area had a frozen r.pv reference to check")


if __name__ == "__main__":
    unittest.main()
