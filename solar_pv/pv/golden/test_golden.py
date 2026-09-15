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

from solar_pv.pv.golden import compare, horizon_check
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


@unittest.skip("port acceptance gate: enable once solar_pv.pv.run_pv and frozen goldens exist")
class PortAcceptanceTest(unittest.TestCase):
    """
    Turn this on in Phase 2. For each area with frozen goldens, run the port over the same
    inputs and assert the per-pixel yearly kWh raster matches within MAX_ABS_PC_YEAR.

    Sketch (fill in when run_pv lands):

        from solar_pv.pv.run_pv import run_pv_on_area   # future
        for area in AREAS:
            if not area.has_goldens():
                continue
            out = run_pv_on_area(area)                    # writes kwh_year.tif etc
            diff = compare.raster_diff(out.kwh_year, join(area.golden_dir, "kwh_year.tif"))
            self.assertTrue(diff.within(MAX_ABS_PC_YEAR), f"{area.name}: {diff}")
    """

    def test_placeholder(self):
        self.assertTrue(GOLDEN_ROOT)
        self.assertIsNotNone(compare.raster_diff)


if __name__ == "__main__":
    unittest.main()
