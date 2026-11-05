# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import unittest

from solar_pv.constants import (
    FLAT_ROOF_DEGREES_THRESHOLD,
    CONFIDENCE_FULL_FIT_MAE,
    CONFIDENCE_ZERO_FIT_MAE,
    CONFIDENCE_ASPECT_FULL_SLOPE,
    CONFIDENCE_SHAPE_SPAN,
    CONFIDENCE_WEIGHTS,
    CONFIDENCE_SUB_SCORE_FLOOR,
    CONFIDENCE_MAX_FOOTPRINT_ASPECT_GAP,
    AZIMUTH_ALIGNMENT_THRESHOLD,
    Z_P90,
    INTERANNUAL_GHI_COV,
)
from solar_pv.pv.aggregate_pixel_results import P90_DERATE
from solar_pv.pv import confidence
from solar_pv.roof_detection.plane_metrics import min_thinness_ratio


class SubScoreTest(unittest.TestCase):

    def test_fit_score_bounds(self):
        self.assertEqual(confidence._fit_score(CONFIDENCE_FULL_FIT_MAE), 1.0)
        self.assertEqual(confidence._fit_score(0.0), 1.0)
        self.assertAlmostEqual(confidence._fit_score(CONFIDENCE_ZERO_FIT_MAE), 0.0)
        self.assertEqual(confidence._fit_score(CONFIDENCE_ZERO_FIT_MAE + 1), 0.0)

    def test_fit_score_is_log_scaled(self):
        # the geometric midpoint of the ramp scores 0.5:
        mid = (CONFIDENCE_FULL_FIT_MAE * CONFIDENCE_ZERO_FIT_MAE) ** 0.5
        self.assertAlmostEqual(confidence._fit_score(mid), 0.5)
        self.assertGreater(confidence._fit_score(0.1), confidence._fit_score(0.2))

    def test_facing_score_flat_roof_is_full(self):
        self.assertEqual(confidence._facing_score(0.0, slope=0), 1.0)
        self.assertEqual(confidence._facing_score(0.0, slope=FLAT_ROOF_DEGREES_THRESHOLD), 1.0)
        self.assertEqual(confidence._facing_score(None, slope=0), 1.0)

    def test_facing_score_at_full_strength(self):
        steep = CONFIDENCE_ASPECT_FULL_SLOPE + 10
        self.assertEqual(confidence._facing_score(1.0, slope=steep), 1.0)
        self.assertEqual(confidence._facing_score(0.0, slope=steep), 0.0)
        self.assertAlmostEqual(confidence._facing_score(0.7, slope=steep), 0.7)

    def test_facing_penalty_fades_in_with_slope(self):
        halfway = (FLAT_ROOF_DEGREES_THRESHOLD + CONFIDENCE_ASPECT_FULL_SLOPE) / 2
        self.assertAlmostEqual(confidence._facing_score(0.0, slope=halfway), 0.5)
        # continuous across the flat-roof threshold:
        just_sloped = FLAT_ROOF_DEGREES_THRESHOLD + 0.01
        self.assertGreater(confidence._facing_score(0.0, slope=just_sloped), 0.99)

    def test_shape_score_relative_to_area_minimum(self):
        for n_pixels in (15, 100, 400, 5000):
            min_ratio = min_thinness_ratio(n_pixels)
            self.assertEqual(confidence._shape_score(min_ratio, n_pixels), 0.0)
            self.assertAlmostEqual(
                confidence._shape_score(min_ratio + CONFIDENCE_SHAPE_SPAN / 2, n_pixels), 0.5)
            self.assertEqual(confidence._shape_score(1.0, n_pixels), 1.0)
        # the same thinness is more acceptable for a larger plane:
        self.assertGreater(confidence._shape_score(0.5, 400), confidence._shape_score(0.5, 15))

    def test_resolution_score_known_and_nearest(self):
        from solar_pv.constants import CONFIDENCE_RESOLUTION_SCORES
        for res, score in CONFIDENCE_RESOLUTION_SCORES.items():
            self.assertEqual(confidence._resolution_score(res), score)
        # an unknown resolution snaps to the nearest known one:
        self.assertEqual(confidence._resolution_score(0.4),
                         CONFIDENCE_RESOLUTION_SCORES[0.5])
        self.assertEqual(confidence._resolution_score(1.9),
                         CONFIDENCE_RESOLUTION_SCORES[2.0])

    def test_geom_agreement_score(self):
        self.assertEqual(confidence._geom_agreement_score(10.0, 10.0), 1.0)
        self.assertEqual(confidence._geom_agreement_score(5.0, 10.0), 0.5)
        self.assertEqual(confidence._geom_agreement_score(10.0, 0.0), 0.0)

    def test_footprint_score(self):
        steep = CONFIDENCE_ASPECT_FULL_SLOPE + 10
        self.assertEqual(confidence._footprint_score(None, slope=steep), 1.0)
        self.assertEqual(confidence._footprint_score(AZIMUTH_ALIGNMENT_THRESHOLD, slope=steep), 1.0)
        self.assertEqual(confidence._footprint_score(CONFIDENCE_MAX_FOOTPRINT_ASPECT_GAP, slope=steep), 0.0)
        halfway = (AZIMUTH_ALIGNMENT_THRESHOLD + CONFIDENCE_MAX_FOOTPRINT_ASPECT_GAP) / 2
        self.assertAlmostEqual(confidence._footprint_score(halfway, slope=steep), 0.5)
        # aspect means little on shallow roofs:
        self.assertEqual(confidence._footprint_score(CONFIDENCE_MAX_FOOTPRINT_ASPECT_GAP, slope=0), 1.0)


class CombineTest(unittest.TestCase):

    def test_all_ones_is_one(self):
        self.assertAlmostEqual(combine_all(1.0), 1.0)

    def test_all_equal_is_that_value(self):
        # a weighted geometric mean of a constant is that constant (weights sum to 1):
        self.assertAlmostEqual(combine_all(0.6), 0.6)

    def test_zero_sub_score_is_floored(self):
        scores = {k: 0.9 for k in CONFIDENCE_WEIGHTS}
        scores["fit"] = 0.0
        floored = dict(scores, fit=CONFIDENCE_SUB_SCORE_FLOOR)
        self.assertGreater(confidence.combine(scores), 0.0)
        self.assertAlmostEqual(confidence.combine(scores), confidence.combine(floored))

    def test_all_zero_is_the_floor(self):
        self.assertAlmostEqual(combine_all(0.0), CONFIDENCE_SUB_SCORE_FLOOR)

    def test_weights_sum_to_one(self):
        self.assertAlmostEqual(sum(CONFIDENCE_WEIGHTS.values()), 1.0)

    def test_unknown_sub_score_counts_neither_way(self):
        scores = {k: 0.6 for k in CONFIDENCE_WEIGHTS}
        self.assertAlmostEqual(confidence.combine(dict(scores, aspect=None)), 0.6)
        scores["fit"] = 0.3
        without = {k: v for k, v in scores.items() if k != "aspect"}
        expected = 1.0
        for k, v in without.items():
            expected *= v ** (CONFIDENCE_WEIGHTS[k] / (1 - CONFIDENCE_WEIGHTS["aspect"]))
        self.assertAlmostEqual(confidence.combine(dict(scores, aspect=None)), expected)


class RoofPlaneConfidenceTest(unittest.TestCase):

    def test_good_roof_scores_high(self):
        meta = {"score": 0.08, "facing_share": 1.0, "thinness_ratio": 0.9, "footprint_aspect_gap": 2.0}
        score, sub = confidence.roof_plane_confidence(
            meta=meta, slope=35, resolution=0.5, area_raw=95.0, area_grown=100.0)
        self.assertGreater(score, 0.85)
        self.assertEqual(set(sub), set(CONFIDENCE_WEIGHTS))

    def test_facing_share_is_scored(self):
        meta = {"score": 0.08, "thinness_ratio": 0.9, "facing_share": 0.9, "footprint_aspect_gap": None}
        _, sub = confidence.roof_plane_confidence(
            meta=meta, slope=35, resolution=1.0, area_raw=95.0, area_grown=100.0)
        self.assertAlmostEqual(sub["aspect"], 0.9)

    def test_unknown_facing_share_is_not_scored(self):
        meta = {"score": 0.08, "thinness_ratio": 0.9, "facing_share": None, "footprint_aspect_gap": None}
        _, sub = confidence.roof_plane_confidence(
            meta=meta, slope=35, resolution=1.0, area_raw=95.0, area_grown=100.0)
        self.assertIsNone(sub["aspect"])
        _, flat = confidence.roof_plane_confidence(
            meta=meta, slope=0, resolution=1.0, area_raw=95.0, area_grown=100.0)
        self.assertEqual(flat["aspect"], 1.0)

    def test_poor_roof_scores_low(self):
        meta = {"score": 0.9, "facing_share": 0.2, "thinness_ratio": 0.2, "footprint_aspect_gap": 40.0}
        score, _ = confidence.roof_plane_confidence(
            meta=meta, slope=35, resolution=2.0, area_raw=40.0, area_grown=100.0)
        self.assertLess(score, 0.3)


class P90Test(unittest.TestCase):

    def test_derate_below_one(self):
        self.assertLess(P90_DERATE, 1.0)
        self.assertAlmostEqual(P90_DERATE, 1 - Z_P90 * INTERANNUAL_GHI_COV)

    def test_p90_below_central(self):
        kwh_year = 3000.0
        self.assertLess(kwh_year * P90_DERATE, kwh_year)


def combine_all(value: float) -> float:
    return confidence.combine({k: value for k in CONFIDENCE_WEIGHTS})


if __name__ == "__main__":
    unittest.main()
