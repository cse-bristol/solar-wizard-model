# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import unittest

from solar_pv.constants import (
    ROOFDET_MAX_MAE,
    FLAT_ROOF_DEGREES_THRESHOLD,
    CONFIDENCE_FULL_FIT_MAE,
    CONFIDENCE_MAX_ASPECT_CIRC_SD,
    CONFIDENCE_ASPECT_FULL_SLOPE,
    CONFIDENCE_SHAPE_SPAN,
    CONFIDENCE_WEIGHTS,
    CONFIDENCE_SUB_SCORE_FLOOR,
    Z_P90,
    INTERANNUAL_GHI_COV,
)
from solar_pv.pv.aggregate_pixel_results import P90_DERATE
from solar_pv.pv import confidence
from solar_pv.roof_detection.ransac import _min_thinness_ratio


class SubScoreTest(unittest.TestCase):

    def test_fit_score_bounds(self):
        self.assertEqual(confidence._fit_score(CONFIDENCE_FULL_FIT_MAE), 1.0)
        self.assertEqual(confidence._fit_score(0.0), 1.0)
        self.assertAlmostEqual(confidence._fit_score(ROOFDET_MAX_MAE), 0.0)
        self.assertEqual(confidence._fit_score(ROOFDET_MAX_MAE + 1), 0.0)

    def test_fit_score_is_log_scaled(self):
        # the geometric midpoint of the ramp scores 0.5:
        mid = (CONFIDENCE_FULL_FIT_MAE * ROOFDET_MAX_MAE) ** 0.5
        self.assertAlmostEqual(confidence._fit_score(mid), 0.5)
        self.assertGreater(confidence._fit_score(0.1), confidence._fit_score(0.2))

    def test_aspect_score_flat_roof_is_full(self):
        self.assertEqual(confidence._aspect_score(1.4, slope=0, is_flat=False), 1.0)
        self.assertEqual(
            confidence._aspect_score(1.4, slope=FLAT_ROOF_DEGREES_THRESHOLD, is_flat=False), 1.0)
        # flat roofs carry the (steeper) panel tilt as their slope:
        self.assertEqual(confidence._aspect_score(1.4, slope=10, is_flat=True), 1.0)

    def test_aspect_score_bounds_at_full_strength(self):
        steep = CONFIDENCE_ASPECT_FULL_SLOPE + 10
        self.assertEqual(confidence._aspect_score(0.0, slope=steep, is_flat=False), 1.0)
        self.assertEqual(confidence._aspect_score(CONFIDENCE_MAX_ASPECT_CIRC_SD, slope=steep, is_flat=False), 0.0)
        self.assertEqual(confidence._aspect_score(CONFIDENCE_MAX_ASPECT_CIRC_SD + 1, slope=steep, is_flat=False), 0.0)

    def test_aspect_penalty_fades_in_with_slope(self):
        halfway = (FLAT_ROOF_DEGREES_THRESHOLD + CONFIDENCE_ASPECT_FULL_SLOPE) / 2
        self.assertAlmostEqual(
            confidence._aspect_score(CONFIDENCE_MAX_ASPECT_CIRC_SD, slope=halfway, is_flat=False), 0.5)
        # continuous across the flat-roof threshold:
        just_sloped = FLAT_ROOF_DEGREES_THRESHOLD + 0.01
        self.assertGreater(
            confidence._aspect_score(CONFIDENCE_MAX_ASPECT_CIRC_SD, slope=just_sloped, is_flat=False), 0.99)

    def test_shape_score_relative_to_area_minimum(self):
        for n_pixels in (15, 100, 400, 5000):
            min_ratio = _min_thinness_ratio(n_pixels)
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


class RoofPlaneConfidenceTest(unittest.TestCase):

    def test_good_roof_scores_high(self):
        meta = {"score": 0.08, "aspect_circ_sd": 0.1, "thinness_ratio": 0.9}
        score, sub = confidence.roof_plane_confidence(
            meta=meta, slope=35, is_flat=False, resolution=0.5, area_raw=95.0, area_grown=100.0)
        self.assertGreater(score, 0.85)
        self.assertEqual(set(sub), set(CONFIDENCE_WEIGHTS))

    def test_poor_roof_scores_low(self):
        meta = {"score": 0.9, "aspect_circ_sd": 1.4, "thinness_ratio": 0.2}
        score, _ = confidence.roof_plane_confidence(
            meta=meta, slope=35, is_flat=False, resolution=2.0, area_raw=40.0, area_grown=100.0)
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
