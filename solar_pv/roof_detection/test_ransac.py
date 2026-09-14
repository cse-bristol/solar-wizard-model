# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import math
import unittest

import numpy as np

from sklearn.linear_model import LinearRegression

from solar_pv.geos import square, slope_deg, aspect_deg
from solar_pv.roof_detection.ransac import _group_areas, _pixel_groups, \
    _exclude_unconnected, _min_thinness_ratio, closest_azimuth, \
    get_potential_aspects, _sample, _convex_hull_ratio, _thinness_ratio, _aspect_stats, \
    _plane_metrics, _evaluate_candidate, _FitContext, _Thresholds


class GroupAreasTest(unittest.TestCase):

    def test_counts_pixels_per_label_ignoring_background(self):
        groups = np.array([[0, 1, 1],
                           [0, 2, 0]])
        self.assertEqual(_group_areas(groups), {1: 2, 2: 1})

    def test_all_background_gives_empty_dict(self):
        self.assertEqual(_group_areas(np.zeros((2, 2), dtype=int)), {})


class PixelGroupsTest(unittest.TestCase):

    def test_contiguous_run_is_one_group(self):
        X = np.array([[0, 0], [0, 1], [0, 2]], dtype=float)
        _, num_groups = _pixel_groups(X, [0, 0], res=1)
        self.assertEqual(num_groups, 1)

    def test_separated_pixels_are_two_groups(self):
        X = np.array([[0, 0], [0, 1], [5, 5]], dtype=float)
        _, num_groups = _pixel_groups(X, [0, 0], res=1)
        self.assertEqual(num_groups, 2)


class ExcludeUnconnectedTest(unittest.TestCase):

    def test_keeps_only_the_largest_contiguous_group(self):
        # A run of 5 connected pixels, plus one isolated pixel far away:
        X = np.array([[0, 0], [0, 1], [0, 2], [0, 3], [0, 4], [10, 10]], dtype=float)
        inlier_mask = np.ones(len(X), dtype=bool)

        result = _exclude_unconnected(X, [0, 0], inlier_mask, res=1)

        self.assertEqual(result.tolist(), [True, True, True, True, True, False])

    def test_no_inliers_gives_all_false(self):
        X = np.array([[0, 0], [0, 1], [10, 10]], dtype=float)
        inlier_mask = np.zeros(len(X), dtype=bool)

        result = _exclude_unconnected(X, [0, 0], inlier_mask, res=1)

        self.assertFalse(result.any())

    def test_points_sharing_a_pixel_resolve_to_the_highest_index(self):
        # two points land on the same pixel; the original per-point loop lets the
        # highest-indexed one represent that pixel, so only it is kept:
        X = np.array([[0, 0], [0, 0]], dtype=float)
        inlier_mask = np.ones(len(X), dtype=bool)

        result = _exclude_unconnected(X, [0, 0], inlier_mask, res=1)

        self.assertEqual(result.tolist(), [False, True])


class MinThinnessRatioTest(unittest.TestCase):

    def test_representative_areas(self):
        self.assertEqual(_min_thinness_ratio(10), 0.45)
        self.assertEqual(_min_thinness_ratio(50), 0.4)
        self.assertEqual(_min_thinness_ratio(100), 0.24)
        self.assertEqual(_min_thinness_ratio(400), 0.2)
        self.assertEqual(_min_thinness_ratio(600), 0.15)
        self.assertEqual(_min_thinness_ratio(900), 0.10)
        self.assertEqual(_min_thinness_ratio(5000), 0.07)

    def test_boundary_between_bands(self):
        self.assertEqual(_min_thinness_ratio(50), 0.4)
        self.assertEqual(_min_thinness_ratio(51), 0.24)

    def test_larger_areas_never_require_a_higher_ratio(self):
        ratios = [_min_thinness_ratio(a) for a in range(0, 4000, 10)]
        self.assertEqual(ratios, sorted(ratios, reverse=True))


class ClosestAzimuthTest(unittest.TestCase):

    def test_returns_nearest_within_threshold(self):
        self.assertEqual(closest_azimuth([10, 100, 200], 105, thresh=10), 100)

    def test_returns_none_when_nothing_within_threshold(self):
        self.assertIsNone(closest_azimuth([10, 100, 200], 105, thresh=3))

    def test_wraps_around_360(self):
        # 355 and 5 are 10 degrees apart across the 0/360 boundary:
        self.assertEqual(closest_azimuth([355], 5, thresh=15), 355)
        self.assertIsNone(closest_azimuth([355], 5, thresh=5))


class GetPotentialAspectsTest(unittest.TestCase):

    def test_returns_nearby_edge_azimuth_and_its_perpendiculars(self):
        # Points hugging the (horizontal) bottom edge of an axis-aligned square.
        # The edge's azimuth is 90; get_potential_aspects also adds the 90/180/270
        # degree rotations, giving all four cardinal directions:
        poly = square(0, 0, 10)
        X = np.array([[2, 0.5], [3, 0.5], [4, 0.5]], dtype=float)

        azimuths = get_potential_aspects(X, poly)

        self.assertEqual(sorted(azimuths), [0, 90, 180, 270])


class ConvexHullRatioTest(unittest.TestCase):

    def test_solid_shape_fills_its_hull(self):
        groups = np.array([[1, 1, 1],
                           [1, 1, 1]])
        ratio, only_largest = _convex_hull_ratio(groups, largest=1, roof_plane_area=6)
        self.assertAlmostEqual(ratio, 1.0)
        self.assertTrue(only_largest.all())

    def test_concave_shape_is_smaller_than_its_hull(self):
        # a U-shape: its convex hull fills the whole 3x3 bounding box:
        groups = np.array([[1, 0, 1],
                           [1, 0, 1],
                           [1, 1, 1]])
        ratio, _ = _convex_hull_ratio(groups, largest=1, roof_plane_area=7)
        self.assertLess(ratio, 1.0)
        self.assertAlmostEqual(ratio, 7 / 9)

    def test_only_considers_the_largest_group(self):
        groups = np.array([[1, 1, 0, 2]])
        _, only_largest = _convex_hull_ratio(groups, largest=1, roof_plane_area=2)
        self.assertEqual(only_largest.tolist(), [[True, True, False, False]])


class ThinnessRatioTest(unittest.TestCase):

    def test_compact_shape_is_less_thin_than_a_sliver(self):
        square_shape = np.ones((4, 4), dtype=bool)
        sliver = np.ones((1, 8), dtype=bool)
        self.assertGreater(_thinness_ratio(square_shape, 16), _thinness_ratio(sliver, 8))


class AspectStatsTest(unittest.TestCase):

    def test_pixels_aligned_with_plane_give_zero_spread_and_difference(self):
        # plane rising towards +y (north) faces south, aspect_rad == pi;
        # pixel aspects all 180 degrees, so they agree with the plane:
        aspect = np.array([180.0, 180.0, 180.0])
        mask = np.array([True, True, True])
        circ_mean, circ_sd, diff = _aspect_stats(aspect, mask, x_coef=0, y_coef=1)
        self.assertAlmostEqual(circ_mean, math.pi)
        self.assertAlmostEqual(circ_sd, 0)
        self.assertAlmostEqual(diff, 0)

    def test_uses_only_masked_in_pixels(self):
        # the masked-out 999 would wreck the stats if it were included:
        aspect = np.array([180.0, 180.0, 999.0])
        mask = np.array([True, True, False])
        circ_mean, circ_sd, diff = _aspect_stats(aspect, mask, x_coef=0, y_coef=1)
        self.assertAlmostEqual(circ_mean, math.pi)
        self.assertAlmostEqual(circ_sd, 0)

    def test_difference_reflects_misalignment(self):
        # pixels face east (90 deg) but the plane faces south (180 deg): 90 deg apart:
        aspect = np.array([90.0, 90.0])
        mask = np.array([True, True])
        _, _, diff = _aspect_stats(aspect, mask, x_coef=0, y_coef=1)
        self.assertAlmostEqual(diff, math.pi / 2)


class PlaneMetricsTest(unittest.TestCase):

    def test_reports_coefficients_and_perfect_fit_metrics(self):
        # points lying exactly on the plane z = 2x + 3y + 5:
        X = np.array([[0, 0], [1, 0], [0, 1], [1, 1], [2, 3]], dtype=float)
        z = 2 * X[:, 0] + 3 * X[:, 1] + 5
        estimator = LinearRegression().fit(X, z)
        mask = np.ones(len(X), dtype=bool)

        metrics = _plane_metrics(estimator, X, z, mask, np.arange(len(X)))

        self.assertAlmostEqual(metrics["x_coef"], 2)
        self.assertAlmostEqual(metrics["y_coef"], 3)
        self.assertAlmostEqual(metrics["intercept"], 5)
        self.assertAlmostEqual(metrics["slope"], slope_deg(2, 3))
        self.assertAlmostEqual(metrics["aspect_raw"], aspect_deg(2, 3))
        self.assertFalse(metrics["is_flat"])
        # a perfect fit -> r2 of 1 and no error:
        self.assertAlmostEqual(metrics["r2"], 1.0)
        self.assertAlmostEqual(metrics["mae"], 0)
        self.assertAlmostEqual(metrics["rmse"], 0)

    def test_metrics_only_cover_the_masked_in_inliers(self):
        X = np.array([[0, 0], [1, 0], [0, 1], [99, 99]], dtype=float)
        z = 2 * X[:, 0] + 3 * X[:, 1] + 5
        estimator = LinearRegression().fit(X[:3], z[:3])
        mask = np.array([True, True, True, False])

        metrics = _plane_metrics(estimator, X, z, mask, np.arange(len(X)))

        self.assertEqual(len(metrics["inliers_xy"]), 3)


class EvaluateCandidateTest(unittest.TestCase):

    def _thresholds(self, **overrides):
        fields = dict(
            min_points_per_plane=8, min_points_per_plane_perc=0.001,
            min_convex_hull_ratio=0.65, max_aspect_circular_mean_degrees=90,
            max_aspect_circular_sd=1.5, resolution_metres=1.0)
        fields.update(overrides)
        return _Thresholds(**fields)

    def _ctx(self, thresholds, **overrides):
        fields = dict(
            thresholds=thresholds, X=None, y=None, aspect=None, polygon=None,
            min_X=None, sample_idxs=None, total_points_in_building=100,
            aspect_fallback_to_circ_mean=False)
        fields.update(overrides)
        return _FitContext(**fields)

    def _solid_block(self, n=8):
        # an n x n grid of pixels at 1m resolution: a compact, fully-connected region
        # that clears the min-points, largest-group, connectivity, convex-hull and
        # thinness checks, letting a test exercise the pipeline past them.
        xs, ys = np.meshgrid(np.arange(n), np.arange(n))
        return np.column_stack([xs.ravel(), ys.ravel()]).astype(float)

    def test_rejects_when_too_few_inliers(self):
        # the min-points check is the first thing _evaluate_candidate does, so the
        # rest of the context is never touched:
        ctx = self._ctx(self._thresholds())
        inlier_mask = np.zeros(20, dtype=bool)
        inlier_mask[:3] = True  # 3 < 8

        reason, cand = _evaluate_candidate(
            ctx, y_pred=None, residuals_subset=None, inlier_mask_subset=inlier_mask,
            coef=None, slope=0.0, score_best=float("inf"), n_inliers_best=1)

        self.assertEqual(reason, "MIN_POINTS_PER_PLANE")
        self.assertIsNone(cand)

    def test_rejects_when_largest_group_too_small(self):
        # enough raw inliers, but they're scattered so no connected group is big enough:
        X = np.array([[i * 5, 0] for i in range(10)], dtype=float)  # 10 isolated pixels
        ctx = self._ctx(self._thresholds(), X=X, min_X=[0.0, 0.0],
                        sample_idxs=np.arange(len(X)))
        inlier_mask = np.ones(len(X), dtype=bool)

        reason, cand = _evaluate_candidate(
            ctx, y_pred=None, residuals_subset=None, inlier_mask_subset=inlier_mask,
            coef=None, slope=0.0, score_best=float("inf"), n_inliers_best=1)

        self.assertEqual(reason, "MIN_POINTS_PER_LARGEST_GROUP")
        self.assertIsNone(cand)

    def test_rejects_a_worse_score(self):
        # a solid block that clears the morphology checks, but predicts badly (high MAE)
        # against an already-good best score -> rejected at the score gate:
        X = self._solid_block()
        n = len(X)
        ctx = self._ctx(self._thresholds(), X=X, y=np.zeros(n), min_X=[0.0, 0.0],
                        sample_idxs=np.arange(n), polygon=square(0, 0, 10),
                        aspect=np.full(n, 180.0))
        mask = np.ones(n, dtype=bool)

        reason, cand = _evaluate_candidate(
            ctx, y_pred=np.full(n, 100.0), residuals_subset=np.zeros(n),
            inlier_mask_subset=mask, coef=[0.001, 0.0], slope=0.0,
            score_best=0.0, n_inliers_best=1)

        self.assertEqual(reason, "WORSE_SCORE")
        self.assertIsNone(cand)

    def test_rejects_a_good_score_with_no_more_inliers(self):
        # a good score, but no more inliers than the current best -> we don't optimise
        # for point count (Tarsha-Kurdi), so it's rejected:
        X = self._solid_block()
        n = len(X)
        ctx = self._ctx(self._thresholds(), X=X, y=np.zeros(n), min_X=[0.0, 0.0],
                        sample_idxs=np.arange(n), polygon=square(0, 0, 10),
                        aspect=np.full(n, 180.0))
        mask = np.ones(n, dtype=bool)

        reason, cand = _evaluate_candidate(
            ctx, y_pred=np.zeros(n), residuals_subset=np.zeros(n),
            inlier_mask_subset=mask, coef=[0.001, 0.0], slope=0.0,
            score_best=0.0, n_inliers_best=1000)

        self.assertEqual(reason, "LESS_INLIERS")
        self.assertIsNone(cand)

    def test_accepts_a_viable_candidate(self):
        # a compact, well-fitting flat block hugging the polygon edges: passes every
        # check and comes back as a _Candidate aligned to the 180-degree face:
        X = self._solid_block()
        n = len(X)
        ctx = self._ctx(self._thresholds(), X=X, y=np.zeros(n), min_X=[0.0, 0.0],
                        sample_idxs=np.arange(n), polygon=square(0, 0, 10),
                        aspect=np.full(n, 180.0))
        mask = np.ones(n, dtype=bool)

        reason, cand = _evaluate_candidate(
            ctx, y_pred=np.zeros(n), residuals_subset=np.zeros(n),
            inlier_mask_subset=mask, coef=[0.001, 0.0], slope=0.0,
            score_best=float("inf"), n_inliers_best=1)

        self.assertIsNone(reason)
        self.assertEqual(cand.n_inliers, n)
        self.assertEqual(cand.aspect, 180)
        self.assertEqual(cand.score, 0.0)
        self.assertEqual(int(cand.inlier_mask.sum()), n)
        self.assertEqual(cand.plane_properties("RANSAC", "id-1")["plane_id"], "id-1")


class SampleTest(unittest.TestCase):

    def test_draws_distinct_in_range_indices_where_mask_allows(self):
        rng = np.random.RandomState(42)
        mask = np.ones(10, dtype=bool)

        sample = _sample(10, 3, random_state=rng, mask=mask)

        self.assertEqual(len(sample), 3)
        self.assertEqual(len(set(sample.tolist())), 3)
        self.assertTrue(all(0 <= i < 10 for i in sample))

    def test_only_samples_from_masked_in_positions(self):
        rng = np.random.RandomState(0)
        mask = np.zeros(10, dtype=bool)
        mask[:5] = True

        for _ in range(50):
            sample = _sample(10, 3, random_state=rng, mask=mask)
            self.assertTrue(all(mask[i] for i in sample))

    def test_returns_none_when_not_enough_masked_in_points(self):
        rng = np.random.RandomState(0)
        mask = np.zeros(10, dtype=bool)
        mask[:2] = True  # only 2 valid, but 3 requested

        self.assertIsNone(_sample(10, 3, random_state=rng, mask=mask))


if __name__ == "__main__":
    unittest.main()
