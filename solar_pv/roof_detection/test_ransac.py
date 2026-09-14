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
    _plane_metrics


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
