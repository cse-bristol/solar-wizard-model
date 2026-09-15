# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import unittest

import numpy as np

from solar_pv.roof_detection.premade_planes import (
    _image, _segment_sizes, _merge_small_segments, _dbscan, roughness,
    _slic_compactness, _segment, _BASE_COMPACTNESS)


class ImageTest(unittest.TestCase):

    def test_places_values_at_scaled_coordinates_and_flips_y(self):
        # points on a 2x2 grid; _image rasters them and flips the y axis so row 0 is
        # the top (highest y), the raster convention:
        xy = np.array([[0, 0], [1, 0], [0, 1]], dtype=float)
        vals = np.array([10, 20, 30], dtype=float)
        image, idxs = _image(xy, vals, res=1, nodata=0.0)
        np.testing.assert_array_equal(image, [[30, 0], [10, 20]])
        # idxs maps each cell back to the index of the point that filled it:
        np.testing.assert_array_equal(idxs, [[2, 0], [0, 1]])

    def test_respects_resolution(self):
        # at res=2 the two points one metre apart land in the same cell:
        xy = np.array([[0, 0], [1, 0]], dtype=float)
        vals = np.array([5, 7], dtype=float)
        image, _ = _image(xy, vals, res=2, nodata=0.0)
        self.assertEqual(image.shape, (1, 1))


class SegmentSizesTest(unittest.TestCase):

    def test_counts_pixels_per_segment_id(self):
        seg = np.array([[0, 1, 1], [0, 2, 0]])
        self.assertEqual(_segment_sizes(seg), [(0, 3), (1, 2), (2, 1)])


class MergeSmallSegmentsTest(unittest.TestCase):

    def test_absorbs_segments_at_or_below_max_size_into_neighbours(self):
        # segment 2 is a single pixel; with max_size=1 it is dissolved and its pixel
        # is grown into from the surrounding segment 1:
        seg = np.array([[1, 1, 2], [1, 1, 1]])
        merged = _merge_small_segments(seg, max_size=1)
        np.testing.assert_array_equal(merged, [[1, 1, 1], [1, 1, 1]])

    def test_keeps_nodata_as_zero(self):
        # the 0 (nodata) cell must stay 0, not be grown into:
        seg = np.array([[1, 1, 0], [1, 1, 1]])
        merged = _merge_small_segments(seg, max_size=1)
        self.assertEqual(merged[0, 2], 0)


class DbscanTest(unittest.TestCase):

    def test_two_separated_clusters(self):
        # two tight clusters of 5 (>= min_samples) far apart in z:
        z = np.array([0.0, 0.1, 0.2, 0.1, 0.05, 10.0, 10.1, 10.2, 10.05, 9.95])
        labels = _dbscan(z)
        # no noise, two clusters of five, and label 0 has been reassigned away (0 is
        # reserved as nodata downstream):
        self.assertNotIn(-1, labels)
        self.assertNotIn(0, labels)
        _, counts = np.unique(labels, return_counts=True)
        self.assertEqual(sorted(counts.tolist()), [5, 5])


class RoughnessTest(unittest.TestCase):

    def test_uniform_image_is_perfectly_smooth(self):
        np.testing.assert_allclose(roughness(np.full((4, 4), 137.0)), 0.0)

    def test_uses_angular_difference_so_it_wraps_at_360(self):
        # 90 and 270 are each 90 degrees from 0 the short way round, so a 0/90 pattern
        # and a 0/270 pattern must be equally rough - a plain difference would not be:
        a = np.array([[0.0, 90.0], [90.0, 0.0]])
        b = np.array([[0.0, 270.0], [270.0, 0.0]])
        np.testing.assert_allclose(roughness(a), roughness(b))
        self.assertGreater(roughness(a).max(), 0)


class SlicCompactnessTest(unittest.TestCase):

    def test_scales_inversely_with_the_value_range(self):
        # an aspect raster (0-360) gets a much smaller compactness than a [0, 1] one,
        # cancelling skimage >=0.19's internal [0, 1] rescale:
        aspect = np.array([[0.0, 360.0], [180.0, 90.0]])
        self.assertAlmostEqual(_slic_compactness(aspect), _BASE_COMPACTNESS / 360)
        unit = np.array([[0.0, 1.0], [0.5, 0.25]])
        self.assertAlmostEqual(_slic_compactness(unit), _BASE_COMPACTNESS)

    def test_uniform_image_falls_back_to_base_compactness(self):
        # range 0 would divide by zero; fall back to the base value:
        self.assertEqual(_slic_compactness(np.full((3, 3), 42.0)), _BASE_COMPACTNESS)


class SegmentTest(unittest.TestCase):

    def test_separates_two_distinct_aspect_regions(self):
        # left half faces one way (45), right half the opposite (225); with an aspect
        # merge threshold of 29 they must not merge into one segment:
        image = np.zeros((10, 10), dtype=float)
        image[:, :5] = 45.0
        image[:, 5:] = 225.0
        mask = np.ones((10, 10), dtype=bool)

        segments = _segment(image, mask, threshold=29)

        self.assertEqual(segments.shape, image.shape)
        # a left pixel and a right pixel end up in different segments:
        self.assertNotEqual(segments[0, 0], segments[0, 9])
        self.assertGreaterEqual(len(set(np.unique(segments)) - {0}), 2)

    def test_masked_out_pixels_are_zero(self):
        image = np.full((8, 8), 100.0)
        image[4:, :] = 200.0
        mask = np.ones((8, 8), dtype=bool)
        mask[0, 0] = False
        segments = _segment(image, mask, threshold=29)
        self.assertEqual(segments[0, 0], 0)


if __name__ == "__main__":
    unittest.main()
