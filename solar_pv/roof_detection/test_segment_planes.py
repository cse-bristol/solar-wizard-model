# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import unittest

import numpy as np

from solar_pv.roof_detection.assign_pixels import NONE, grid_index
from solar_pv.roof_detection.segment_planes import segment_planes, _local_normals


def _grid(width: int, height: int, z_fn, res: float = 1.0, noise: float = 0.0, seed: int = 0):
    xs, ys = np.meshgrid((np.arange(width) + 0.5) * res, (np.arange(height) + 0.5) * res)
    x, y = xs.ravel(), ys.ravel()
    z = z_fn(x, y) + np.random.RandomState(seed).normal(0, noise, len(x))
    return np.column_stack([x, y, z])


def _gable(x, y):
    """Ridge along y at x = 5, 35 degree pitch."""
    return 10 - np.abs(x - 5) * np.tan(np.radians(35))


class SegmentPlanesTest(unittest.TestCase):

    def test_gable_splits_at_ridge(self):
        xyz = _grid(10, 12, _gable, noise=0.03)
        labels = segment_planes(xyz, 1.0)
        west, east = xyz[:, 0] < 5, xyz[:, 0] > 5
        self.assertEqual(len(set(labels) - {NONE}), 2)
        self.assertEqual(len(set(labels[west])), 1)
        self.assertEqual(len(set(labels[east])), 1)
        self.assertNotEqual(labels[west][0], labels[east][0])

    def test_steps_between_parallel_planes_split(self):
        # two flat roofs 0.5m apart: same normal, so only the distance test splits them
        xyz = _grid(12, 10, lambda x, y: np.where(x < 6, 5.0, 5.5), noise=0.01)
        labels = segment_planes(xyz, 1.0)
        low, high = xyz[:, 0] < 6, xyz[:, 0] > 6
        self.assertEqual(len(set(labels[low])), 1)
        self.assertEqual(len(set(labels[high])), 1)
        self.assertNotEqual(labels[low][0], labels[high][0])

    def test_small_regions_unlabelled(self):
        xyz = _grid(3, 3, lambda x, y: np.full(len(x), 5.0))
        labels = segment_planes(xyz, 1.0, min_pixels=10)
        self.assertTrue((labels == NONE).all())

    def test_ridge_pixels_take_their_own_sides_normal(self):
        xyz = _grid(10, 12, _gable)
        grid, rows, cols = grid_index(xyz[:, :2], 1.0)
        normals, rms = _local_normals(grid, rows, cols, xyz[:, 2], 1.0)
        # the pixels either side of the ridge, where the centred window straddles it:
        for x, sign in ((4.5, -1), (5.5, 1)):
            idx = np.flatnonzero((xyz[:, 0] == x) & (xyz[:, 1] == 5.5))[0]
            self.assertAlmostEqual(rms[idx], 0, places=6)
            slope = np.degrees(np.arccos(normals[idx, 2]))
            self.assertAlmostEqual(slope, 35, places=4)
            self.assertEqual(np.sign(normals[idx, 0]), sign)
