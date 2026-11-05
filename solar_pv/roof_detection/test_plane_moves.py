# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import unittest

import numpy as np

from solar_pv.roof_detection.plane_fit import PlaneFit
from solar_pv.roof_detection.plane_moves import Plane, merge_by_energy, remove_unprofitable


def _scale(is_flat: bool) -> float:
    return 0.1 if is_flat else 0.25


def _fit(xyz: np.ndarray):
    lr = PlaneFit().fit(xyz[:, :2], xyz[:, 2])
    return (lr.coef_[0], lr.coef_[1], lr.intercept_), False


def _planes(xyz: np.ndarray, labels: np.ndarray):
    planes = {}
    for plane_id in np.unique(labels[labels >= 0]):
        idxs = labels == plane_id
        coef, is_flat = _fit(xyz[idxs])
        planes[int(plane_id)] = Plane(coef=coef, is_flat=is_flat, plane_type="segmented",
                                      plane_id=f"segmented_{plane_id}", xy=xyz[idxs, :2], proposed={})
    return planes


def _grid(z_fn, width=10, height=6, noise=0.02):
    xs, ys = np.meshgrid(np.arange(width) + 0.5, np.arange(height) + 0.5)
    x, y = xs.ravel(), ys.ravel()
    return np.column_stack([x, y, z_fn(x, y) + np.random.RandomState(0).normal(0, noise, len(x))])


class MergeByEnergyTest(unittest.TestCase):

    def test_halves_of_one_face_merge(self):
        xyz = _grid(lambda x, y: y * np.tan(np.radians(30)) + 5)
        labels = np.where(xyz[:, 0] < 5, 0, 1)
        planes = _planes(xyz, labels)

        merges = merge_by_energy(xyz, labels, planes, 1.0, 8.0, _scale, _fit)

        self.assertEqual(merges, [(0, 1)])
        self.assertEqual(list(planes), [0])
        self.assertTrue((labels == 0).all())
        self.assertEqual(len(planes[0].xy), len(xyz))
        self.assertEqual(planes[0].plane_id, "segmented_0_MERGED_segmented_1")
        self.assertAlmostEqual(planes[0].slope, 30, delta=0.5)

    def test_faces_of_a_gable_do_not(self):
        xyz = _grid(lambda x, y: 10 - np.abs(y - 3) * np.tan(np.radians(30)))
        labels = np.where(xyz[:, 1] < 3, 0, 1)
        planes = _planes(xyz, labels)

        self.assertEqual(merge_by_energy(xyz, labels, planes, 1.0, 8.0, _scale, _fit), [])
        self.assertEqual(len(planes), 2)

    def test_only_mergeable_pairs_merge(self):
        xyz = _grid(lambda x, y: y * np.tan(np.radians(30)) + 5)
        labels = np.where(xyz[:, 0] < 5, 0, 1)
        planes = _planes(xyz, labels)

        self.assertEqual(merge_by_energy(xyz, labels, planes, 1.0, 8.0, _scale, _fit, lambda p, q: False), [])

    def test_good_enough_share(self):
        # halves of a face with a step between them: one plane is within 0.15m of every
        # pixel at a step of 0.3m, so explains them all, but at 0.6m none
        def halves(step):
            xyz = _grid(lambda x, y: y * np.tan(np.radians(30)) + 5 + np.where(x < 5, 0, step))
            labels = np.where(xyz[:, 0] < 5, 0, 1)
            return xyz, labels, _planes(xyz, labels)

        xyz, labels, planes = halves(0.3)
        self.assertEqual(merge_by_energy(xyz, labels, planes, 1.0, 8.0, _scale, _fit), [])
        self.assertEqual(merge_by_energy(xyz, labels, planes, 1.0, 8.0, _scale, _fit, good_enough_share=0.9), [(0, 1)])
        xyz, labels, planes = halves(0.6)
        self.assertEqual(merge_by_energy(xyz, labels, planes, 1.0, 8.0, _scale, _fit, good_enough_share=0.9), [])


class RemoveUnprofitableTest(unittest.TestCase):

    def test_plane_explaining_little_more_than_its_neighbour_is_removed(self):
        # a 2-pixel strip on the same face as the plane next to it:
        xyz = _grid(lambda x, y: y * np.tan(np.radians(30)) + 5)
        labels = np.where(xyz[:, 0] < 8, 0, 1)
        planes = _planes(xyz, labels)

        self.assertEqual(remove_unprofitable(xyz, labels, planes, 1.0, 8.0, _scale, -1), [1])
        self.assertEqual(list(planes), [0])
        self.assertTrue((labels[xyz[:, 0] > 8] == -1).all())
