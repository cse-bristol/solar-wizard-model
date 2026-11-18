# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import math
import unittest

import numpy as np

from solar_pv.roof_detection.assign_pixels import assign_pixels, NONE


def _gable(width: int = 20, depth: int = 12, slope_deg: float = 30, noise: float = 0.03):
    """A gable roof with its ridge along y = depth / 2, on a 1m grid; which pixels are on its
    south face; and the coefficients of its south and north faces."""
    rng = np.random.default_rng(1)
    xs, ys = np.meshgrid(np.arange(width) + 0.5, np.arange(depth) + 0.5)
    xs, ys = xs.ravel(), ys.ravel()
    ridge = depth / 2
    t = math.tan(math.radians(slope_deg))
    z = 10 - np.abs(ys - ridge) * t + rng.normal(0, noise, len(xs))
    return np.column_stack([xs, ys, z]), ys < ridge, [(0, t, 10 - ridge * t), (0, -t, 10 + ridge * t)]


def _assign(xyz, labels, coefs, **kwargs):
    return assign_pixels(xyz, labels, coefs, [0.25] * len(coefs), 1.0, **kwargs)[0]


class AssignPixelsTest(unittest.TestCase):

    def test_fills_gaps_between_inliers(self):
        xyz, south, coefs = _gable()
        dropped = np.random.default_rng(2).random(len(xyz)) < 0.3
        labels = np.where(south, 0, 1)
        labels[dropped] = NONE

        assigned = _assign(xyz, labels, coefs)

        np.testing.assert_array_equal(assigned, np.where(south, 0, 1))

    def test_extra_cost_unassigns_pixels(self):
        xyz, south, coefs = _gable()
        messy = (xyz[:, 0] > 5) & (xyz[:, 0] < 12) & (xyz[:, 1] < 5)
        extra = np.where(messy, 3.0, 0.0)
        labels = np.where(south, 0, 1)

        assigned = _assign(xyz, labels, coefs, extra_costs=[extra, None])

        assert np.all(assigned[messy] == NONE)
        assert np.all(assigned[south & ~messy] == 0)
        assert np.all(assigned[~south] == 1)

    def test_obstacle_pixels_are_unassigned(self):
        xyz, south, coefs = _gable()
        chimney = (xyz[:, 0] > 8) & (xyz[:, 0] < 11) & (xyz[:, 1] > 1) & (xyz[:, 1] < 4)
        xyz[chimney, 2] += 1.5
        labels = np.where(south, 0, 1)
        labels[chimney] = NONE

        assigned = _assign(xyz, labels, coefs)

        assert np.all(assigned[chimney] == NONE)
        assert np.all(assigned[south & ~chimney] == 0)

    def test_plane_cannot_claim_distant_pixels(self):
        xyz, south, coefs = _gable()
        # only the west end of the south plane was detected:
        labels = np.where(south, NONE, 1)
        labels[south & (xyz[:, 0] < 5)] = 0

        assigned = _assign(xyz, labels, coefs, max_dist_m=3)

        assert xyz[assigned == 0, 0].max() < 5 + 3

    def test_plane_with_too_few_pixels_is_dropped(self):
        xyz, south, coefs = _gable()
        labels = np.where(south, 0, 1)
        labels[south] = NONE
        labels[np.flatnonzero(south)[:2]] = 0

        assigned = _assign(xyz, labels, coefs)

        assert not np.any(assigned == 0)

    def test_only_cuts_a_plane_if_worth_it(self):
        # a flat roof with a slightly raised strip across it, which a second plane has
        # started on: taking the whole strip would cut the roof in two
        xs, ys = np.meshgrid(np.arange(20) + 0.5, np.arange(8) + 0.5)
        xs, ys = xs.ravel(), ys.ravel()
        strip = (xs > 9) & (xs < 11)
        xyz = np.column_stack([xs, ys, np.where(strip, 10.12, 10.0)])
        labels = np.where(strip & (ys < 4), 1, 0)
        coefs = [(0, 0, 10.0), (0, 0, 10.12)]

        assigned, _ = assign_pixels(xyz, labels, coefs, [0.1, 0.1], 1.0, max_dist_m=5)
        assert np.all(assigned[~strip] == 0)
        assert np.all(assigned[strip & (ys < 4)] == 1)
