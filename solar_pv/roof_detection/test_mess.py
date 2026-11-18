# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import unittest

import numpy as np

from solar_pv.roof_detection import rejections
from solar_pv.roof_detection import mess
from solar_pv.roof_detection.mess import reject_clutter, clutter, MessCost, obstacle_holes
from solar_pv.roof_detection.segment_planes import local_fits

_FLAT = 0
_OTHER = 1
_NONE = -1


def _flat_roof(size: int = 20, height: float = 10.0):
    xs, ys = np.meshgrid(np.arange(size) + 0.5, np.arange(size) + 0.5)
    xyz = np.column_stack([xs.ravel(), ys.ravel(), np.full(size * size, height)])
    return xyz, np.full(size * size, _FLAT)


def _planes(pitched_mae: float = 0.2):
    return {
        _FLAT: {'is_flat': True, 'mae': 0.03, 'x_coef': 0.0, 'y_coef': 0.0, 'intercept': 10.0},
        _OTHER: {'is_flat': False, 'mae': pitched_mae, 'x_coef': 0.5, 'y_coef': 0.0, 'intercept': 0.0},
    }


def _square(xyz, x0, y0, w):
    return (xyz[:, 0] > x0) & (xyz[:, 0] < x0 + w) & (xyz[:, 1] > y0) & (xyz[:, 1] < y0 + w)


class ClutterTest(unittest.TestCase):

    def test_small_pitched_plane_within_flat_roof_is_clutter(self):
        xyz, labels = _flat_roof()
        labels[_square(xyz, 8, 8, 4)] = _OTHER
        planes = _planes()

        assert clutter(planes, labels, xyz, 1.0) == {_FLAT: False, _OTHER: True}
        assert reject_clutter(planes, labels, xyz, 1.0) == [planes[_FLAT]]

    def test_small_pitched_plane_at_building_edge_is_not_clutter(self):
        xyz, labels = _flat_roof()
        labels[_square(xyz, -1, 8, 5)] = _OTHER
        planes = _planes()

        assert not clutter(planes, labels, xyz, 1.0)[_OTHER]
        assert len(reject_clutter(planes, labels, xyz, 1.0)) == 2

    def test_rejections_are_recorded(self):
        xyz, labels = _flat_roof()
        labels[_square(xyz, 8, 8, 4)] = _OTHER
        planes = {k: {**p, 'building_id': 'b'} for k, p in _planes().items()}

        rejected = []
        reject_clutter(planes, labels, xyz, 1.0, rejected=rejected)

        assert [r['rejection_reason'] for r in rejected] == [rejections.CLUTTER]


class MessCostTest(unittest.TestCase):

    def _cost(self, xyz):
        above_ground = np.ones(len(xyz), dtype=bool)
        old = mess.MESS_WEIGHT
        mess.MESS_WEIGHT = 1.0
        try:
            return MessCost(xyz, local_fits(xyz, 1.0)[1], above_ground, 1.0).cost((0.0, 0.0, 10.0), np.arange(len(xyz)))
        finally:
            mess.MESS_WEIGHT = old

    def test_rough_part_of_flat_roof_costs_more(self):
        xyz, _ = _flat_roof()
        rough = _square(xyz, 8, 8, 8)
        checkerboard = (np.floor(xyz[:, 0]) + np.floor(xyz[:, 1])) % 2 * 2 - 1
        xyz[rough, 2] += 0.05 * checkerboard[rough]

        cost = self._cost(xyz)

        assert (cost[_square(xyz, 10, 10, 4)] > 0).all()
        assert not cost[~_square(xyz, 5, 5, 14)].any()

    def test_lone_obstacle_costs_nothing(self):
        xyz, _ = _flat_roof()
        xyz[_square(xyz, 8, 8, 2), 2] += 1.0

        assert not self._cost(xyz).any()

    def test_obstacles_close_together_cost_more_between_them(self):
        xyz, _ = _flat_roof()
        for x0 in (4, 8, 12):
            for y0 in (4, 8, 12):
                xyz[_square(xyz, x0, y0, 2), 2] += 1.0

        cost = self._cost(xyz)

        assert cost[_square(xyz, 9, 9, 1)].all()
        assert not cost[_square(xyz, 0, 0, 3)].any()

    def test_taller_part_of_building_beside_flat_roof_costs_nothing(self):
        xyz, _ = _flat_roof()
        xyz[xyz[:, 0] > 12, 2] += 3.0

        assert not self._cost(xyz).any()


class ObstacleHolesTest(unittest.TestCase):

    def _holes(self, xyz, labels):
        return obstacle_holes(xyz, np.ones(len(xyz), dtype=bool), np.flatnonzero(labels == _FLAT), (0.0, 0.0, 10.0), 1.0)

    def test_lone_obstacle_is_cut_out_alone(self):
        xyz, labels = _flat_roof()
        unit = _square(xyz, 8, 8, 2)
        xyz[unit, 2] += 1.0
        labels[unit] = _NONE

        np.testing.assert_array_equal(self._holes(xyz, labels), unit)

    def test_obstacles_close_together_are_cut_out_without_the_roof_between_them(self):
        xyz, labels = _flat_roof()
        units = np.zeros(len(xyz), dtype=bool)
        for x0 in (5, 8, 11):
            for y0 in (5, 8, 11):
                units |= _square(xyz, x0, y0, 1.5)
        xyz[units, 2] += 1.0
        labels[units] = _NONE

        np.testing.assert_array_equal(self._holes(xyz, labels), units)

    def test_rooflight_is_cut_out(self):
        xyz, labels = _flat_roof()
        rooflight = _square(xyz, 8, 8, 3)
        xyz[rooflight, 2] -= 1.0
        labels[rooflight] = _NONE

        np.testing.assert_array_equal(self._holes(xyz, labels), rooflight)

    def test_rough_part_of_flat_roof_is_cut_out(self):
        xyz, labels = _flat_roof()
        rough = _square(xyz, 8, 8, 8)
        checkerboard = (np.floor(xyz[:, 0]) + np.floor(xyz[:, 1])) % 2 * 2 - 1
        xyz[rough, 2] += 0.05 * checkerboard[rough]

        holes = self._holes(xyz, labels)

        assert holes[_square(xyz, 9, 9, 6)].all()
        assert not holes[~_square(xyz, 7, 7, 10)].any()

    def test_taller_part_of_building_beside_flat_roof_is_not_an_obstacle(self):
        xyz, labels = _flat_roof()
        wall = xyz[:, 0] > 15
        xyz[wall, 2] += 3.0
        labels[wall] = _NONE

        assert not self._holes(xyz, labels).any()

