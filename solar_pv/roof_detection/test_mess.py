# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import unittest

import numpy as np

from solar_pv.roof_detection.mess import reject_mess, plane_mess

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


class MessTest(unittest.TestCase):

    def test_flat_roof_with_scattered_plant_is_messy(self):
        xyz, labels = _flat_roof()
        for x0, y0 in [(3, 3), (3, 12), (12, 3), (12, 12), (7, 8), (15, 8)]:
            unit = _square(xyz, x0, y0, 2)
            xyz[unit, 2] += 1.0
            labels[unit] = _NONE
        planes = {_FLAT: _planes()[_FLAT]}

        mess, _ = plane_mess(planes, labels, xyz, 1.0)

        assert mess[_FLAT].obstacle_groups == 6
        assert mess[_FLAT].messy
        assert reject_mess(planes, labels, xyz, 1.0) == []

    def test_parapet_is_not_an_obstacle(self):
        xyz, labels = _flat_roof()
        edge = (xyz[:, 0] < 1) | (xyz[:, 0] > 19) | (xyz[:, 1] < 1) | (xyz[:, 1] > 19)
        xyz[edge, 2] += 1.0
        labels[edge] = _NONE
        planes = {_FLAT: _planes()[_FLAT]}

        mess, _ = plane_mess(planes, labels, xyz, 1.0)

        assert mess[_FLAT].obstacle_pixels == 0
        assert len(reject_mess(planes, labels, xyz, 1.0)) == 1

    def test_small_pitched_plane_within_flat_roof_is_clutter(self):
        xyz, labels = _flat_roof()
        labels[_square(xyz, 8, 8, 4)] = _OTHER
        planes = _planes()

        mess, _ = plane_mess(planes, labels, xyz, 1.0)

        assert mess[_OTHER].clutter
        assert reject_mess(planes, labels, xyz, 1.0) == [planes[_FLAT]]

    def test_small_pitched_plane_at_building_edge_is_not_clutter(self):
        xyz, labels = _flat_roof()
        labels[_square(xyz, -1, 8, 5)] = _OTHER
        planes = _planes()

        mess, _ = plane_mess(planes, labels, xyz, 1.0)

        assert not mess[_OTHER].clutter
        assert len(reject_mess(planes, labels, xyz, 1.0)) == 2
