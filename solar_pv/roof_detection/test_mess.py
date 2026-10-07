# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import unittest

import numpy as np

from solar_pv.roof_detection import rejections
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


# enough on a 20x20m roof for it to be messy by their number, not their area:
_PLANT_UNITS = [(x0, y0) for x0 in (3, 8, 13) for y0 in (3, 8, 13)]


def _square(xyz, x0, y0, w):
    return (xyz[:, 0] > x0) & (xyz[:, 0] < x0 + w) & (xyz[:, 1] > y0) & (xyz[:, 1] < y0 + w)


class MessTest(unittest.TestCase):

    def test_flat_roof_with_scattered_plant_is_messy(self):
        xyz, labels = _flat_roof()
        for x0, y0 in _PLANT_UNITS:
            unit = _square(xyz, x0, y0, 2)
            xyz[unit, 2] += 1.0
            labels[unit] = _NONE
        planes = {_FLAT: _planes()[_FLAT]}

        mess, _ = plane_mess(planes, labels, xyz, 1.0)

        assert mess[_FLAT].obstacle_groups == 9
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

    def test_taller_part_of_building_is_not_an_obstacle(self):
        xyz, labels = _flat_roof()
        taller = xyz[:, 0] > 14
        xyz[taller, 2] += 3.0
        labels[taller] = _NONE
        # a plane interrupting the foot of the taller part:
        labels[(xyz[:, 0] > 13) & (xyz[:, 0] < 15) & (xyz[:, 1] > 8) & (xyz[:, 1] < 12)] = _OTHER
        planes = _planes()

        mess, _ = plane_mess(planes, labels, xyz, 1.0)

        assert mess[_FLAT].obstacle_pixels == 0

    def test_pixels_between_flat_roof_and_higher_plane_are_not_obstacles(self):
        xyz, labels = _flat_roof()
        higher = xyz[:, 0] > 10
        xyz[higher, 2] += 2.0
        labels[higher] = _OTHER
        seam = (xyz[:, 0] > 10) & (xyz[:, 0] < 11) & (xyz[:, 1] > 3) & (xyz[:, 1] < 17)
        labels[seam & (np.floor(xyz[:, 1]) % 2 == 0)] = _NONE
        planes = {_FLAT: _planes()[_FLAT],
                  _OTHER: {**_planes()[_FLAT], 'intercept': 12.0}}

        mess, _ = plane_mess(planes, labels, xyz, 1.0)

        assert mess[_FLAT].obstacle_groups == 0

    def test_clutter_standing_on_flat_roof_is_an_obstacle(self):
        xyz, labels = _flat_roof()
        unit = _square(xyz, 8, 8, 4)
        xyz[unit, 2] += 1.0
        labels[unit] = _OTHER
        planes = _planes()

        mess, _ = plane_mess(planes, labels, xyz, 1.0)

        assert mess[_OTHER].clutter
        assert mess[_FLAT].obstacle_pixels == 16
        assert mess[_FLAT].obstacle_groups == 1

    def test_clutter_on_step_between_flat_roofs_is_not_an_obstacle(self):
        xyz, labels = _flat_roof()
        upper = xyz[:, 0] > 12
        xyz[upper, 2] += 2.0
        labels[upper] = 2
        step = (xyz[:, 0] > 10) & (xyz[:, 0] < 12) & (xyz[:, 1] > 8) & (xyz[:, 1] < 12)
        xyz[step, 2] += 1.0
        labels[step] = _OTHER
        planes = {**_planes(), 2: {**_planes()[_FLAT], 'intercept': 12.0}}

        mess, _ = plane_mess(planes, labels, xyz, 1.0)

        assert mess[_OTHER].clutter
        assert mess[_FLAT].obstacle_pixels == 0
        assert mess[2].obstacle_pixels == 0

    def test_rooflight_is_an_obstacle(self):
        xyz, labels = _flat_roof()
        rooflight = _square(xyz, 8, 8, 3)
        xyz[rooflight, 2] -= 1.0
        labels[rooflight] = _NONE
        planes = {_FLAT: _planes()[_FLAT]}

        mess, _ = plane_mess(planes, labels, xyz, 1.0)

        assert mess[_FLAT].obstacle_pixels == 9
        assert mess[_FLAT].obstacle_groups == 1

    def test_ground_within_footprint_is_not_an_obstacle(self):
        xyz, labels = _flat_roof()
        yard = _square(xyz, 8, 8, 3)
        xyz[yard, 2] -= 8.0
        labels[yard] = _NONE
        planes = {_FLAT: _planes()[_FLAT]}

        mess, _ = plane_mess(planes, labels, xyz, 1.0, above_ground=~yard)

        assert mess[_FLAT].obstacle_pixels == 0

    def test_low_plant_within_flat_roof_is_an_obstacle(self):
        xyz, labels = _flat_roof()
        xyz[_square(xyz, 8, 8, 3), 2] += 0.1
        planes = {_FLAT: _planes()[_FLAT]}

        mess, _ = plane_mess(planes, labels, xyz, 1.0)

        assert mess[_FLAT].obstacle_pixels == 9
        assert mess[_FLAT].obstacle_groups == 1

    def test_large_flat_roof_with_a_few_obstacles_is_not_messy(self):
        xyz, labels = _flat_roof(size=50)
        for x0, y0 in _PLANT_UNITS:
            unit = _square(xyz, x0, y0, 2)
            xyz[unit, 2] += 1.0
            labels[unit] = _NONE
        planes = {_FLAT: _planes()[_FLAT]}

        mess, _ = plane_mess(planes, labels, xyz, 1.0)

        assert mess[_FLAT].obstacle_groups == 9
        assert not mess[_FLAT].messy

    def test_single_pixel_low_bump_is_not_an_obstacle(self):
        xyz, labels = _flat_roof()
        xyz[_square(xyz, 8, 8, 1), 2] += 0.15
        planes = {_FLAT: _planes()[_FLAT]}

        mess, _ = plane_mess(planes, labels, xyz, 1.0)

        assert mess[_FLAT].obstacle_pixels == 0

    def test_fall_to_drain_is_not_an_obstacle(self):
        xyz, labels = _flat_roof()
        xyz[:, 2] += 0.002 * ((xyz[:, 0] - 10) ** 2 + (xyz[:, 1] - 10) ** 2)
        planes = {_FLAT: _planes()[_FLAT]}

        mess, _ = plane_mess(planes, labels, xyz, 1.0)

        assert mess[_FLAT].obstacle_pixels == 0

    def test_clutter_counts_on_each_flat_roof_it_stands_on(self):
        xyz, labels = _flat_roof()
        labels[xyz[:, 0] > 10] = 2
        unit = _square(xyz, 8, 8, 4)
        xyz[unit, 2] += 1.0
        labels[unit] = _OTHER
        planes = {**_planes(), 2: _planes()[_FLAT]}

        mess, _ = plane_mess(planes, labels, xyz, 1.0)

        assert mess[_FLAT].obstacle_pixels == 16
        assert mess[2].obstacle_pixels == 16

    def test_small_pitched_plane_surrounded_by_mess_is_rejected(self):
        xyz, labels = _flat_roof()
        for x0, y0 in _PLANT_UNITS:
            unit = _square(xyz, x0, y0, 2)
            xyz[unit, 2] += 1.0
            labels[unit] = _NONE
        labels[_square(xyz, -1, 8, 3)] = _OTHER
        planes = _planes()

        mess, _ = plane_mess(planes, labels, xyz, 1.0)

        assert not mess[_OTHER].clutter
        assert mess[_OTHER].surrounded_by_mess
        rejected = []
        reject_mess({k: {**p, 'building_id': 'b'} for k, p in planes.items()}, labels, xyz, 1.0, rejected=rejected)
        assert rejections.SURROUNDED_BY_MESS in {r['rejection_reason'] for r in rejected}

    def test_small_building_edged_with_unassigned_pixels_is_not_mess(self):
        xyz, labels = _flat_roof(size=6)
        edge = (xyz[:, 0] < 1) | (xyz[:, 0] > 5) | (xyz[:, 1] < 1) | (xyz[:, 1] > 5)
        xyz[edge, 2] -= 1.0
        labels[:] = _OTHER
        labels[edge] = _NONE
        planes = {_OTHER: _planes()[_OTHER]}

        mess, _ = plane_mess(planes, labels, xyz, 1.0)

        assert not mess[_OTHER].messy

    def test_rejections_are_recorded(self):
        xyz, labels = _flat_roof()
        for x0, y0 in _PLANT_UNITS:
            unit = _square(xyz, x0, y0, 2)
            xyz[unit, 2] += 1.0
            labels[unit] = _NONE
        planes = {_FLAT: {**_planes()[_FLAT], 'building_id': 'b'}}

        rejected = []
        reject_mess(planes, labels, xyz, 1.0, rejected=rejected)

        by_reason = {r['rejection_reason']: r for r in rejected}
        assert set(by_reason) == {rejections.MESSY_FLAT_ROOF, rejections.OBSTACLES}
        assert by_reason[rejections.MESSY_FLAT_ROOF]['rejection_details']['obstacle_groups'] == 9
        assert len(by_reason[rejections.OBSTACLES]['inliers_xy']) == 9 * 4
