# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import unittest

import numpy as np
from networkx import Graph

from solar_pv.roof_detection.detect_messy_roofs import (
    _mess_score, _obstacle_group_count, _obstacle_groups_img, detect_messy_roofs)


def _building(n: int, hole):
    """An n x n building that is entirely one plane (label 0) except for a rectangular
    interior `hole` (r0, r1, c0, c1) of outlier pixels. Returns (planes, labels, xy)."""
    r0, r1, c0, c1 = hole
    return _building_with_holes(n, [(y, x) for y in range(r0, r1) for x in range(c0, c1)])


def _building_with_holes(n: int, holes):
    """An n x n building that is one flat plane (label 0) except for the given `holes`
    (row, col) of outlier pixels. Returns (planes, labels, xy)."""
    xy = np.array([[x, y] for y in range(n) for x in range(n)], dtype=float)
    labels = np.zeros(n * n, dtype=int)  # all plane 0
    for (y, x) in holes:
        labels[y * n + x] = 999  # 999 is not a plane id, so these are outliers
    planes = {0: {"is_flat": True, "aspect": 180, "plane_id": "p0"}}
    return planes, labels, xy


class MessScoreTest(unittest.TestCase):

    def test_sums_only_neighbouring_obstacle_group_sizes(self):
        g = Graph()
        g.add_node(0, obstacle_group=False, inliers=40, is_flat=True)   # the flat plane
        g.add_node(1, obstacle_group=True, inliers=5)                   # obstacle group
        g.add_node(2, obstacle_group=False, inliers=99, is_flat=True)   # a neighbour plane
        g.add_node(3, obstacle_group=True, inliers=3)                   # obstacle group
        g.add_edges_from([(0, 1), (0, 2), (0, 3)])
        # 5 + 3 from the two obstacle groups; the neighbouring plane (99) is ignored:
        self.assertEqual(_mess_score(g, 0), 8)

    def test_zero_when_no_obstacle_neighbours(self):
        g = Graph()
        g.add_node(0, obstacle_group=False, inliers=40, is_flat=True)
        g.add_node(1, obstacle_group=False, inliers=10, is_flat=True)
        g.add_edge(0, 1)
        self.assertEqual(_mess_score(g, 0), 0)


class ObstacleGroupCountTest(unittest.TestCase):

    def test_counts_only_obstacle_group_neighbours(self):
        g = Graph()
        g.add_node(0, obstacle_group=False, inliers=40, is_flat=True)
        g.add_node(1, obstacle_group=True, inliers=5)
        g.add_node(2, obstacle_group=False, inliers=99, is_flat=True)  # a plane, not counted
        g.add_node(3, obstacle_group=True, inliers=3)
        g.add_edges_from([(0, 1), (0, 2), (0, 3)])
        self.assertEqual(_obstacle_group_count(g, 0), 2)


class ObstacleGroupsImgTest(unittest.TestCase):

    def test_interior_hole_becomes_an_obstacle_group(self):
        planes, labels, xy = _building(7, (3, 4, 3, 4))  # single interior outlier pixel
        img = _obstacle_groups_img(planes, labels, xy, res=1, connectivity=1)
        values = set(np.unique(img).tolist())
        self.assertIn(0, values)                       # the plane
        # the interior hole is labelled as an obstacle group (a distinct id > the plane
        # ids); no -1 edge marker here, since the building's whole boundary is plane:
        self.assertTrue(any(v > 0 for v in values))


class DetectMessyRoofsTest(unittest.TestCase):

    def test_returns_planes_unchanged_when_none_are_flat(self):
        # the mess heuristic only applies to flat roofs; a pitched roof is returned as-is
        # without even building the obstacle graph:
        planes = {0: {"is_flat": False, "aspect": 45}, 1: {"is_flat": False, "aspect": 200}}
        dummy = np.zeros((1, 2))
        result = detect_messy_roofs(planes, np.zeros(1, dtype=int), dummy, res=1)
        self.assertEqual(len(result), 2)

    def test_keeps_a_flat_roof_with_a_tiny_obstacle(self):
        # one stray outlier pixel is well below the mess threshold, so the roof survives:
        planes, labels, xy = _building(7, (3, 4, 3, 4))
        result = detect_messy_roofs(planes, labels, xy, res=1)
        self.assertEqual(len(result), 1)

    def test_rejects_a_flat_roof_riddled_with_obstacles(self):
        # a 3x3 interior hole of outliers pushes the mess score over the threshold, so
        # the roof is rejected as messy and nothing is returned:
        planes, labels, xy = _building(7, (2, 5, 2, 5))
        result = detect_messy_roofs(planes, labels, xy, res=1)
        self.assertEqual(len(result), 0)

    def test_rejects_a_flat_roof_fragmented_by_many_small_obstacles(self):
        # six scattered single-pixel obstacles: their total area is well below the
        # density threshold, but the count of separate groups (6) exceeds the group-
        # count threshold, so the roof is still recognised as messy:
        holes = [(2, 2), (2, 5), (2, 8), (5, 5), (8, 2), (8, 8)]
        planes, labels, xy = _building_with_holes(11, holes)
        result = detect_messy_roofs(planes, labels, xy, res=1)
        self.assertEqual(len(result), 0)


if __name__ == "__main__":
    unittest.main()
