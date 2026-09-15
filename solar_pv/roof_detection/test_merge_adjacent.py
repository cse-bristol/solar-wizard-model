# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import unittest

import numpy as np
from networkx import Graph
from sklearn import metrics
from sklearn.linear_model import LinearRegression

from solar_pv.geos import slope_deg, aspect_deg
from solar_pv.roof_detection.merge_adjacent import (
    _edge_weight, _update_node_data, merge_adjacent, DO_MERGE, DO_NOT_MERGE)


def _grid(x0, x1, y0, y1):
    return np.array([[x, y] for y in range(y0, y1) for x in range(x0, x1)], dtype=float)


def _plane_attrs(xy, z, **extra):
    """Plane-level attributes (mae/r2/slope/aspect etc), as a RoofPlane dict would carry."""
    xy = np.asarray(xy, dtype=float)
    z = np.asarray(z, dtype=float)
    lr = LinearRegression().fit(xy, z)
    pred = lr.predict(xy)
    attrs = dict(
        mae=metrics.mean_absolute_error(z, pred),
        r2=metrics.r2_score(z, pred),
        slope=slope_deg(lr.coef_[0], lr.coef_[1]),
        aspect=int(round(aspect_deg(lr.coef_[0], lr.coef_[1]))),
        is_flat=slope_deg(lr.coef_[0], lr.coef_[1]) <= 4.9,
        plane_type="segmented_aspect", plane_id="p",
    )
    attrs.update(extra)
    return attrs


def _plane_node(xy, z, **extra):
    """A full RAG plane node: plane attrs plus the graph-internal point subsets."""
    xy = np.asarray(xy, dtype=float)
    z = np.asarray(z, dtype=float)
    node = _plane_attrs(xy, z, xy_subset=xy, z_subset=z, outlier=False, res=1, labels=[0])
    node.update(extra)
    return node


def _outlier_node(xy, z):
    xy = np.asarray(xy, dtype=float)
    z = np.asarray(z, dtype=float)
    return dict(xy_subset=xy, z_subset=z, outlier=True, res=1, labels=[0])


def _two_node_graph(node0, node1):
    g = Graph()
    g.add_node(0, **node0)
    g.add_node(1, **node1)
    g.add_edge(0, 1)
    return g


class EdgeWeightTest(unittest.TestCase):

    def test_two_outliers_never_merge(self):
        g = _two_node_graph(_outlier_node(_grid(0, 2, 0, 2), np.zeros(4)),
                            _outlier_node(_grid(2, 4, 0, 2), np.zeros(4)))
        self.assertEqual(_edge_weight(g, 0, 1), DO_NOT_MERGE)

    def test_two_coplanar_planes_merge(self):
        # two sloped patches of the exact same plane z = 2x + 3y: fitting them together
        # is a perfect fit, so they should merge:
        a = _grid(0, 3, 0, 3); b = _grid(3, 6, 0, 3)
        za = 2 * a[:, 0] + 3 * a[:, 1]; zb = 2 * b[:, 0] + 3 * b[:, 1]
        g = _two_node_graph(_plane_node(a, za), _plane_node(b, zb))
        self.assertEqual(_edge_weight(g, 0, 1), DO_MERGE)

    def test_divergent_planes_do_not_merge(self):
        # two steep planes facing opposite ways don't fit a common plane, so the edge
        # weight stays above the (0) merge threshold:
        a = _grid(0, 3, 0, 3); b = _grid(3, 6, 0, 3)
        za = 3 * a[:, 0]            # rises east
        zb = -3 * b[:, 0] + 100     # rises west
        g = _two_node_graph(_plane_node(a, za), _plane_node(b, zb))
        self.assertGreater(_edge_weight(g, 0, 1), 0)

    def test_plane_and_far_off_outlier_do_not_merge(self):
        # an outlier nowhere near the plane wrecks the fit, so the weight is large/positive:
        a = _grid(0, 3, 0, 3)
        za = 2 * a[:, 0] + 3 * a[:, 1]
        outlier = _outlier_node(np.array([[1.0, 1.0]]), np.array([1000.0]))
        g = _two_node_graph(_plane_node(a, za), outlier)
        self.assertGreater(_edge_weight(g, 0, 1), 0)


class UpdateNodeDataTest(unittest.TestCase):

    def test_merges_points_and_recomputes_the_plane(self):
        a = _grid(0, 2, 0, 3); b = _grid(2, 4, 0, 3)  # adjacent, together a 4x3 block
        za = 2 * a[:, 0] + 3 * a[:, 1]; zb = 2 * b[:, 0] + 3 * b[:, 1]
        g = _two_node_graph(_plane_node(a, za, plane_id="a"),
                            _plane_node(b, zb, plane_id="b"))

        _update_node_data(g, src=1, dst=0)
        dst = g.nodes[0]

        # combined point set, no longer an outlier, provenance recorded:
        self.assertEqual(len(dst['xy_subset']), len(a) + len(b))
        self.assertIs(dst['outlier'], False)
        self.assertEqual(dst['plane_id'], "a_MERGED_b")
        self.assertIn("_MERGED_", dst['plane_type'])
        # coplanar, so the refit reproduces the shared plane's slope/aspect and fits perfectly:
        self.assertAlmostEqual(dst['slope'], slope_deg(2, 3), places=6)
        self.assertAlmostEqual(dst['aspect_raw'], aspect_deg(2, 3), places=6)
        self.assertAlmostEqual(dst['mae'], 0, places=6)
        self.assertAlmostEqual(dst['r2'], 1.0, places=6)


class MergeAdjacentTest(unittest.TestCase):

    def _regions(self, zb_fn):
        xy = _grid(0, 6, 0, 3)
        left = xy[:, 0] < 3
        z = np.empty(len(xy))
        z[left] = 2 * xy[left, 0] + 3 * xy[left, 1]
        z[~left] = zb_fn(xy[~left])
        labels = np.where(left, 0, 1)
        planes = {0: _plane_attrs(xy[left], z[left], plane_id="a"),
                  1: _plane_attrs(xy[~left], z[~left], plane_id="b")}
        return xy, z, labels, planes

    def test_adjacent_coplanar_regions_merge_into_one(self):
        xy, z, labels, planes = self._regions(lambda b: 2 * b[:, 0] + 3 * b[:, 1])
        merged, _ = merge_adjacent(xy, z, labels.copy(), planes, res=1, nodata=-9999)
        self.assertEqual(len(merged), 1)

    def test_adjacent_divergent_regions_stay_separate(self):
        # right region faces the opposite way, so the two planes are kept apart:
        xy, z, labels, planes = self._regions(lambda b: -3 * b[:, 0] + 200)
        merged, _ = merge_adjacent(xy, z, labels.copy(), planes, res=1, nodata=-9999)
        self.assertEqual(len(merged), 2)

    def test_rejects_threshold_at_or_above_do_not_merge(self):
        xy, z, labels, planes = self._regions(lambda b: 2 * b[:, 0] + 3 * b[:, 1])
        with self.assertRaises(ValueError):
            merge_adjacent(xy, z, labels.copy(), planes, res=1, nodata=-9999, thresh=DO_NOT_MERGE)


if __name__ == "__main__":
    unittest.main()
