# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import copy
import unittest

import numpy as np
from shapely import Polygon, box

from solar_pv.evaluation.roof_polygons import polygon_metrics, triple_points
from solar_pv.roof_polygons.roof_polygons import _create_roof_polygons


def _grid(x0: int, x1: int, y0: int, y1: int) -> np.ndarray:
    xs, ys = np.meshgrid(np.arange(x0, x1) + 0.5, np.arange(y0, y1) + 0.5)
    return np.column_stack([xs.ravel(), ys.ravel()])


def _hipped_roof():
    """A 20x10m hipped roof, its ridge along y=5 from x=5 to x=15: (footprint, planes)."""
    xy = _grid(0, 20, 0, 10)
    x, y = xy[:, 0], xy[:, 1]
    face = np.argmin(np.column_stack([0.8 * y, 8 - 0.8 * y, 0.8 * x, 16 - 0.8 * x]), axis=1)
    slope = float(np.degrees(np.arctan(0.8)))
    coefs = [(0, 0.8, 0), (0, -0.8, 8), (0.8, 0, 0), (-0.8, 0, 16)]
    planes = [{"building_id": "b", "inliers_xy": xy[face == f], "aspect": aspect, "slope": slope,
               "is_flat": False, "x_coef": coefs[f][0], "y_coef": coefs[f][1], "intercept": coefs[f][2]}
              for f, aspect in enumerate((180, 0, 270, 90))]
    return box(0, 0, 20, 10), planes


def _metrics(footprint, planes, polygons, triples=()):
    return polygon_metrics(footprint, planes, [(k, g, g, True) for k, g in polygons], list(triples), 1.0, 0.1)


class RoofPolygonsEvalTest(unittest.TestCase):

    def test_gable_shares_an_edge_reaching_the_wall(self):
        footprint = box(0, 0, 10, 4)
        planes = [{"inliers_xy": _grid(0, 5, 0, 4)}, {"inliers_xy": _grid(5, 10, 0, 4)}]
        m, _ = _metrics(footprint, planes, [(0, box(0.1, 0.1, 5, 3.9)), (1, box(5, 0.1, 9.9, 3.9))])
        self.assertEqual((m["touching_pairs"], m["touching_unshared"]), (1, 0))
        self.assertEqual((m["shared_edges"], m["shared_edges_to_wall"]), (1, 1))
        self.assertEqual((m["vertices"], m["holes"], m["overlap"]), (8, 0, 0))
        self.assertAlmostEqual(m["shared_len"], 2 * 3.8, delta=0.01)
        self.assertAlmostEqual(m["free_len"], 0)

    def test_touching_planes_with_apart_polygons_share_no_edge(self):
        footprint = box(0, 0, 10, 4)
        planes = [{"inliers_xy": _grid(0, 5, 0, 4)}, {"inliers_xy": _grid(5, 10, 0, 4)}]
        m, _ = _metrics(footprint, planes, [(0, box(0.1, 0.1, 4.5, 3.9)), (1, box(5, 0.1, 9.9, 3.9))])
        self.assertEqual((m["touching_pairs"], m["touching_unshared"], m["shared_edges"]), (1, 1, 0))
        self.assertAlmostEqual(m["free_len"], 2 * 3.8, delta=0.01)

    def test_hole_between_polygons(self):
        footprint = box(0, 0, 10, 10)
        ring = Polygon([(0.1, 0.1), (9.9, 0.1), (9.9, 9.9), (0.1, 9.9)], [[(4, 4), (6, 4), (6, 6), (4, 6)]])
        m, _ = _metrics(footprint, [{"inliers_xy": _grid(0, 10, 0, 10)}], [(0, ring)])
        self.assertEqual(m["holes"], 1)
        self.assertAlmostEqual(m["hole_area"], 4)

    def test_triple_point_distance_to_where_polygons_meet(self):
        footprint = box(0, 0, 10, 10)
        planes = [{"inliers_xy": _grid(0, 5, 0, 10)}, {"inliers_xy": _grid(5, 10, 0, 5)},
                  {"inliers_xy": _grid(5, 10, 5, 10)}]
        polygons = [(0, box(0.1, 0.1, 5, 9.9)), (1, box(5, 0.1, 9.9, 5)), (2, box(5, 5, 9.9, 9.9))]
        m, dist = _metrics(footprint, planes, polygons, [((0, 1, 2), (5.2, 5.0)), ((0, 1, 2), (5.0, 5.0))])
        self.assertEqual((m["triples"], m["triples_shared"]), (2, 2))
        self.assertAlmostEqual(dist[0], 0.2)
        self.assertAlmostEqual(dist[1], 0)

        polygons[2] = (2, box(5.5, 5, 9.9, 9.9))
        m, dist = _metrics(footprint, planes, polygons, [((0, 1, 2), (5.0, 5.0))])
        self.assertEqual((m["triples"], m["triples_shared"], dist), (1, 0, [None]))

    def test_hipped_roof_triple_points(self):
        footprint, planes = _hipped_roof()
        triples = triple_points(footprint, planes, 1.0, 0.1)
        self.assertEqual(sorted(t for t, _ in triples), [(0, 1, 2), (0, 1, 3)])
        points = dict(triples)
        np.testing.assert_allclose(points[(0, 1, 2)], (5, 5), atol=1e-6)
        np.testing.assert_allclose(points[(0, 1, 3)], (15, 5), atol=1e-6)

        polygons = _create_roof_polygons(footprint, copy.deepcopy(planes), max_roof_slope_degrees=80,
                                         min_roof_area_m=8, min_roof_degrees_from_north=45, flat_roof_degrees=10,
                                         min_dist_to_edge_m=0.1, resolution_metres=1.0)
        m, dist = polygon_metrics(footprint, planes, [(k, p["roof_geom_27700"], p["roof_geom_raw_27700"], p["usable"])
                                                      for k, p in enumerate(polygons)], triples, 1.0, 0.1)
        self.assertEqual(m["triples_shared"], 2)
        self.assertLess(max(dist), 1e-6)
        self.assertEqual((m["touching_pairs"], m["touching_unshared"]), (5, 0))
        self.assertEqual(m["holes"], 0)
