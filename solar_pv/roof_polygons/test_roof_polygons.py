# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import copy
import itertools
import json
from os.path import join
from typing import Tuple, List
from unittest.mock import patch

import numpy as np
import shapely
from shapely import wkt, Polygon
from shapely.geometry import JOIN_STYLE

from solar_pv import paths
from solar_pv.roof_detection.detect_roofs import _detect_building_roof_planes
from solar_pv.roof_detection.test_detect_roofs import _load_data
from solar_pv.datatypes import RoofPolygon
from solar_pv.roof_polygons import roof_polygons
from solar_pv.roof_polygons.roof_polygons import _create_roof_polygons
from solar_pv.test_utils.test_funcs import ParameterisedTestCase


def _load_test_data(building_id: str):
    roof_polys_dir = join(paths.TEST_DATA, "roof_polygons")
    with open(join(roof_polys_dir, f"{building_id}.json")) as f:
        data = json.load(f)
        planes = data['planes']
        for plane in planes:
            plane['inliers_xy'] = np.array(plane['inliers_xy'])
            # fix up old test data...
            if 'is_flat' not in plane:
                plane['is_flat'] = plane['slope'] <= 5
        building_geom = wkt.loads(data['building_geom'])
    return planes, building_geom


def _create_polygons_using_test_data(building_id: str,
                                     max_roof_slope_degrees: int = 80,
                                     min_roof_area_m: int = 8,
                                     min_roof_degrees_from_north: int = 45,
                                     flat_roof_degrees: int = 10,
                                     min_dist_to_edge_m: float = 0.3) -> Tuple[List[RoofPolygon], Polygon]:
    planes, building_geom = _load_test_data(building_id)
    planes = _create_roof_polygons(
        building_geom,
        planes,
        max_roof_slope_degrees=max_roof_slope_degrees,
        min_roof_area_m=min_roof_area_m,
        min_roof_degrees_from_north=min_roof_degrees_from_north,
        flat_roof_degrees=flat_roof_degrees,
        min_dist_to_edge_m=min_dist_to_edge_m,
        resolution_metres=1.0)

    return planes, building_geom


def _create_polygons_from_lidar(building_id: str, min_dist_to_edge_m: float = 0.3):
    """Detect planes from a roof detection fixture, then make polygons."""
    building = _load_data(join(paths.TEST_DATA, "roof_detection", f"{building_id}.json"))
    planes = _detect_building_roof_planes(building, building_id, 1.0)
    polygons = _create_roof_polygons(
        building['polygon'],
        planes,
        max_roof_slope_degrees=80,
        min_roof_area_m=8,
        min_roof_degrees_from_north=45,
        flat_roof_degrees=10,
        min_dist_to_edge_m=min_dist_to_edge_m,
        resolution_metres=1.0)
    return polygons, building['polygon']


class RoofPolygonTest(ParameterisedTestCase):

    def test_from_lidar(self):
        def _do_test(building_id: str):
            min_dist_to_edge_m = 0.3
            polygons, building_geom = _create_polygons_from_lidar(building_id, min_dist_to_edge_m)
            assert len(polygons) > 0
            shrunk = building_geom.buffer(-min_dist_to_edge_m, join_style=JOIN_STYLE.mitre)
            for i, p1 in enumerate(polygons):
                crossover = p1['roof_geom_27700'].difference(shrunk).area
                assert crossover < 0.1, f"plane {i} overlaps -ve buffered building by {crossover} m2"
                for j, p2 in enumerate(polygons):
                    if i != j:
                        crossover = p1['roof_geom_27700'].intersection(p2['roof_geom_27700']).area
                        assert crossover == 0, f"plane {i} overlaps {j} by {crossover} m2"

        self.parameterised_test([
            (building_id, None)
            for building_id in ("0004", "0005", "0007", "0015", "0020", "0022", "0026", "0032")
        ], _do_test)

    def test_roof_polygons_do_not_overlap(self):
        def _do_test(building_id: str):
            planes, _ = _create_polygons_using_test_data(building_id)
            for i, p1 in enumerate(planes):
                for j, p2 in enumerate(planes):
                    poly1 = p1['roof_geom_27700']
                    poly2 = p2['roof_geom_27700']
                    if i != j:
                        crossover = poly1.intersection(poly2).area
                        assert crossover == 0, f"plane {i} overlaps {j} by {crossover} m2"

        self.parameterised_test([
            (building_id, None)
            for building_id in ("0001", "0005", "0011", "0015", "0016")
        ], _do_test)

    def test_roof_polygons_stay_within_building(self):
        def _do_test(building_id: str):
            min_dist_to_edge_m = 0.55
            planes, building_geom = _create_polygons_using_test_data(
                building_id, min_dist_to_edge_m=min_dist_to_edge_m)
            building_geom = building_geom.buffer(-min_dist_to_edge_m, join_style=JOIN_STYLE.mitre)
            for i, p in enumerate(planes):
                poly = p['roof_geom_27700']
                crossover = poly.difference(building_geom).area
                assert crossover < 0.1, f"plane {i} overlaps -ve buffered building by {crossover} m2"

        self.parameterised_test([
            (building_id, None)
            for building_id in ("0001", "0005", "0011", "0015")
        ], _do_test)

    def test_failing_roof_polygons(self):
        def _do_test(building_id: str, min_dist_to_edge_m: float):
            _create_polygons_using_test_data(building_id, min_dist_to_edge_m=min_dist_to_edge_m)

        self.parameterised_test([
            (building_id, min_dist_to_edge_m, None)
            for building_id, min_dist_to_edge_m in (
                ("0008", 0.55),
                ("0004", 0.55),
                ("0006", 0.55),
                ("0003", 0.55),
                ("0009", 0.4),
                ("0011", 0.1),
                ("0010", 0.1),
                ("0013", 0.1),
                ("0012", 0.1),
                ("0014", 0.1),
                ("0015", 0.1),
                ("0016", 0.1),
                ("0017", 0.1),
                ("0018", 0.1),
                ("0019", 0.3),
            )
        ], _do_test)

    def test_independent_of_plane_order(self):
        # flat, west and east faces; the meeting line between the faces nearly coincides
        # with their shared edge, which polygonizing once merged into the east face
        planes, building_geom = _load_test_data("0020")
        areas = []
        for order in itertools.permutations(range(len(planes))):
            polygons = _create_roof_polygons(
                building_geom, [copy.deepcopy(planes[i]) for i in order], max_roof_slope_degrees=80,
                min_roof_area_m=8, min_roof_degrees_from_north=45, flat_roof_degrees=10, min_dist_to_edge_m=0.3,
                resolution_metres=1.0)
            areas.append({p['plane_id']: p['roof_geom_27700'].area for p in polygons})
        for a in areas[1:]:
            self.assertEqual(a.keys(), areas[0].keys())
            for plane_id in a:
                self.assertAlmostEqual(a[plane_id], areas[0][plane_id], places=3)
        self.assertGreater(areas[0]["segmented_1"], 40)


def _grid(x0: int, x1: int, y0: int, y1: int) -> np.ndarray:
    xs, ys = np.meshgrid(np.arange(x0, x1) + 0.5, np.arange(y0, y1) + 0.5)
    return np.column_stack([xs.ravel(), ys.ravel()])


def _plane(xy: np.ndarray, aspect: float, slope: float, coefs: Tuple[float, float, float] = None) -> dict:
    """Just what roof polygons read of a plane. With no `coefs`, no nodes are placed."""
    plane = {"building_id": "b", "inliers_xy": xy, "aspect": aspect, "slope": slope, "is_flat": slope < 5}
    if coefs is not None:
        plane["x_coef"], plane["y_coef"], plane["intercept"] = coefs
    return plane


def _polygons(building: Polygon, planes: List[dict]) -> List[RoofPolygon]:
    return _create_roof_polygons(building, planes, max_roof_slope_degrees=80, min_roof_area_m=8,
                                 min_roof_degrees_from_north=45, flat_roof_degrees=10, min_dist_to_edge_m=0.1,
                                 resolution_metres=1.0)


def _shared_edge(polygons: List[RoofPolygon]):
    return polygons[0]['roof_geom_27700'].boundary.intersection(polygons[1]['roof_geom_27700'].boundary)


def _is_straight(line) -> bool:
    return len(shapely.get_coordinates(shapely.line_merge(line).simplify(1e-6))) == 2


class LinesTest(ParameterisedTestCase):

    def test_ridge_straight_where_pixels_wobble_across_it(self):
        # a 20x10m gable, ridge along y=5, with the pixel assignment 2 rows out either side
        # of it in places:
        xy = _grid(0, 20, 0, 10)
        x, y = xy[:, 0], xy[:, 1]
        south = (y < 5) & ~(((x > 3) & (x < 7)) & (y > 3)) | ((x > 11) & (x < 15) & (y < 7))
        building = Polygon([(0, 0), (20, 0), (20, 10), (0, 10)])
        planes = [_plane(xy[south], 180, 30), _plane(xy[~south], 0, 30)]

        polygons = _polygons(building, copy.deepcopy(planes))
        edge = _shared_edge(polygons)
        self.assertTrue(_is_straight(edge))
        self.assertGreater(edge.length, 19)
        # the wobble is symmetric, so y=4.5 separates the pixels as well as y=5:
        np.testing.assert_allclose(shapely.get_coordinates(edge)[:, 1], 5, atol=0.51)

    def test_step_straight_along_grid_axis(self):
        # two roofs facing about south side by side at a step along y=5, the pixel
        # assignment jogging a row across it in one place:
        xy = _grid(0, 20, 0, 10)
        x, y = xy[:, 0], xy[:, 1]
        lower = (y < 5) & ~((x > 4) & (x < 8) & (y > 4))
        building = Polygon([(0, 0), (20, 0), (20, 10), (0, 10)])
        planes = [_plane(xy[lower], 180, 30), _plane(xy[~lower], 176, 34)]

        edge = _shared_edge(_polygons(building, copy.deepcopy(planes)))
        self.assertTrue(_is_straight(edge))
        self.assertGreater(edge.length, 19)

    def test_valleys_of_cross_gable(self):
        # a gable along x (ridge y=5, z=10) meeting a higher cross gable along y (ridge
        # x=15, z=12). The cross gable's west face meets the main roof's faces in valleys
        # that end at the main ridge, and carries on past where each valley line would run.
        xy = _grid(0, 20, 0, 10)
        x, y = xy[:, 0], xy[:, 1]
        main = np.where(x < 15, 10 - 0.8 * np.abs(y - 5), -np.inf)
        cross = np.where(x > 7.5, 12 - 0.8 * np.abs(x - 15), -np.inf)
        on_cross = cross > main
        slope = float(np.degrees(np.arctan(0.8)))
        building = Polygon([(0, 0), (20, 0), (20, 10), (0, 10)])
        planes = [_plane(xy[~on_cross & (y < 5)], 180, slope), _plane(xy[~on_cross & (y > 5)], 0, slope),
                  _plane(xy[on_cross & (x < 15)], 270, slope), _plane(xy[on_cross & (x > 15)], 90, slope)]

        polygons = _polygons(building, copy.deepcopy(planes))
        for main_face, valley in ((0, lambda p: p[:, 0] - p[:, 1] - 7.5), (1, lambda p: p[:, 0] + p[:, 1] - 17.5)):
            edge = polygons[main_face]['roof_geom_27700'].boundary.intersection(polygons[2]['roof_geom_27700'].boundary)
            # the last metre or so, where four faces meet at the ridge, is split differently
            self.assertGreater(edge.length, 4)
            dist = np.abs(valley(shapely.get_coordinates(edge))) / np.sqrt(2)
            self.assertLess(dist.max(), 0.5)

        with patch.object(roof_polygons, "LINE_END_TILES", 100):
            owned = [p['inliers_xy'] for p in planes]
            lines = roof_polygons._meeting_lines(planes, owned, [roof_polygons._pixel_region(o, 1.0) for o in owned],
                                                 building.buffer(-0.1), 1.0)
            self.assertNotIn((0, 2), lines)
            self.assertNotIn((1, 2), lines)

    def test_hips_meet_ridge_at_one_point(self):
        # a 20x10m hipped roof, ridge along y=5 from x=5 to x=15, with no pixels assigned
        # within 1.5m of either end of the ridge:
        xy = _grid(0, 20, 0, 10)
        x, y = xy[:, 0], xy[:, 1]
        heights = np.column_stack([0.8 * y, 8 - 0.8 * y, 0.8 * x, 16 - 0.8 * x])
        face = np.argmin(heights, axis=1)
        unassigned = (np.hypot(x - 5, y - 5) < 1.5) | (np.hypot(x - 15, y - 5) < 1.5)
        slope = float(np.degrees(np.arctan(0.8)))
        building = Polygon([(0, 0), (20, 0), (20, 10), (0, 10)])
        coefs = [(0, 0.8, 0), (0, -0.8, 8), (0.8, 0, 0), (-0.8, 0, 16)]
        planes = [_plane(xy[(face == f) & ~unassigned], aspect, slope, coefs[f])
                  for f, aspect in enumerate((180, 0, 270, 90))]

        def common_points(polygons):
            g = [p['roof_geom_27700'] for p in polygons]
            return [g[0].intersection(g[1]).intersection(g[k]) for k in (2, 3)]

        west, east = common_points(_polygons(building, copy.deepcopy(planes)))
        self.assertLess(west.distance(shapely.Point(5, 5)), 1e-6)
        self.assertLess(east.distance(shapely.Point(15, 5)), 1e-6)

    def test_line_applies_only_beside_it_at_node(self):
        # lines from a node at 0°, 40° and 80°, the last between plane 2 (40°-80°) and plane
        # 1, whose sector (80°-360°) is wider than 180°:
        def ray(angle_deg: float):
            u = np.array([np.cos(np.radians(angle_deg)), np.sin(np.radians(angle_deg))])
            n = np.array([u[1], -u[0]])
            shared = np.array([t * u + o * n for t in (1, 2, 3, 4, 5) for o in (-0.5, 0.5)])
            return roof_polygons._Line(point=np.zeros(2), normal=n, direction=1.0, shared_xy=shared,
                                       start=0.0, end=5.5, meets=True, at_node=[True, False])

        rays = {(0, 1): ray(0), (0, 2): ray(40), (1, 2): ray(80)}
        lines = {pair: roof_polygons._restrict(line, 1.0, 2) for pair, line in rays.items()}
        # in plane 1's sector, past where the 80° line would run on through the node, so on
        # plane 2's side of that line:
        point = shapely.Point(2 * np.cos(np.radians(-10)), 2 * np.sin(np.radians(-10)))
        self.assertTrue(lines[(1, 2)].zone.contains(point))
        roof_polygons._clip_at_nodes(lines)
        self.assertFalse(lines[(1, 2)].zone.contains(point))
        self.assertTrue(lines[(1, 2)].zone.contains(shapely.Point(0.5, 3)))

    def test_ridge_reaches_gable_wall(self):
        # a 20x10m gable, ridge along y=5, with no pixels assigned by either gable wall
        # next to the ridge, more of them on the north side:
        xy = _grid(0, 20, 0, 10)
        x, y = xy[:, 0], xy[:, 1]
        xy = xy[~(((x < 1) | (x > 19)) & (y > 4) & (y < 7))]
        south = xy[:, 1] < 5
        building = Polygon([(0, 0), (20, 0), (20, 10), (0, 10)])
        planes = [_plane(xy[south], 180, 30), _plane(xy[~south], 0, 30)]

        edge = _shared_edge(_polygons(building, copy.deepcopy(planes)))
        self.assertTrue(_is_straight(edge))
        x = shapely.get_coordinates(edge)[:, 0]
        self.assertAlmostEqual(x.min(), 0.1)
        self.assertAlmostEqual(x.max(), 19.9)

        with patch.object(roof_polygons, "WALL_MAX_TILES", 0), patch.object(roof_polygons, "MEETING_WALL_MAX_TILES", 0):
            self.assertFalse(_is_straight(_shared_edge(_polygons(building, copy.deepcopy(planes)))))

    def test_direction_sensitivity(self):
        ridge = roof_polygons._direction_sensitivity({"aspect": 180, "slope": 30}, {"aspect": 0, "slope": 30})
        hip = roof_polygons._direction_sensitivity({"aspect": 180, "slope": 30}, {"aspect": 90, "slope": 30})
        step = roof_polygons._direction_sensitivity({"aspect": 182, "slope": 30}, {"aspect": 178, "slope": 34})
        self.assertLess(ridge, 5)
        self.assertLess(hip, 10)
        self.assertGreater(step, 30)


class SmoothTest(ParameterisedTestCase):

    def test_staircase_edge_smoothed(self):
        # a flat roof whose pixels stop along a diagonal, short of the building's edge:
        xy = _grid(0, 15, 0, 15)
        xy = xy[xy.sum(axis=1) < 15]
        building = Polygon([(0, 0), (15, 0), (15, 15), (0, 15)])
        planes = [_plane(xy, 180, 0)]

        smooth = _polygons(building, copy.deepcopy(planes))[0]['roof_geom_27700']
        self.assertLessEqual(len(smooth.exterior.coords), 5)
        # cutting the staircase's corners loses a little of its pixels
        self.assertGreater(smooth.area, len(xy) * 0.9)


class ObstaclesTest(ParameterisedTestCase):

    def _flat_roof(self, obstacle):
        xy = _grid(0, 20, 0, 20)
        plane = dict(_plane(xy[~obstacle(xy)], 180, 0), plane_id="p", obstacles_xy=xy[obstacle(xy)])
        return _polygons(Polygon([(0, 0), (20, 0), (20, 20), (0, 20)]), [plane])

    def test_obstacle_cut_out_with_margin(self):
        polygons = self._flat_roof(lambda xy: (np.abs(xy[:, 0] - 10) < 1) & (np.abs(xy[:, 1] - 10) < 1))

        self.assertEqual(len(polygons), 1)
        roof = polygons[0]['roof_geom_27700']
        self.assertEqual(len(roof.interiors), 1)
        hole = 2 + 2 * roof_polygons.OBSTACLE_SETBACK_M
        self.assertAlmostEqual(Polygon(roof.interiors[0]).area, hole * hole, places=6)

    def test_obstacles_across_roof_leave_its_largest_piece(self):
        polygons = self._flat_roof(lambda xy: np.abs(xy[:, 1] - 6) < 1)

        self.assertEqual([p['plane_id'] for p in polygons], ["p"])
        roof = polygons[0]['roof_geom_27700']
        self.assertEqual(roof.geom_type, "Polygon")
        self.assertGreaterEqual(roof.bounds[1], 7 + roof_polygons.OBSTACLE_SETBACK_M - 1e-6)
