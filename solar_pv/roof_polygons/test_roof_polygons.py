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

_METHODS = ("legacy", "mincut")


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
                                     min_dist_to_edge_m: float = 0.3,
                                     method: str = "legacy") -> Tuple[List[RoofPolygon], Polygon]:
    planes, building_geom = _load_test_data(building_id)
    planes = _create_roof_polygons(
        building_geom,
        planes,
        max_roof_slope_degrees=max_roof_slope_degrees,
        min_roof_area_m=min_roof_area_m,
        min_roof_degrees_from_north=min_roof_degrees_from_north,
        flat_roof_degrees=flat_roof_degrees,
        min_dist_to_edge_m=min_dist_to_edge_m,
        resolution_metres=1.0,
        method=method,
        debug=True)

    return planes, building_geom


def _create_polygons_from_lidar(building_id: str, min_dist_to_edge_m: float = 0.3):
    """Detect planes from a roof detection fixture, then make polygons, both by min-cut."""
    building = _load_data(join(paths.TEST_DATA, "roof_detection", f"{building_id}.json"))
    np.random.seed(42)
    planes = _detect_building_roof_planes(building, building_id, 1.0, method="mincut")
    polygons = _create_roof_polygons(
        building['polygon'],
        planes,
        max_roof_slope_degrees=80,
        min_roof_area_m=8,
        min_roof_degrees_from_north=45,
        flat_roof_degrees=10,
        min_dist_to_edge_m=min_dist_to_edge_m,
        resolution_metres=1.0,
        method="mincut")
    return polygons, building['polygon']


class RoofPolygonTest(ParameterisedTestCase):

    def test_mincut_from_lidar(self):
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
        def _do_test(building_id: str, method: str):
            planes, _ = _create_polygons_using_test_data(building_id, method=method)
            for i, p1 in enumerate(planes):
                for j, p2 in enumerate(planes):
                    poly1 = p1['roof_geom_27700']
                    poly2 = p2['roof_geom_27700']
                    if i != j:
                        crossover = poly1.intersection(poly2).area
                        assert crossover == 0, f"{method}: plane {i} overlaps {j} by {crossover} m2"

        self.parameterised_test([
            ("0001", "legacy", None),
            ("0005", "legacy", None),
        ] + [
            (building_id, "mincut", None)
            for building_id in ("0001", "0005", "0011", "0015", "0016")
        ], _do_test)

    def test_roof_polygons_stay_within_building(self):
        def _do_test(building_id: str, method: str):
            min_dist_to_edge_m = 0.55
            planes, building_geom = _create_polygons_using_test_data(
                building_id, min_dist_to_edge_m=min_dist_to_edge_m, method=method)
            building_geom = building_geom.buffer(-min_dist_to_edge_m, join_style=JOIN_STYLE.mitre)
            for i, p in enumerate(planes):
                poly = p['roof_geom_27700']
                crossover = poly.difference(building_geom).area
                assert crossover < 0.1, f"{method}: plane {i} overlaps -ve buffered building by {crossover} m2"

        self.parameterised_test([
            (building_id, method, None)
            for building_id in ("0001", "0005", "0011", "0015")
            for method in _METHODS
        ], _do_test)

    def test_failing_roof_polygons(self):
        def _do_test(building_id: str, min_dist_to_edge_m: float, method: str):
            _create_polygons_using_test_data(building_id, min_dist_to_edge_m=min_dist_to_edge_m, method=method)

        self.parameterised_test([
            (building_id, min_dist_to_edge_m, method, None)
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
            for method in _METHODS
        ], _do_test)

    def test_mincut_independent_of_plane_order(self):
        # flat, west and east faces; the meeting line between the faces nearly coincides
        # with their shared edge, which polygonizing once merged into the east face
        planes, building_geom = _load_test_data("0020")
        areas = []
        for order in itertools.permutations(range(len(planes))):
            polygons = _create_roof_polygons(
                building_geom, [copy.deepcopy(planes[i]) for i in order], max_roof_slope_degrees=80,
                min_roof_area_m=8, min_roof_degrees_from_north=45, flat_roof_degrees=10, min_dist_to_edge_m=0.3,
                resolution_metres=1.0, method="mincut")
            areas.append({p['plane_id']: p['roof_geom_27700'].area for p in polygons})
        for a in areas[1:]:
            self.assertEqual(a.keys(), areas[0].keys())
            for plane_id in a:
                self.assertAlmostEqual(a[plane_id], areas[0][plane_id], places=3)
        self.assertGreater(areas[0]["segmented_1"], 40)

    def test_merge(self):
        # "mincut" leaves merging planes to roof detection
        planes, _ = _create_polygons_using_test_data("0007", method="legacy")
        assert len(planes) == 2


def _flat_plane(xy: np.ndarray, z: float) -> dict:
    return {"building_id": "b", "inliers_xy": xy, "x_coef": 0.0, "y_coef": 0.0, "intercept": z,
            "slope": 0.0, "aspect": 180, "is_flat": True}


def _pitched_plane(xy: np.ndarray, x_coef: float, intercept: float) -> dict:
    return {"building_id": "b", "inliers_xy": xy, "x_coef": x_coef, "y_coef": 0.0, "intercept": intercept,
            "slope": float(np.degrees(np.arctan(abs(x_coef)))), "aspect": 270 if x_coef > 0 else 90,
            "is_flat": False}


def _grid(x0: int, x1: int, y0: int, y1: int) -> np.ndarray:
    xs, ys = np.meshgrid(np.arange(x0, x1) + 0.5, np.arange(y0, y1) + 0.5)
    return np.column_stack([xs.ravel(), ys.ravel()])


class LegacyCarveTest(ParameterisedTestCase):

    def test_polygon_kept_off_pixels_off_its_plane(self):
        # a flat roof at 10m with a 2x2 block of plant 1m above it, at its corner:
        xy = _grid(0, 10, 0, 10)
        z = np.where((xy[:, 0] > 8) & (xy[:, 1] > 8), 11.0, 10.0)
        plant = z > 10.5
        building = Polygon([(0, 0), (10, 0), (10, 10), (0, 10)])
        polygons = _create_roof_polygons(
            building, [_flat_plane(xy[~plant], 10.0)], max_roof_slope_degrees=80, min_roof_area_m=8,
            min_roof_degrees_from_north=45, flat_roof_degrees=10, min_dist_to_edge_m=0.1,
            resolution_metres=1.0, method="legacy", pixels_xyz=np.column_stack([xy, z]))
        self.assertEqual(len(polygons), 1)
        poly = polygons[0]['roof_geom_27700']
        self.assertFalse(poly.contains(Polygon([(8.4, 8.4), (8.6, 8.4), (8.6, 8.6)]).centroid))
        self.assertTrue(poly.contains(Polygon([(5, 5), (5.1, 5), (5.1, 5.1)]).centroid))

    def test_parallel_planes_at_a_step_not_merged(self):
        # two touching roof faces facing the same way at the same pitch, 1m apart in height:
        xy1, xy2 = _grid(0, 10, 0, 5), _grid(0, 10, 5, 10)
        z1, z2 = 0.5 * xy1[:, 0] + 10, 0.5 * xy2[:, 0] + 11
        building = Polygon([(0, 0), (10, 0), (10, 10), (0, 10)])
        pixels = np.column_stack([np.concatenate([xy1, xy2]), np.concatenate([z1, z2])])
        polygons = _create_roof_polygons(
            building, [_pitched_plane(xy1, 0.5, 10), _pitched_plane(xy2, 0.5, 11)], max_roof_slope_degrees=80,
            min_roof_area_m=8, min_roof_degrees_from_north=45, flat_roof_degrees=10, min_dist_to_edge_m=0.1,
            resolution_metres=1.0, method="legacy", pixels_xyz=pixels)
        self.assertEqual(len(polygons), 2)


def _plane(xy: np.ndarray, aspect: float, slope: float) -> dict:
    """Just what the mincut polygon method reads of a plane."""
    return {"building_id": "b", "inliers_xy": xy, "aspect": aspect, "slope": slope, "is_flat": slope < 5}


def _mincut(building: Polygon, planes: List[dict]) -> List[RoofPolygon]:
    return _create_roof_polygons(building, planes, max_roof_slope_degrees=80, min_roof_area_m=8,
                                 min_roof_degrees_from_north=45, flat_roof_degrees=10, min_dist_to_edge_m=0.1,
                                 resolution_metres=1.0, method="mincut")


def _shared_edge(polygons: List[RoofPolygon]):
    return polygons[0]['roof_geom_27700'].boundary.intersection(polygons[1]['roof_geom_27700'].boundary)


def _is_straight(line) -> bool:
    return len(shapely.get_coordinates(shapely.line_merge(line).simplify(1e-6))) == 2


class MincutLinesTest(ParameterisedTestCase):

    def test_ridge_straight_where_pixels_wobble_across_it(self):
        # a 20x10m gable, ridge along y=5, with the pixel assignment 2 rows out either side
        # of it in places:
        xy = _grid(0, 20, 0, 10)
        x, y = xy[:, 0], xy[:, 1]
        south = (y < 5) & ~(((x > 3) & (x < 7)) & (y > 3)) | ((x > 11) & (x < 15) & (y < 7))
        building = Polygon([(0, 0), (20, 0), (20, 10), (0, 10)])
        planes = [_plane(xy[south], 180, 30), _plane(xy[~south], 0, 30)]

        polygons = _mincut(building, copy.deepcopy(planes))
        edge = _shared_edge(polygons)
        self.assertTrue(_is_straight(edge))
        self.assertGreater(edge.length, 19)
        # the wobble is symmetric, so y=4.5 separates the pixels as well as y=5:
        np.testing.assert_allclose(shapely.get_coordinates(edge)[:, 1], 5, atol=0.51)

        with patch.object(roof_polygons, "CORE_HALF_WIDTH_TILES", 0):
            self.assertFalse(_is_straight(_shared_edge(_mincut(building, copy.deepcopy(planes)))))

    def test_step_straight_along_grid_axis(self):
        # two roofs facing about south side by side at a step along y=5, the pixel
        # assignment jogging a row across it in one place:
        xy = _grid(0, 20, 0, 10)
        x, y = xy[:, 0], xy[:, 1]
        lower = (y < 5) & ~((x > 4) & (x < 8) & (y > 4))
        building = Polygon([(0, 0), (20, 0), (20, 10), (0, 10)])
        planes = [_plane(xy[lower], 180, 30), _plane(xy[~lower], 176, 34)]

        edge = _shared_edge(_mincut(building, copy.deepcopy(planes)))
        self.assertTrue(_is_straight(edge))
        self.assertGreater(edge.length, 19)

        with patch.object(roof_polygons, "STEP_LINES", False):
            self.assertFalse(_is_straight(_shared_edge(_mincut(building, copy.deepcopy(planes)))))

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

        polygons = _mincut(building, copy.deepcopy(planes))
        for main_face, valley in ((0, lambda p: p[:, 0] - p[:, 1] - 7.5), (1, lambda p: p[:, 0] + p[:, 1] - 17.5)):
            edge = polygons[main_face]['roof_geom_27700'].boundary.intersection(polygons[2]['roof_geom_27700'].boundary)
            # the last metre or so, where four faces meet at the ridge, is split differently
            self.assertGreater(edge.length, 4)
            dist = np.abs(valley(shapely.get_coordinates(edge))) / np.sqrt(2)
            self.assertLess(dist.max(), 0.5)

        with patch.object(roof_polygons, "LINE_END_TILES", 100):
            owned = [p['inliers_xy'] for p in planes]
            lines = roof_polygons._meeting_lines(planes, owned, [roof_polygons._pixel_region(o, 1.0) for o in owned], 1.0)
            self.assertNotIn((0, 2), lines)
            self.assertNotIn((1, 2), lines)

    def test_direction_sensitivity(self):
        ridge = roof_polygons._direction_sensitivity({"aspect": 180, "slope": 30}, {"aspect": 0, "slope": 30})
        hip = roof_polygons._direction_sensitivity({"aspect": 180, "slope": 30}, {"aspect": 90, "slope": 30})
        step = roof_polygons._direction_sensitivity({"aspect": 182, "slope": 30}, {"aspect": 178, "slope": 34})
        self.assertLess(ridge, 5)
        self.assertLess(hip, 10)
        self.assertGreater(step, 30)


class MincutSmoothTest(ParameterisedTestCase):

    def test_staircase_edge_smoothed(self):
        # a flat roof whose pixels stop along a diagonal, short of the building's edge:
        xy = _grid(0, 15, 0, 15)
        xy = xy[xy.sum(axis=1) < 15]
        building = Polygon([(0, 0), (15, 0), (15, 15), (0, 15)])
        planes = [_plane(xy, 180, 0)]

        smooth = _mincut(building, copy.deepcopy(planes))[0]['roof_geom_27700']
        self.assertLessEqual(len(smooth.exterior.coords), 5)
        # cutting the staircase's corners loses a little of its pixels
        self.assertGreater(smooth.area, len(xy) * 0.9)

        with patch.object(roof_polygons, "SMOOTH_TOLERANCE_TILES", 0):
            stairs = _mincut(building, copy.deepcopy(planes))[0]['roof_geom_27700']
            self.assertGreater(len(stairs.exterior.coords), 20)
