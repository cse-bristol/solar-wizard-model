# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import os
import tempfile
import unittest

import numpy as np
import shapely
from osgeo import ogr

from solar_pv.evaluation import roof_labels


def _building(x0: float, size: int, slope: float = 0.0) -> dict:
    pixels = [{"x": x0 + i + 0.5, "y": j + 0.5, "elevation": 10.0, "slope": slope}
              for i in range(size) for j in range(size)]
    return {"polygon": shapely.box(x0, 0, x0 + size, size), "pixels": pixels, "max_ground_height": 5.0}


def _run(flat_usable: dict) -> list:
    return [{"building_id": b, "methods": {"run": {"usable_area": a, "flat_share": 1.0}}}
            for b, a in flat_usable.items()]


class PickTest(unittest.TestCase):

    def test_disagreeing_then_random_candidates(self):
        data = {"resolution": 1.0, "buildings": {
            "agree": _building(0, 10), "disagree": _building(100, 10), "big": _building(200, 70),
            "pitched": _building(300, 10, slope=30), "small_spread": _building(400, 10)}}
        runs = [_run({"agree": 50, "disagree": 0, "big": 0, "pitched": 0, "small_spread": 50}),
                _run({"agree": 50, "disagree": 80, "big": 4000, "pitched": 90, "small_spread": 60})]

        picked = roof_labels.pick(data, runs, n_disagree=5, n_random=5)

        self.assertEqual(sorted((b, s) for b, s, _ in picked),
                         [("agree", "random"), ("disagree", "disagree"), ("small_spread", "random")])


class LabelsTest(unittest.TestCase):

    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.dir.name, "labels.gpkg")
        self.data = {"resolution": 1.0, "buildings": {"a": _building(0, 10), "b": _building(100, 10)}}
        roof_labels.write_labels(self.path, self.data, [("a", "disagree", 30.0), ("b", "random", 0.0)])

    def tearDown(self):
        self.dir.cleanup()

    def _label(self, building_id: str, clear):
        ds = ogr.Open(self.path, 1)
        buildings = ds.GetLayerByName("building")
        for f in buildings:
            if f.GetField("building_id") == building_id:
                f.SetField("done", 1)
                f.SetField("confidence", "0.9")
                buildings.SetFeature(f)
        layer = ds.GetLayerByName("clear")
        for f in list(layer):
            if f.GetField("building_id") == building_id:
                layer.DeleteFeature(f.GetFID())
        if clear is not None:
            f = ogr.Feature(layer.GetLayerDefn())
            f.SetField("building_id", building_id)
            f.SetGeometry(ogr.ForceToMultiPolygon(ogr.CreateGeometryFromWkb(shapely.to_wkb(clear))))
            layer.CreateFeature(f)
        ds = None

    def test_only_done_buildings_are_loaded(self):
        self._label("a", shapely.box(0, 0, 5, 10))

        labels = roof_labels.load_labels(self.path)

        self.assertEqual(list(labels), ["a"])
        self.assertAlmostEqual(labels["a"]["clear"].area, 50)
        self.assertEqual(labels["a"]["stratum"], "disagree")
        self.assertEqual(labels["a"]["confidence"], 0.9)

    def test_building_with_no_clear_roof(self):
        self._label("b", None)

        labels = roof_labels.load_labels(self.path)

        self.assertTrue(labels["b"]["clear"].is_empty)

    def test_score_against_label(self):
        self._label("a", shapely.box(0, 0, 5, 10))
        label = roof_labels.load_labels(self.path)["a"]
        footprint = self.data["buildings"]["a"]["polygon"]
        polygons = [{"roof_geom_27700": shapely.box(2, 0, 8, 10), "usable": True, "is_flat": True, "slope": 1},
                    {"roof_geom_27700": shapely.box(0, 0, 2, 5), "usable": True, "is_flat": False, "slope": 8},
                    {"roof_geom_27700": shapely.box(8, 0, 10, 10), "usable": True, "is_flat": False, "slope": 30},
                    {"roof_geom_27700": shapely.box(0, 5, 2, 7), "usable": False, "is_flat": True, "slope": 1}]

        m = roof_labels.score_against_label(footprint, polygons, label)

        self.assertAlmostEqual(m["clear_m2"], 50)
        self.assertAlmostEqual(m["model_m2"], 60)
        self.assertAlmostEqual(m["hit_m2"], 30)
        self.assertAlmostEqual(m["low_model_m2"], 70)
        self.assertAlmostEqual(m["low_hit_m2"], 40)

        [row] = roof_labels.summarise([("run", m)], "all")
        self.assertEqual((row["precision"], row["recall"], row["iou"]), (0.5, 0.6, 0.375))
        self.assertEqual((row["false_usable"], row["missed"]), (0, 0))
        [row] = roof_labels.summarise([("run", m)], "all", "low_")
        self.assertEqual((row["precision"], row["recall"]), (0.571, 0.8))

    def test_confidence(self):
        self.assertEqual([roof_labels._confidence(v) for v in ("0.7", 0.2, "High", None, "")],
                         [0.7, 0.2, 1.0, 1.0, 1.0])


class LocalReliefTest(unittest.TestCase):

    def test_bump_on_flat_roof(self):
        xs, ys = np.meshgrid(np.arange(20) + 0.5, np.arange(20) + 0.5)
        xyz = np.column_stack([xs.ravel(), ys.ravel(), np.full(400, 10.0)])
        bump = (np.abs(xyz[:, 0] - 10) < 1) & (np.abs(xyz[:, 1] - 10) < 1)
        xyz[bump, 2] += 0.3

        relief = roof_labels.local_relief(xyz, 1.0)

        np.testing.assert_allclose(relief[bump], 0.3)
        np.testing.assert_allclose(relief[~bump], 0)
