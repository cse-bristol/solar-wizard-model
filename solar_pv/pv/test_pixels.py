# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import unittest

import numpy as np
from shapely.geometry import box

from solar_pv.pv import pixels


class PixelsForGeomsTest(unittest.TestCase):

    def setUp(self):
        # 10x10 grid, 1 m cells, north-up; value encodes row/col as r*10 + c.
        self.gt = (1000.0, 1.0, 0.0, 1050.0, 0.0, -1.0)
        rr, cc = np.meshgrid(np.arange(10), np.arange(10), indexing="ij")
        self.arrays = {"kwh_year": (rr * 10 + cc).astype(float),
                       "horizon_00": np.zeros((10, 10))}

    def _fields(self, valid=None):
        return pixels.PixelFields.from_dense(self.arrays, self.gt, valid=valid)

    def test_selects_pixels_whose_centre_is_inside(self):
        # a 3x3 m box -> the 9 pixels whose centres fall inside:
        geom = box(1002.0, 1045.0, 1005.0, 1048.0)
        out = pixels.pixels_for_geoms(self._fields(), {"t1": geom})
        px = out["t1"]
        self.assertEqual(len(px), 9)
        # rows 2..4, cols 2..4 (y = 1049.5 - r, x = 1000.5 + c):
        vals = sorted(p["kwh_year"] for p in px)
        expected = sorted(float(r * 10 + c) for r in (2, 3, 4) for c in (2, 3, 4))
        self.assertEqual(vals, expected)

    def test_pixel_dict_shape_matches_seam(self):
        geom = box(1002.0, 1045.0, 1005.0, 1048.0)
        px = pixels.pixels_for_geoms(self._fields(), {"t1": geom})["t1"][0]
        self.assertEqual(set(px), {"toid", "x", "y", "pixel_id", "kwh_year", "horizon_00"})
        self.assertEqual(px["toid"], "t1")
        self.assertEqual(px["pixel_id"], f"t1:{px['x']}:{px['y']}")

    def test_building_outside_grid_returns_nothing(self):
        geom = box(5000.0, 5000.0, 5001.0, 5001.0)
        out = pixels.pixels_for_geoms(self._fields(), {"t1": geom})
        self.assertNotIn("t1", out)

    def test_multiple_buildings_keyed_by_toid(self):
        out = pixels.pixels_for_geoms(self._fields(), {
            "a": box(1001.0, 1047.0, 1003.0, 1049.0),
            "b": box(1006.0, 1041.0, 1008.0, 1043.0)})
        self.assertEqual(set(out), {"a", "b"})
        self.assertTrue(all(p["toid"] == "a" for p in out["a"]))

    def test_no_data_pixels_are_dropped(self):
        # a valid mask covering only some cells -> pixels outside it are not returned even when
        # their centre falls in the geom:
        valid = np.zeros((10, 10), dtype=bool)
        valid[2, 2] = valid[2, 3] = True  # two of the nine cells in the box below
        geom = box(1002.0, 1045.0, 1005.0, 1048.0)
        out = pixels.pixels_for_geoms(self._fields(valid), {"t1": geom})
        self.assertEqual(len(out["t1"]), 2)


if __name__ == "__main__":
    unittest.main()
