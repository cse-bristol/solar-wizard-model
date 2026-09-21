# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import os
import shutil
import tempfile
import unittest
from os.path import join
from typing import Optional, Tuple

import numpy as np
from osgeo import gdal, osr

from solar_pv import gdal_helpers
from solar_pv.lidar.lidar import LidarTile, Resolution, LIDAR_NODATA
from solar_pv.lidar import lidar_selector

gdal.UseExceptions()

# A 20m x 20m job area, upper-left at a valid British National Grid location:
_ULX = 400_000
_ULY = 100_000
_SIZE_M = 20
_BOUNDS: Tuple[float, float, float, float] = (_ULX, _ULY - _SIZE_M, _ULX + _SIZE_M, _ULY)


def _mk(path: str, res: float, value: float,
        ulx: float = _ULX, uly: float = _ULY, size_m: float = _SIZE_M,
        nodata: float = LIDAR_NODATA,
        hole: Optional[Tuple[slice, slice]] = None) -> str:
    """Write a square single-band 27700 GeoTIFF of `size_m` filled with `value`."""
    n = int(size_m / res)
    ds = gdal.GetDriverByName('GTiff').Create(path, n, n, 1, gdal.GDT_Float32)
    ds.SetGeoTransform((ulx, res, 0, uly, 0, -res))
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(27700)
    ds.SetProjection(srs.ExportToWkt())
    arr = np.full((n, n), value, dtype=np.float32)
    if hole is not None:
        arr[hole] = nodata
    band = ds.GetRasterBand(1)
    band.SetNoDataValue(nodata)
    band.WriteArray(arr)
    ds.FlushCache()
    return path


class LidarSelectorTest(unittest.TestCase):
    def setUp(self):
        self.d = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.d, ignore_errors=True)

    def _tile(self, name, res, value, year=None, resolution=None, **kw) -> LidarTile:
        path = _mk(join(self.d, name), res, value, **kw)
        return LidarTile(filename=path, year=year, resolution=resolution)

    def _vrt_value_at_centre(self, paths) -> float:
        vrt = join(self.d, "out.vrt")
        gdal_helpers.create_vrt(paths, vrt)
        ds = gdal.Open(vrt)
        arr = ds.GetRasterBand(1).ReadAsArray()
        return float(arr[arr.shape[0] // 2, arr.shape[1] // 2])

    # --- coverage / target resolution --------------------------------------

    def test_full_1m_gives_1m(self):
        tiles = [self._tile("a_1m.tif", 1.0, 10, resolution=Resolution.R_1M)]
        by_res = lidar_selector._by_resolution(tiles)
        self.assertEqual(Resolution.R_1M,
                         lidar_selector._target_resolution(by_res, _BOUNDS))

    def test_full_50cm_counts_as_1m(self):
        # 50cm is never worked at directly, but its coverage counts towards 1m,
        # so full 50cm coverage keeps the target at 1m rather than dropping to 2m:
        tiles = [self._tile("a_50cm.tif", 0.5, 10, resolution=Resolution.R_50CM)]
        by_res = lidar_selector._by_resolution(tiles)
        self.assertEqual(Resolution.R_1M,
                         lidar_selector._target_resolution(by_res, _BOUNDS))

    def test_sparse_1m_full_2m_gives_2m(self):
        # 1m only covers a 2x2m corner (cov 0.01 < 0.25), 2m covers all (cov ~1):
        sparse_1m = self._tile("s_1m.tif", 1.0, 10, size_m=2, resolution=Resolution.R_1M)
        full_2m = self._tile("f_2m.tif", 2.0, 20, resolution=Resolution.R_2M)
        by_res = lidar_selector._by_resolution([sparse_1m, full_2m])
        self.assertEqual(Resolution.R_2M,
                         lidar_selector._target_resolution(by_res, _BOUNDS))

    def test_coverage_respects_nodata(self):
        # Half the tile is nodata -> ~0.5 coverage:
        n = int(_SIZE_M / 1.0)
        half_hole = self._tile("half_1m.tif", 1.0, 10, resolution=Resolution.R_1M,
                               hole=(slice(0, n // 2), slice(None)))
        cov = lidar_selector._coverage([half_hole], Resolution.R_1M, _BOUNDS)
        self.assertAlmostEqual(0.5, cov, places=2)

    # --- merge ordering (newest / highest-res wins on overlap) --------------

    def test_newest_year_wins_on_overlap(self):
        old = self._tile("old_1m.tif", 1.0, 10, year=2015, resolution=Resolution.R_1M)
        new = self._tile("new_1m.tif", 1.0, 20, year=2020, resolution=Resolution.R_1M)
        paths, res = lidar_selector.select_lidar([new, old], _BOUNDS, _BOUNDS, self.d)
        self.assertEqual(Resolution.R_1M, res)
        self.assertEqual(20, self._vrt_value_at_centre(paths))

    def test_highest_res_wins_same_year(self):
        # Same year: the higher-resolution (50cm) tile wins, resampled onto the 1m grid:
        one_m = self._tile("d_1m.tif", 1.0, 40, year=2015, resolution=Resolution.R_1M)
        fifty = self._tile("c_50cm.tif", 0.5, 30, year=2015, resolution=Resolution.R_50CM)
        paths, res = lidar_selector.select_lidar([one_m, fifty], _BOUNDS, _BOUNDS, self.d)
        self.assertEqual(Resolution.R_1M, res)
        self.assertEqual(30, self._vrt_value_at_centre(paths))

    def test_2m_excluded_when_target_is_1m(self):
        one_m = self._tile("e_1m.tif", 1.0, 10, resolution=Resolution.R_1M)
        two_m = self._tile("e_2m.tif", 2.0, 20, resolution=Resolution.R_2M)
        paths, res = lidar_selector.select_lidar([one_m, two_m], _BOUNDS, _BOUNDS, self.d)
        self.assertEqual(Resolution.R_1M, res)
        # 2m is never upsampled into a 1m target, so only the 1m value appears:
        self.assertEqual(10, self._vrt_value_at_centre(paths))

    def test_extent_bounds_excludes_far_tiles(self):
        near = self._tile("near_1m.tif", 1.0, 10, resolution=Resolution.R_1M)
        far = self._tile("far_1m.tif", 1.0, 20, resolution=Resolution.R_1M,
                        ulx=_ULX + 10_000, uly=_ULY + 10_000)
        paths, _ = lidar_selector.select_lidar([near, far], _BOUNDS, _BOUNDS, self.d)
        self.assertEqual(1, len(paths))

    # --- usable tile count (skip decision) ----------------------------------

    def test_count_usable_tiles(self):
        one_m = self._tile("g_1m.tif", 1.0, 10, resolution=Resolution.R_1M)
        two_m = self._tile("g_2m.tif", 2.0, 20, resolution=Resolution.R_2M)
        # target is 1m, so the 2m tile is not usable:
        self.assertEqual(1, lidar_selector.count_usable_tiles([one_m, two_m], _BOUNDS))

    def test_count_usable_tiles_none_intersect(self):
        far = self._tile("h_1m.tif", 1.0, 10, resolution=Resolution.R_1M,
                        ulx=_ULX + 10_000, uly=_ULY + 10_000)
        self.assertEqual(0, lidar_selector.count_usable_tiles([far], _BOUNDS))

    # --- pass-through vs resample ------------------------------------------

    def test_target_res_tile_used_as_is(self):
        # A 1m, grid-aligned, -9999-nodata tile needs no resampling for a 1m
        # target, so it feeds the VRT unchanged rather than being rewritten:
        one_m = self._tile("keep_1m.tif", 1.0, 10, resolution=Resolution.R_1M)
        paths, _ = lidar_selector.select_lidar([one_m], _BOUNDS, _BOUNDS, self.d)
        self.assertEqual([one_m.filename], paths)

    def test_coarser_tile_is_resampled(self):
        # 50cm into a 1m target must be resampled:
        fifty = self._tile("c_50cm.tif", 0.5, 30, resolution=Resolution.R_50CM)
        paths, _ = lidar_selector.select_lidar([fifty], _BOUNDS, _BOUNDS, self.d)
        self.assertNotEqual([fifty.filename], paths)
        self.assertTrue(paths[0].startswith(self.d))

    def test_off_grid_tile_is_resampled(self):
        # Right resolution but origin off the grid -> resampled so it aligns:
        off = self._tile("off_1m.tif", 1.0, 10, resolution=Resolution.R_1M,
                        ulx=_ULX + 0.3, uly=_ULY + 0.3)
        self.assertFalse(lidar_selector._on_target_grid(off.filename, 1.0))

    def test_foreign_nodata_tile_is_resampled(self):
        # A different nodata must be remapped to LIDAR_NODATA, which the resample
        # does - so such a tile is never used as-is:
        foreign = self._tile("nd_1m.tif", 1.0, 10, resolution=Resolution.R_1M,
                            nodata=-3.0e38)
        self.assertFalse(lidar_selector._on_target_grid(foreign.filename, 1.0))
        paths, _ = lidar_selector.select_lidar([foreign], _BOUNDS, _BOUNDS, self.d)
        self.assertTrue(paths[0].startswith(self.d))

    # --- resolution derived from the raster when not supplied ---------------

    def test_resolution_read_from_raster(self):
        tile = self._tile("i.tif", 1.0, 10)  # no resolution passed
        self.assertEqual(Resolution.R_1M, lidar_selector._resolution_of(tile))


if __name__ == '__main__':
    unittest.main()
