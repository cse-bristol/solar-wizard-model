# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
"""
File-based LiDAR selection

Takes elevation tiles on disk (any mix of 50cm/1m/2m) and produces the ordered
list of tiles that `generate_rasters` builds its elevation VRT from, plus the
resolution to work at.

Details:
- coverage is the valid- (non-nodata) pixel area within the job bounds, per
  resolution, over the bounds area,
- the target resolution is 1m unless 1m-or-better coverage is sparse and 2m is
  much better,
- higher-res tiles are resampled onto the target grid and lower-res tiles are
  never upsampled into a finer target (RANSAC reads upsampled coarse tiles as
  flat steps),
- on overlap the newest tile wins, and for a given year the highest-res tile
  wins.
"""
import logging
from collections import defaultdict
from os.path import join, basename
from typing import Dict, List, Optional, Tuple

import numpy as np
from osgeo import gdal

from solar_pv import gdal_helpers
from solar_pv.lidar.lidar import LidarTile, Resolution, LIDAR_NODATA

gdal.UseExceptions()

Bounds = Tuple[float, float, float, float]
"""(xmin, ymin, xmax, ymax) in EPSG:27700"""

_USE_1M_COVERAGE_THRESHOLD = 0.25
_USE_2M_COVERAGE_MARGIN = 0.5


def select_lidar(tiles: List[LidarTile],
                 cov_bounds: Bounds,
                 extent_bounds: Bounds,
                 output_dir: str) -> Tuple[List[str], Resolution]:
    """
    Resolve the elevation tiles feeding `generate_rasters`.

    cov_bounds: the (unbuffered) job bounds - the resolution choice is 
        based on LiDAR coverage within these.
    extent_bounds: the job bounds buffered by the horizon search radius -
        only tiles intersecting this contribute terrain (the ring around the
        edge buildings is needed for horizon tracing).
    returns: (ordered resampled tile paths, target resolution). The paths are
        ordered so a last-wins VRT reproduces the newest/highest-res merge.
    """
    by_res = _by_resolution(tiles)
    target = _target_resolution(by_res, cov_bounds)

    # Never upsample a coarser tile into a finer target (RANSAC treats an
    # upsampled 2m tile as a flat step), so only merge resolutions >= target.
    used = [t for res, res_tiles in by_res.items() if res.value <= target.value
            for t in res_tiles
            if _intersect(_tile_extent(t.filename), extent_bounds) is not None]

    used.sort(key=lambda t: (_year_key(t), -_resolution_of(t).value))

    paths = []
    for i, tile in enumerate(used):
        if _on_target_grid(tile.filename, target.value):
            # Already at the target resolution, on the grid and normalised: use
            # it as-is rather than reading and rewriting the whole tile.
            paths.append(tile.filename)
        else:
            out = join(output_dir, f"resampled_{i:04d}_{basename(tile.filename)}")
            _resample(tile.filename, out, target.value)
            paths.append(out)
    return paths, target


def count_usable_tiles(tiles: List[LidarTile], cov_bounds: Bounds) -> int:
    """
    Number of tiles at a usable resolution (>= the target) intersecting the job
    bounds. Zero means there is no LiDAR to run on - the replacement for the old
    `raster_tile_coverage_count`, which likewise counted only tiles at the
    target resolution or finer.
    """
    by_res = _by_resolution(tiles)
    target = _target_resolution(by_res, cov_bounds)
    return sum(1 for res, res_tiles in by_res.items() if res.value <= target.value
               for t in res_tiles
               if _intersect(_tile_extent(t.filename), cov_bounds) is not None)


def _by_resolution(tiles: List[LidarTile]) -> Dict[Resolution, List[LidarTile]]:
    by_res = defaultdict(list)
    for tile in tiles:
        by_res[_resolution_of(tile)].append(tile)
    return by_res


def _target_resolution(by_res: Dict[Resolution, List[LidarTile]],
                       cov_bounds: Bounds) -> Resolution:
    cov_50cm = _coverage(by_res.get(Resolution.R_50CM, []), Resolution.R_50CM, cov_bounds)
    cov_1m = _coverage(by_res.get(Resolution.R_1M, []), Resolution.R_1M, cov_bounds)
    cov_2m = _coverage(by_res.get(Resolution.R_2M, []), Resolution.R_2M, cov_bounds)
    logging.info(f"LiDAR coverage:  50cm: {cov_50cm}, 1m: {cov_1m}, 2m: {cov_2m}")

    # 50cm is never worked at directly (too slow) but is merged into 1m, so it
    # counts towards 1m coverage:
    cov_1m = max(cov_50cm, cov_1m)
    if cov_1m < _USE_1M_COVERAGE_THRESHOLD and cov_2m > cov_1m + _USE_2M_COVERAGE_MARGIN:
        target = Resolution.R_2M
    else:
        target = Resolution.R_1M
    logging.info(f"Using resolution {target}")
    return target


def _coverage(tiles: List[LidarTile], res: Resolution, bounds: Bounds) -> float:
    """
    Fraction of `bounds` covered by valid (non-nodata) pixels of `tiles`. Ports
    `SUM(st_count(st_clip(rast, bounds))) * pixel_area / area(bounds)`.
    """
    area = (bounds[2] - bounds[0]) * (bounds[3] - bounds[1])
    if area <= 0:
        return 0.0
    pixel_area = res.value ** 2
    valid_pixels = sum(_valid_pixels_in_bounds(t.filename, bounds) for t in tiles)
    return (valid_pixels * pixel_area) / area


def _valid_pixels_in_bounds(path: str, bounds: Bounds) -> int:
    """Count non-nodata pixels of the tile at `path` that fall within `bounds`."""
    isect = _intersect(_tile_extent(path), bounds)
    if isect is None:
        return 0
    xmin, ymin, xmax, ymax = isect
    # projWin (a VRT window, so only the clipped pixels are read) crops without
    # resampling, mirroring ST_Clip's pixel selection:
    clipped = gdal.Translate('', gdal.Open(path), format='VRT',
                             projWin=[xmin, ymax, xmax, ymin])
    band = clipped.GetRasterBand(1)
    nodata = band.GetNoDataValue()
    arr = band.ReadAsArray()
    if arr is None:
        return 0
    valid = np.isfinite(arr)
    if nodata is not None:
        valid &= arr != nodata
    return int(np.count_nonzero(valid))


def _on_target_grid(path: str, res: float) -> bool:
    """
    Whether the tile can feed the VRT unchanged: it is at the target resolution,
    unrotated, its origin sits on the target grid (so it aligns with the
    target-aligned resampled tiles), and its nodata is already normalised. If
    not, it must be resampled - which also remaps a foreign nodata to
    LIDAR_NODATA, so a tile with a different nodata is never used as-is.
    """
    ds = gdal.Open(path)
    ulx, xres, xskew, uly, yskew, yres = ds.GetGeoTransform()
    if xskew or yskew:
        return False
    if round(abs(xres), 10) != res or round(abs(yres), 10) != res:
        return False
    if not _on_grid(ulx, res) or not _on_grid(uly, res):
        return False
    nodata = ds.GetRasterBand(1).GetNoDataValue()
    return nodata is None or nodata == LIDAR_NODATA


def _on_grid(coord: float, res: float, tol: float = 1e-6) -> bool:
    steps = coord / res
    return abs(steps - round(steps)) < tol


def _resample(src: str, dst: str, res: float) -> None:
    """
    Resample `src` onto the target-resolution grid (target-aligned so every
    resampled tile shares one grid), nearest-neighbour as ST_Resample did.
    Tiles already at the target resolution are snapped to the same grid.
    """
    gdal.Warp(dst, src,
              xRes=res, yRes=res,
              targetAlignedPixels=True,
              resampleAlg="near",
              dstNodata=LIDAR_NODATA,
              creationOptions=["TILED=YES", "COMPRESS=PACKBITS", "BIGTIFF=YES"])


def _resolution_of(tile: LidarTile) -> Resolution:
    if tile.resolution is not None:
        return tile.resolution
    return _bucket_resolution(gdal_helpers.get_res(tile.filename))


def _bucket_resolution(res: float) -> Resolution:
    """Snap an arbitrary raster resolution to the nearest resolution we model."""
    return min(Resolution, key=lambda r: abs(r.value - res))


def _tile_extent(path: str) -> Bounds:
    ds = gdal.Open(path)
    ulx, xres, _, uly, _, yres = ds.GetGeoTransform()
    lrx = ulx + ds.RasterXSize * xres
    lry = uly + ds.RasterYSize * yres
    return min(ulx, lrx), min(uly, lry), max(ulx, lrx), max(uly, lry)


def _intersect(a: Bounds, b: Bounds) -> Optional[Bounds]:
    xmin = max(a[0], b[0])
    ymin = max(a[1], b[1])
    xmax = min(a[2], b[2])
    ymax = min(a[3], b[3])
    if xmin >= xmax or ymin >= ymax:
        return None
    return xmin, ymin, xmax, ymax


def _year_key(tile: LidarTile) -> float:
    """Undated tiles sort oldest, so a dated tile wins the overlap over them."""
    return tile.year if tile.year is not None else float("-inf")
