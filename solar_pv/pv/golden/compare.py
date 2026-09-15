# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
"""
Reusable comparison helpers for validating the native-Python PV port against the frozen
GRASS/PVMAPS golden rasters (the primary, tight oracle) and against the cached PVGIS API
outputs (a loose, secondary "shape" check only — PVMAPS is frozen and the API has diverged
from it, so API agreement is never the pass/fail gate).

See docs/pv-grass-removal-plan.md.
"""
from dataclasses import dataclass
from typing import Optional

import numpy as np
from osgeo import gdal

gdal.UseExceptions()


@dataclass
class RasterDiff:
    """Summary of the per-pixel difference between two co-registered rasters, over the
    pixels valid (non-nodata, finite) in both."""
    n: int
    max_abs_pc: float   # max |100*(port-golden)/golden|, over golden != 0
    mean_abs_pc: float
    p95_abs_pc: float
    max_abs: float      # max |port-golden| in raw units (for horizon: radians)
    mean_abs: float

    def within(self, max_abs_pc: float) -> bool:
        return self.n > 0 and self.max_abs_pc <= max_abs_pc

    def __str__(self) -> str:
        return (f"n={self.n} max_abs_pc={self.max_abs_pc:.3f}% "
                f"mean_abs_pc={self.mean_abs_pc:.3f}% p95_abs_pc={self.p95_abs_pc:.3f}% "
                f"max_abs={self.max_abs:.4g} mean_abs={self.mean_abs:.4g}")


def read_geotiff(path: str):
    """Return (array float64 with nodata->nan, geotransform, (rows, cols))."""
    ds = gdal.Open(path)
    if ds is None:
        raise FileNotFoundError(path)
    band = ds.GetRasterBand(1)
    arr = band.ReadAsArray().astype(np.float64)
    nodata = band.GetNoDataValue()
    if nodata is not None and not np.isnan(nodata):
        arr = np.where(arr == nodata, np.nan, arr)
    return arr, ds.GetGeoTransform(), arr.shape


def crop_to_window(arr: np.ndarray, src_gt, dst_gt, dst_shape) -> np.ndarray:
    """Crop a north-up array on grid `src_gt` to the pixel window described by `dst_gt`/
    `dst_shape` (same resolution/CRS, pixel-aligned). Out-of-source pixels come back NaN."""
    col_off = int(round((dst_gt[0] - src_gt[0]) / src_gt[1]))
    row_off = int(round((dst_gt[3] - src_gt[3]) / src_gt[5]))
    dst_rows, dst_cols = dst_shape
    out = np.full(dst_shape, np.nan, dtype=np.float64)
    sr0 = max(0, row_off)
    sc0 = max(0, col_off)
    dr0 = max(0, -row_off)
    dc0 = max(0, -col_off)
    rh = min(arr.shape[0] - sr0, dst_rows - dr0)
    cw = min(arr.shape[1] - sc0, dst_cols - dc0)
    if rh > 0 and cw > 0:
        out[dr0:dr0 + rh, dc0:dc0 + cw] = arr[sr0:sr0 + rh, sc0:sc0 + cw]
    return out


def _read_band(path: str) -> np.ndarray:
    ds = gdal.Open(path)
    if ds is None:
        raise FileNotFoundError(path)
    band = ds.GetRasterBand(1)
    arr = band.ReadAsArray().astype(np.float64)
    nodata = band.GetNoDataValue()
    if nodata is not None:
        arr = np.where(arr == nodata, np.nan, arr)
    return arr


def raster_diff(port_path: str, golden_path: str,
                pc_floor: float = 0.0) -> RasterDiff:
    """
    Compare two co-registered single-band rasters over the pixels valid in both.

    :param pc_floor: golden values with |value| <= this are excluded from the percentage
        stats (but not the absolute stats), to avoid dividing tiny denominators. For yearly
        kWh leave at 0; for near-zero-heavy rasters (e.g. winter months, horizons) set a
        small floor.
    """
    port = _read_band(port_path)
    golden = _read_band(golden_path)
    return array_diff(port, golden, pc_floor)


def array_diff(port: np.ndarray, golden: np.ndarray, pc_floor: float = 0.0) -> RasterDiff:
    """As raster_diff, for in-memory arrays (nodata already NaN)."""
    if port.shape != golden.shape:
        raise ValueError(f"shape mismatch: port {port.shape} vs golden {golden.shape}")

    valid = np.isfinite(port) & np.isfinite(golden)
    if not valid.any():
        return RasterDiff(0, np.nan, np.nan, np.nan, np.nan, np.nan)

    p = port[valid]
    g = golden[valid]
    abs_err = np.abs(p - g)

    pc_mask = np.abs(g) > pc_floor
    if pc_mask.any():
        abs_pc = np.abs(100.0 * (p[pc_mask] - g[pc_mask]) / g[pc_mask])
        max_pc = float(np.max(abs_pc))
        mean_pc = float(np.mean(abs_pc))
        p95_pc = float(np.percentile(abs_pc, 95))
    else:
        max_pc = mean_pc = p95_pc = np.nan

    return RasterDiff(
        n=int(valid.sum()),
        max_abs_pc=max_pc, mean_abs_pc=mean_pc, p95_abs_pc=p95_pc,
        max_abs=float(np.max(abs_err)), mean_abs=float(np.mean(abs_err)))


@dataclass
class PointDiff:
    """Summary of port-vs-oracle agreement at a set of sample points."""
    n: int
    max_abs_pc_year: float
    mean_abs_pc_year: float

    def within(self, max_abs_pc: float) -> bool:
        return self.n > 0 and self.max_abs_pc_year <= max_abs_pc

    def __str__(self) -> str:
        return (f"n={self.n} max_abs_pc_year={self.max_abs_pc_year:.3f}% "
                f"mean_abs_pc_year={self.mean_abs_pc_year:.3f}%")


def _pc_diff(a: float, b: float) -> float:
    if b == 0.0 and a == 0.0:
        return 0.0
    if b == 0.0:
        return np.nan
    return 100.0 * (a - b) / b


def point_year_diff(port_year: np.ndarray, oracle_year: np.ndarray) -> PointDiff:
    """Compare yearly totals at matched sample points (same order)."""
    port_year = np.asarray(port_year, dtype=np.float64)
    oracle_year = np.asarray(oracle_year, dtype=np.float64)
    if port_year.shape != oracle_year.shape:
        raise ValueError(f"length mismatch: {port_year.shape} vs {oracle_year.shape}")
    diffs = np.array([abs(_pc_diff(p, o)) for p, o in zip(port_year, oracle_year)])
    finite = diffs[np.isfinite(diffs)]
    if finite.size == 0:
        return PointDiff(0, np.nan, np.nan)
    return PointDiff(n=int(finite.size),
                     max_abs_pc_year=float(np.max(finite)),
                     mean_abs_pc_year=float(np.mean(finite)))
