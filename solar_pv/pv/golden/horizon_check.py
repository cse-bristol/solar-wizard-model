# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
"""
Compare the native-Python horizon port against the frozen GRASS/PVMAPS horizon goldens
(Phase 1; docs/pv-grass-removal-plan.md).

The committed test elevation and the golden are on grids that differ by a sub-pixel shift
(GRASS resamples the DEM onto its mask-zoomed region on import — visible only at the finest
test tile). In production all rasters share one grid, so before comparing we warp the
elevation onto the golden's exact grid; the port then reproduces GRASS to a fraction of a
degree, with a handful of near-obstruction diagonal-tie pixels (inherent to nearest-neighbour
ray sampling) as the only outliers.
"""
from dataclasses import dataclass
from os.path import join

import numpy as np
from osgeo import gdal

from solar_pv.pv import horizon, horizon_geo
from solar_pv.pv.golden import compare
from solar_pv.pv.golden.fixtures import Area, HORIZON_STEP_DEGREES, HORIZON_SEARCH_DISTANCE

gdal.UseExceptions()


@dataclass
class HorizonStats:
    area: str
    n_dirs: int
    n_pixels: int
    mean_deg: float      # mean abs error over all pixels/directions
    p99_deg: float
    max_deg: float
    pct_over_5deg: float

    def __str__(self):
        return (f"{self.area}: dirs={self.n_dirs} n={self.n_pixels} "
                f"mean={self.mean_deg:.4f} p99={self.p99_deg:.4f} "
                f"max={self.max_deg:.4f} >5deg={self.pct_over_5deg:.3f}% (all deg)")


def _warp_to_golden_grid(src_path: str, ggt, gshape, resample: str, nodata):
    """Warp a raster onto the golden's exact grid (removes the fixture's sub-pixel
    grid-provenance shift; a no-op when grids already coincide)."""
    rows, cols = gshape
    bounds = (ggt[0], ggt[3] + ggt[5] * rows, ggt[0] + ggt[1] * cols, ggt[3])
    mem = "/vsimem/horizon_warp.tif"
    gdal.Warp(mem, src_path, xRes=abs(ggt[1]), yRes=abs(ggt[5]), resampleAlg=resample,
              outputBounds=bounds, srcNodata=nodata, dstNodata=nodata)
    arr, gt, shape = compare.read_geotiff(mem)
    gdal.Unlink(mem)
    return arr, gt, shape


def check_area(area: Area) -> HorizonStats:
    directions = horizon.grass_directions(HORIZON_STEP_DEGREES)
    _, ggt, gshape = compare.read_geotiff(join(area.golden_dir, "horizon_00.tif"))

    elev, egt, eshape = _warp_to_golden_grid(
        area.input_path(area.elevation), ggt, gshape, "bilinear", -9999)
    # The mask restricts the port to the same building-footprint pixels r.horizonmask
    # evaluated (where the golden is non-null); nearest-neighbour keeps it categorical:
    mask, _, _ = _warp_to_golden_grid(
        area.input_path(area.mask), ggt, gshape, "near", 0)

    vectors = horizon_geo.grass_marching_vectors(egt, eshape, directions)
    horizons = horizon.compute_horizons(
        elev, abs(egt[1]), abs(egt[5]), vectors, HORIZON_SEARCH_DISTANCE,
        mask=np.nan_to_num(mask) != 0)

    errs = []
    for d_idx in range(len(directions)):
        golden = compare._read_band(join(area.golden_dir, f"horizon_{d_idx:02d}.tif"))
        port = horizons[d_idx]
        valid = np.isfinite(port) & np.isfinite(golden)
        errs.append(np.degrees(np.abs(port[valid] - golden[valid])))
    all_err = np.concatenate(errs)
    return HorizonStats(
        area=area.name,
        n_dirs=len(directions),
        n_pixels=int(all_err.size),
        mean_deg=float(all_err.mean()),
        p99_deg=float(np.percentile(all_err, 99)),
        max_deg=float(all_err.max()),
        pct_over_5deg=float(100.0 * np.mean(all_err > 5.0)))
