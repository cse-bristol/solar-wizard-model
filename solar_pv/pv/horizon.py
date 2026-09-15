# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
"""
Native-Python port of GRASS/PVMAPS r.horizonmask (Phase 1 of the GRASS removal;
docs/pv-grass-removal-plan.md).

For each requested cell and each azimuth direction (CCW from East, matching r.horizon),
computes the angular height of the terrain horizon: the max over cells along the ray of
    atan( (z_cell - z_origin - curvature_drop) / distance )
where curvature_drop = 0.5 * distance^2 / EARTH_RADIUS. The result is clamped to [0, pi/2]
(a negative running-max means nothing rose above the origin -> 0; the PV step needs
horizons in that range). Like r.horizonmask, a mask restricts which cells are evaluated
(only building-footprint pixels need a horizon), while the full elevation is used as terrain.

Faithful to r.horizonmask/main.c:
- marches in ~1-cell steps (stepxy = 0.5*(ew_res+ns_res), r.horizon's `distance` coeff = 1.0),
  registering the nearest cell (round) at each step -> cell offset
  (round(k*stepxy*cos/ew_res), round(k*stepxy*sin/ns_res));
- samples elevation nearest-neighbour at that cell;
- distance is between cell centres: hypot(di*ew_res, dj*ns_res);
- reproduces the running-max horizon exactly (the C code's z100 low-res grid and per-cell
  maxlength are search optimisations that don't change the result within max_distance).
"""
import math
from typing import Sequence

import numpy as np

EARTH_RADIUS = 6371000.0
PI_HALF = math.pi / 2.0
# Elevations at/below this are treated as nodata. Catches GRASS UNDEFZ (-9999) and sits
# far below any real UK terrain (lowest land ~ -3 m); GeoTIFF nodata is separately NaN'd on
# read, so this only guards arrays passed in directly.
NODATA_BELOW = -9990.0


def grass_directions(step_degrees: float,
                     start_degrees: float = 0.0,
                     end_degrees: float = 360.0) -> list:
    """Azimuths in radians, CCW from East: range(start, end, step) in degrees."""
    n = int(round((end_degrees - start_degrees) / step_degrees))
    return [math.radians(start_degrees + i * step_degrees) for i in range(n)]


def nominal_vectors(directions_rad: Sequence[float]):
    """Marching unit vectors (cos, sin) straight from the azimuths, ignoring grid
    convergence. Fine on/near the projection's central meridian."""
    return [(math.cos(a), math.sin(a)) for a in directions_rad]


def compute_horizons(elevation: np.ndarray,
                     ew_res: float,
                     ns_res: float,
                     direction_vectors: Sequence,
                     max_distance: float,
                     earth_radius: float = EARTH_RADIUS,
                     mask: np.ndarray = None) -> np.ndarray:
    """
    :param elevation: 2D DEM, north-up (row 0 = north), as read from a GeoTIFF. nodata cells
        must be <= NODATA_BELOW (or NaN).
    :param ew_res: east-west cell size (metres, positive).
    :param ns_res: north-south cell size (metres, positive; pass abs of a negative GT[5]).
    :param direction_vectors: one (cos, sin) unit vector per direction, in the CCW-from-East
        grid convention (East=(1,0), North=(0,1)). Use nominal_vectors() near the central
        meridian, or grass_marching_vectors() (in horizon_geo) to match r.horizon's
        convergence-corrected directions exactly.
    :param max_distance: horizon search radius in metres.
    :param mask: optional 2D array; horizons are computed only where it is truthy (typically
        building-footprint pixels), the rest of the output being NaN. The full elevation is
        still used as terrain. When None, every valid-elevation cell is evaluated.
    :return: array (n_directions, rows, cols) of horizon angles in radians, clamped to
        [0, pi/2]; unevaluated / nodata-origin cells are NaN.
    """
    z = np.asarray(elevation, dtype=np.float64)
    valid = np.isfinite(z) & (z > NODATA_BELOW)
    # Terrain cells that can never raise a horizon (nodata) must not contribute:
    z_terrain = np.where(valid, z, -np.inf)

    rows, cols = z.shape
    stepxy = 0.5 * (ew_res + ns_res)
    k_max = int(math.ceil(max_distance / stepxy)) + 1

    evaluate = valid if mask is None else valid & (np.asarray(mask) != 0)
    # Origin cells to evaluate, as flat row/col index arrays; work per-origin (not per grid
    # cell) so a sparse building mask is cheap:
    orow, ocol = np.nonzero(evaluate)
    z_orig = z[orow, ocol]

    out = np.full((len(direction_vectors), rows, cols), np.nan, dtype=np.float64)

    for d_idx, (cos_a, sin_a) in enumerate(direction_vectors):
        best_tan = np.full(orow.shape, -np.inf)
        seen = set()
        for k in range(1, k_max + 1):
            # r.horizon registers cell (int)(pos/res + 0.5); for interior cells this is
            # floor(offset + 0.5) (round half up), independent of the origin index:
            di = math.floor(k * stepxy * cos_a / ew_res + 0.5)   # cells east (col +)
            dj = math.floor(k * stepxy * sin_a / ns_res + 0.5)   # cells north (row -)
            if di == 0 and dj == 0:
                continue
            if (di, dj) in seen:
                continue
            seen.add((di, dj))

            # |di|, |dj| and hence length are non-decreasing in k, so once the ray leaves
            # the grid or exceeds the search radius no later step can contribute -> break:
            length = math.hypot(di * ew_res, dj * ns_res)
            if length > max_distance:
                break
            # north-up array: north is row-negative, east is col-positive:
            drow, dcol = -dj, di
            if abs(drow) >= rows or abs(dcol) >= cols:
                break

            tr = orow + drow
            tc = ocol + dcol
            in_bounds = (tr >= 0) & (tr < rows) & (tc >= 0) & (tc < cols)
            z_cell = np.full(orow.shape, -np.inf)
            z_cell[in_bounds] = z_terrain[tr[in_bounds], tc[in_bounds]]
            curvature = 0.5 * length * length / earth_radius
            tan_k = (z_cell - z_orig - curvature) / length
            np.maximum(best_tan, tan_k, out=best_tan)

        # atan of the running-max slope, clamped to [0, pi/2]: best_tan == -inf (nothing
        # rose above the origin) -> atan -> -pi/2 -> clamps to 0.
        out[d_idx][orow, ocol] = np.clip(np.arctan(best_tan), 0.0, PI_HALF)

    return out
