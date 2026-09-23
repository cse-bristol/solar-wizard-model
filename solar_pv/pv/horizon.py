# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
"""
Native-Python port of GRASS r.horizon.

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
- reproduces the running-max horizon exactly. Like the C code's z100 low-res grid, the search
  stops early once no higher terrain within reach could raise the horizon; this never changes
  the result.
"""
import math
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional, Sequence, Tuple

import numpy as np

from solar_pv.util import get_cpu_count

EARTH_RADIUS = 6371000.0
PI_HALF = math.pi / 2.0
# Elevations at/below this are treated as nodata. Catches GRASS UNDEFZ (-9999) and sits
# far below any real UK terrain (lowest land ~ -3 m); GeoTIFF nodata is separately NaN'd on
# read, so this only guards arrays passed in directly.
NODATA_BELOW = -9990.0
# Origins per work unit, and (origin, step) pairs traced per numpy call. The Python between
# numpy calls holds the GIL, so each call must do enough work for that serial part to stay
# small at high thread counts; batches are made of more steps as early termination shrinks the
# set of origins still being traced.
CHUNK_SIZE = 8192
BATCH_SIZE = 262144


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


class _Ray:
    """The distinct cell offsets one direction's march visits, nearest first."""

    def __init__(self, cos_a: float, sin_a: float, ew_res: float, ns_res: float,
                 max_distance: float, earth_radius: float, rows: int, cols: int):
        stepxy = 0.5 * (ew_res + ns_res)
        k_max = int(math.ceil(max_distance / stepxy)) + 1
        di_list, dj_list, lengths = [], [], []
        seen = set()
        for k in range(1, k_max + 1):
            # r.horizon registers cell (int)(pos/res + 0.5); for interior cells this is
            # floor(offset + 0.5) (round half up), independent of the origin index:
            di = math.floor(k * stepxy * cos_a / ew_res + 0.5)   # cells east (col +)
            dj = math.floor(k * stepxy * sin_a / ns_res + 0.5)   # cells north (row -)
            if (di == 0 and dj == 0) or (di, dj) in seen:
                continue
            seen.add((di, dj))
            # |di|, |dj| and hence length are non-decreasing in k, so once the ray leaves
            # the grid or exceeds the search radius no later step can contribute -> stop:
            length = math.hypot(di * ew_res, dj * ns_res)
            if length > max_distance or abs(dj) >= rows or abs(di) >= cols:
                break
            di_list.append(di)
            dj_list.append(dj)
            lengths.append(length)

        self.di = np.array(di_list, dtype=np.int64)
        self.dj = np.array(dj_list, dtype=np.int64)
        self.length = np.array(lengths, dtype=np.float64)
        self.curvature = 0.5 * self.length * self.length / earth_radius
        # north-up array: north is row-negative, east is col-positive:
        self.flat_offset = -self.dj * cols + self.di
        self.east = cos_a >= 0
        self.north = sin_a >= 0

    def __len__(self):
        return self.length.size

    def exit_steps(self, orow: np.ndarray, ocol: np.ndarray, rows: int, cols: int) -> np.ndarray:
        """Per origin, the index of the first step that falls off the grid (len(self) if none).
        Offsets grow monotonically in magnitude along the ray, so every later step is off too."""
        if self.east:
            col_exit = np.searchsorted(self.di, cols - 1 - ocol, side="right")
        else:
            col_exit = np.searchsorted(-self.di, ocol, side="right")
        if self.north:
            row_exit = np.searchsorted(self.dj, orow, side="right")
        else:
            row_exit = np.searchsorted(-self.dj, rows - 1 - orow, side="right")
        return np.minimum(col_exit, row_exit)


def _max_in_reach(z_terrain: np.ndarray, orow: np.ndarray, ocol: np.ndarray,
                  reach_rows: int, reach_cols: int) -> np.ndarray:
    """Per origin, an upper bound on the terrain height within reach_rows/reach_cols cells:
    the max over a coarse block grid, dilated by the reach."""
    rows, cols = z_terrain.shape
    block = max(1, max(reach_rows, reach_cols) // 16)
    br, bc = -(-rows // block), -(-cols // block)
    padded = np.full((br * block, bc * block), -np.inf)
    padded[:rows, :cols] = z_terrain
    blocks = padded.reshape(br, block, bc, block).max(axis=(1, 3))

    # a cell within `reach` of an origin is at most ceil(reach / block) blocks away:
    for axis, reach in ((0, -(-reach_rows // block)), (1, -(-reach_cols // block))):
        dilated = blocks.copy()
        n = blocks.shape[axis]
        for shift in range(1, min(reach, n - 1) + 1):
            lo = [slice(None)] * 2
            hi = [slice(None)] * 2
            lo[axis], hi[axis] = slice(0, n - shift), slice(shift, n)
            np.maximum(dilated[tuple(lo)], blocks[tuple(hi)], out=dilated[tuple(lo)])
            np.maximum(dilated[tuple(hi)], blocks[tuple(lo)], out=dilated[tuple(hi)])
        blocks = dilated
    return blocks[orow // block, ocol // block]


def _horizon_chunk(z_flat: np.ndarray, orow: np.ndarray, ocol: np.ndarray, z_orig: np.ndarray,
                   z_reach: np.ndarray, rays: List[_Ray], rows: int, cols: int,
                   out: np.ndarray) -> None:
    """Fill out (n, n_directions) with the horizon for one chunk of origins."""
    n = orow.size
    o_flat = orow * cols + ocol
    # the most any terrain in reach rises above each origin; every step's tan is bounded by
    # rise / length, so an origin is finished once its best tan reaches rise / next length.
    # An origin with nothing higher in reach is finished before starting (its horizon is 0).
    rise = z_reach - z_orig
    # reused across batches: allocating these afresh each time costs page faults, which also
    # serialise the threads.
    idx_buf = np.empty(BATCH_SIZE, dtype=np.int64)
    tan_buf = np.empty(BATCH_SIZE, dtype=np.float64)
    max_buf = np.empty(n, dtype=np.float64)
    best = np.empty(n, dtype=np.float64)

    for d_idx, ray in enumerate(rays):
        best.fill(-np.inf)
        exit_step = ray.exit_steps(orow, ocol, rows, cols)
        active = np.flatnonzero(rise > 0)
        best_a, o_a, zo_a, rise_a, exit_a = (
            best[active], o_flat[active], z_orig[active], rise[active], exit_step[active])
        n_steps = len(ray)
        s = 0
        while s < n_steps and active.size:
            # rounding is monotonic, so rise / length (rounded) bounds every later rounded tan:
            keep = (best_a < rise_a / ray.length[s]) & (exit_a > s)
            if not keep.all():
                best[active] = best_a
                active, best_a, o_a, zo_a, rise_a, exit_a = (
                    active[keep], best_a[keep], o_a[keep], zo_a[keep], rise_a[keep], exit_a[keep])
                if not active.size:
                    break
            end = min(s + max(1, BATCH_SIZE // active.size), n_steps)
            shape = (end - s, active.size)
            size = shape[0] * shape[1]
            idx = idx_buf[:size].reshape(shape)
            tan = tan_buf[:size].reshape(shape)
            # (steps, active) at once: few, large numpy calls keep the GIL mostly released.
            # Same operation order as (z_cell - z_orig - curvature) / length, so results match
            # the unoptimised trace bit-for-bit:
            np.add(ray.flat_offset[s:end, None], o_a, out=idx)
            np.take(z_flat, idx, out=tan, mode="clip")
            tan -= zo_a
            tan -= ray.curvature[s:end, None]
            tan /= ray.length[s:end, None]
            # only origins near the grid edge ever fall off; skip the mask when none do here:
            if exit_a.min() < end:
                np.copyto(tan, -np.inf, where=exit_a <= np.arange(s, end)[:, None])
            batch_max = np.max(tan, axis=0, out=max_buf[:active.size])
            np.maximum(best_a, batch_max, out=best_a)
            s = end
        best[active] = best_a
        # atan of the running-max slope, clamped to [0, pi/2]: best == -inf (nothing rose
        # above the origin) -> atan -> -pi/2 -> clamps to 0.
        out[:, d_idx] = np.clip(np.arctan(best), 0.0, PI_HALF)


def compute_horizons_flat(elevation: np.ndarray,
                          ew_res: float,
                          ns_res: float,
                          direction_vectors: Sequence,
                          max_distance: float,
                          earth_radius: float = EARTH_RADIUS,
                          mask: np.ndarray = None,
                          workers: Optional[int] = None
                          ) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Horizon profiles for only the evaluated cells, without ever allocating a full grid — the
    memory-lean form used on real job grids, where the mask covers a small fraction of cells.

    :param elevation: 2D DEM, north-up (row 0 = north). nodata cells must be <= NODATA_BELOW
        (or NaN).
    :param ew_res: east-west cell size (metres, positive).
    :param ns_res: north-south cell size (metres, positive; pass abs of a negative GT[5]).
    :param direction_vectors: one (cos, sin) unit vector per direction, in the CCW-from-East
        grid convention (East=(1,0), North=(0,1)). Use nominal_vectors() near the central
        meridian, or grass_marching_vectors() (in horizon_geo) to match r.horizon's
        convergence-corrected directions exactly.
    :param max_distance: horizon search radius in metres.
    :param mask: optional 2D array; horizons are computed only where it is truthy (typically
        building-footprint pixels). The full elevation is still used as terrain. When None,
        every valid-elevation cell is evaluated.
    :param workers: threads to trace with (default: all available CPUs).
    :return: (values, rows, cols): values is (M, n_directions) horizon angles in radians
        clamped to [0, pi/2]; rows/cols are the (M,) grid indices of those evaluated cells.
    """
    z = np.asarray(elevation, dtype=np.float64)
    valid = np.isfinite(z) & (z > NODATA_BELOW)
    # Terrain cells that can never raise a horizon (nodata) must not contribute:
    z_terrain = np.where(valid, z, -np.inf)
    z_flat = z_terrain.ravel()

    rows, cols = z.shape
    evaluate = valid if mask is None else valid & (np.asarray(mask) != 0)
    # Origin cells to evaluate, as flat row/col index arrays; work per-origin (not per grid
    # cell) so a sparse building mask is cheap. Row-major order keeps each chunk's origins
    # (and so its terrain reads) spatially local:
    orow, ocol = np.nonzero(evaluate)
    values = np.empty((orow.size, len(direction_vectors)), dtype=np.float64)
    if orow.size == 0:
        return values, orow, ocol

    z_orig = z[orow, ocol]
    z_reach = _max_in_reach(z_terrain, orow, ocol,
                            int(math.ceil(max_distance / ns_res)),
                            int(math.ceil(max_distance / ew_res)))
    rays = [_Ray(cos_a, sin_a, ew_res, ns_res, max_distance, earth_radius, rows, cols)
            for cos_a, sin_a in direction_vectors]

    def run(start: int) -> None:
        end = start + CHUNK_SIZE
        _horizon_chunk(z_flat, orow[start:end], ocol[start:end], z_orig[start:end],
                       z_reach[start:end], rays, rows, cols, values[start:end])

    starts = range(0, orow.size, CHUNK_SIZE)
    workers = min(workers or get_cpu_count(), len(starts))
    if workers == 1:
        for start in starts:
            run(start)
    else:
        with ThreadPoolExecutor(workers) as pool:
            list(pool.map(run, starts))
    return values, orow, ocol
