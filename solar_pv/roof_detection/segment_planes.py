# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
"""
Over-segment a building's pixels into planar regions by region growing on local plane
fits, as candidate planes for `assign_pixels` - an alternative to finding them with
DETSAC and RANSAC.

Each pixel's normal comes from the best-fitting (lowest RMS) of the 9 3x3 windows that
contain it, rather than the window centred on it: at a ridge, hip, eave or step the
centred window straddles two planes, but some window lies wholly on the pixel's side.

Regions grow from the best-fitting unused seed, over 4-neighbours which lie close to the
region's plane (refitted as it grows) and, if their own normal is reliable, whose normal
is close to the region's. The distance test separates parallel planes at different
heights, which normals alone can't, and stops a pitched region at a ridge; the normal
test stops it at shallow creases. Testing against the region's plane, rather than the
neighbour's normal, stops regions creeping over curved surfaces.

Roof planes are often only 3 or 4 pixels wide at 1m, so that every window straddles a
ridge or the edge of the building: those pixels have no reliable normal, and are
seeds of last resort.
"""
from collections import deque
from typing import List, Optional, Tuple

import numpy as np
from scipy import ndimage

from solar_pv.constants import FLAT_ROOF_DEGREES_THRESHOLD
from solar_pv.roof_detection.assign_pixels import grid_index, NONE

_OFFSETS = [(dr, dc) for dr in (-1, 0, 1) for dc in (-1, 0, 1)]
_FLAT_COS = np.cos(np.radians(FLAT_ROOF_DEGREES_THRESHOLD))
_MIN_WINDOW_PIXELS = 6


def segment_planes(xyz: np.ndarray,
                   resolution_metres: float,
                   max_normal_angle_degrees: float = 12,
                   max_distance: float = 0.25,
                   max_flat_distance: float = 0.1,
                   max_normal_rms: float = 0.1,
                   min_pixels: int = 8,
                   local: Optional[Tuple[np.ndarray, np.ndarray]] = None) -> np.ndarray:
    """
    A region label per pixel of `xyz` (0, 1, ...), or NONE for pixels in no region of at
    least `min_pixels`. Distances are in metres, from the region's plane; flat regions use
    `max_flat_distance`. A pixel's normal is reliable if its local fit's RMS is at most
    `max_normal_rms`.

    `local` is each pixel's (normal, RMS) from `local_fits`, if already known - it must be
    for segmenting a subset of a building's pixels, whose local fits should come from all
    of them.
    """
    xy = xyz[:, :2]
    z = xyz[:, 2] - xyz[:, 2].min()
    grid, rows, cols = grid_index(xy, resolution_metres)
    normals, rms = local if local is not None else _local_normals(grid, rows, cols, z, resolution_metres)

    labels = np.full(len(z), NONE)
    if len(z) == 0:
        return labels

    neighbours = _neighbour_lists(grid, rows, cols)
    cos_max = np.cos(np.radians(max_normal_angle_degrees))
    x_l = (xy[:, 0] - xy[:, 0].min()).tolist()
    y_l = (xy[:, 1] - xy[:, 1].min()).tolist()
    z_l = z.tolist()
    nx, ny, nz = (normals[:, i].tolist() for i in range(3))
    reliable = (rms <= max_normal_rms).tolist()

    region = 0
    for seed in np.argsort(rms, kind='stable'):
        if labels[seed] != NONE or not np.isfinite(rms[seed]):
            continue
        members = _grow(int(seed), labels, region, neighbours, x_l, y_l, z_l, nx, ny, nz, reliable,
                        cos_max, max_distance, max_flat_distance)
        if len(members) >= min_pixels:
            region += 1
        else:
            labels[members] = NONE
    return labels


def local_fits(xyz: np.ndarray, resolution_metres: float) -> Tuple[np.ndarray, np.ndarray]:
    """Per pixel, the unit upward normal and RMS residual of the best-fitting 3x3 window
    containing it (see `_local_normals`)."""
    grid, rows, cols = grid_index(xyz[:, :2], resolution_metres)
    return _local_normals(grid, rows, cols, xyz[:, 2] - xyz[:, 2].min(), resolution_metres)


def _grow(seed: int, labels: np.ndarray, region: int, neighbours: List[List[int]],
          x: list, y: list, z: list, nx: list, ny: list, nz: list, reliable: list,
          cos_max: float, max_distance: float, max_flat_distance: float) -> List[int]:
    """Grow a region from `seed`, labelling its pixels `region`, and return them."""
    x0, y0 = x[seed], y[seed]
    # the region's plane z = a x + b y + c in coordinates relative to the seed,
    # starting from the seed's local normal:
    a, b = -nx[seed] / nz[seed], -ny[seed] / nz[seed]
    c = z[seed]
    unit = (nx[seed], ny[seed], nz[seed])
    dist = max_flat_distance if unit[2] >= _FLAT_COS else max_distance
    sums = [0.0] * 9
    members = []
    refit_at = 4

    def add(i):
        dx, dy, dz = x[i] - x0, y[i] - y0, z[i]
        s = sums
        s[0] += 1; s[1] += dx; s[2] += dy; s[3] += dz
        s[4] += dx * dx; s[5] += dx * dy; s[6] += dy * dy; s[7] += dx * dz; s[8] += dy * dz
        members.append(i)
        labels[i] = region

    add(seed)
    queue = deque([seed])
    while queue:
        p = queue.popleft()
        for q in neighbours[p]:
            if labels[q] != NONE:
                continue
            if reliable[q] and nx[q] * unit[0] + ny[q] * unit[1] + nz[q] * unit[2] < cos_max:
                continue
            if abs(z[q] - (a * (x[q] - x0) + b * (y[q] - y0) + c)) > dist:
                continue
            add(q)
            queue.append(q)
            if len(members) >= refit_at:
                fit = _solve(sums)
                if fit is not None:
                    a, b, c = fit
                    norm = (a * a + b * b + 1) ** 0.5
                    unit = (-a / norm, -b / norm, 1 / norm)
                    dist = max_flat_distance if unit[2] >= _FLAT_COS else max_distance
                refit_at = int(refit_at * 1.5) + 1
    return members


def _solve(sums: list):
    n, sx, sy, sz, sxx, sxy, syy, sxz, syz = sums
    mx, my, mz = sx / n, sy / n, sz / n
    cxx, cxy, cyy = sxx - sx * mx, sxy - sx * my, syy - sy * my
    cxz, cyz = sxz - sx * mz, syz - sy * mz
    det = cxx * cyy - cxy * cxy
    if not det > 1e-9 * (cxx + cyy) ** 2:
        return None
    a = (cxz * cyy - cyz * cxy) / det
    b = (cyz * cxx - cxz * cxy) / det
    return a, b, mz - a * mx - b * my


def _neighbour_lists(grid: np.ndarray, rows: np.ndarray, cols: np.ndarray) -> List[List[int]]:
    padded = np.pad(grid, 1, constant_values=-1)
    r, c = rows + 1, cols + 1
    nbrs = np.stack([padded[r - 1, c], padded[r + 1, c], padded[r, c - 1], padded[r, c + 1]], axis=1)
    return [[q for q in row if q >= 0] for row in nbrs.tolist()]


def _local_normals(grid: np.ndarray, rows: np.ndarray, cols: np.ndarray, z: np.ndarray,
                   res: float) -> Tuple[np.ndarray, np.ndarray]:
    """
    Per pixel, the unit upward normal and RMS residual of the best-fitting plane over
    the 3x3 windows containing it. Windows need `_MIN_WINDOW_PIXELS`, as 4 or 5 pixels
    (a corner of the building) fit a plane closely whatever it is. RMS is inf where no
    window qualifies.
    """
    valid = (grid >= 0).astype(float)
    zimg = np.zeros(grid.shape)
    zimg[rows, cols] = z

    d = np.array([-res, 0.0, res])
    # kernels as offsets (row, col) from the window centre; rows are +y, cols +x
    kx = np.tile(d, (3, 1))
    ky = kx.T

    def corr(img, k):
        return ndimage.correlate(img, k, mode='constant', cval=0.0)

    ones = np.ones((3, 3))
    vz, vzz = valid * zimg, valid * zimg * zimg
    n = corr(valid, ones)
    sx, sy = corr(valid, kx), corr(valid, ky)
    sxx, syy, sxy = corr(valid, kx * kx), corr(valid, ky * ky), corr(valid, kx * ky)
    sz, sxz, syz, szz = corr(vz, ones), corr(vz, kx), corr(vz, ky), corr(vzz, ones)

    A = np.stack([np.stack([sxx, sxy, sx], -1),
                  np.stack([sxy, syy, sy], -1),
                  np.stack([sx, sy, n], -1)], -2)
    rhs = np.stack([sxz, syz, sz], -1)
    det = np.linalg.det(A)
    ok = (n >= _MIN_WINDOW_PIXELS) & (np.abs(det) > 1e-6 * res ** 4)
    coef = np.zeros(rhs.shape)
    coef[ok] = np.linalg.solve(A[ok], rhs[ok][..., None])[..., 0]
    sse = szz - (coef * rhs).sum(-1)
    win_rms = np.full(grid.shape, np.inf)
    win_rms[ok] = np.sqrt(np.maximum(sse[ok], 0) / n[ok])

    # for each pixel, the best of the windows centred on it and its 8 neighbours
    padded_rms = np.pad(win_rms, 1, constant_values=np.inf)
    padded_coef = np.pad(coef, ((1, 1), (1, 1), (0, 0)))
    best_rms = np.full(len(z), np.inf)
    best_ab = np.zeros((len(z), 2))
    for dr, dc in _OFFSETS:
        r, c = rows + 1 + dr, cols + 1 + dc
        cand = padded_rms[r, c]
        better = cand < best_rms
        best_rms[better] = cand[better]
        best_ab[better] = padded_coef[r[better], c[better], :2]

    normals = np.column_stack([-best_ab[:, 0], -best_ab[:, 1], np.ones(len(z))])
    normals /= np.linalg.norm(normals, axis=1, keepdims=True)
    return normals, best_rms
