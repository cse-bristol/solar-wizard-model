# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
"""
Assign each LiDAR pixel of a building to one roof plane, or to none, by minimising

    sum over pixels of D(pixel, label)  +  smoothness * (number of 4-neighbour pairs with different labels)

where D is the pixel's elevation residual against the plane (in units of that plane's
residual scale, capped), and a constant `none_cost` for no plane. Multi-label Potts can't be solved
exactly; alpha-expansion (Boykov, Veksler & Zabih 2001) gets within a factor of 2 of the
optimum, and each expansion step is a single exact min-cut.
"""
from typing import Optional, Sequence, Tuple

import numpy as np
import scipy.sparse as sp
from scipy.ndimage import distance_transform_edt
from scipy.sparse.csgraph import breadth_first_order, maximum_flow

NONE = -1
RESIDUAL_CAP = 4.0
NONE_COST = 2.0
SMOOTHNESS_PER_M = 0.5
# maximum_flow needs int32 capacities: energy is quantized to this many steps per unit.
_ENERGY_SCALE = 1000


def assign_pixels(xyz: np.ndarray,
                  labels: np.ndarray,
                  n_planes: int,
                  resolution_metres: float,
                  residual_scales: Optional[Sequence[float]] = None,
                  residual_cap: float = RESIDUAL_CAP,
                  none_cost: float = NONE_COST,
                  smoothness_per_m: float = SMOOTHNESS_PER_M,
                  max_dist_m: float = 3,
                  max_cycles: int = 5,
                  return_energy: bool = False,
                  coefs: Optional[Sequence[Tuple[float, float, float]]] = None):
    """
    Reassign `labels` (per pixel of `xyz`: a plane index in [0, n_planes), or NONE).
    `residual_scales` is per plane, 0.25m if not given.

    Each plane is fitted by least squares to the pixels it starts with, and may only claim
    pixels within `max_dist_m` of them, so a coplanar patch elsewhere on the building
    can't be taken over. Planes that start with fewer than 3 pixels are not assigned any.

    A pixel's data cost stands for its area and a boundary's smoothness cost for its
    length, so per pixel edge the smoothness is `smoothness_per_m` scaled by 1/resolution:
    otherwise boundaries would cost relatively more at coarser resolutions, and small
    planes would lose out.

    If `coefs` is given, plane k is z = a x + b y + c for (a, b, c) = coefs[k], rather than
    a least-squares fit to its starting pixels.

    If `return_energy`, also returns the energy of the starting labels.
    """
    if residual_scales is None:
        residual_scales = [0.25] * n_planes
    smoothness = smoothness_per_m / resolution_metres
    max_dist_pixels = max_dist_m / resolution_metres
    xy = xyz[:, :2]
    z = xyz[:, 2]
    grid, rows, cols = grid_index(xy, resolution_metres)

    # internally, label 0 is none and plane k is k + 1
    current = labels + 1
    data = np.full((len(z), n_planes + 1), none_cost)
    allowed = np.zeros((len(z), n_planes + 1), dtype=bool)
    allowed[:, 0] = True
    for k in range(1, n_planes + 1):
        idxs = np.flatnonzero(current == k)
        if len(idxs) < 3:
            current[idxs] = 0
            continue
        if coefs is None:
            predicted = _fit_plane(xy[idxs], z[idxs])(xy)
        else:
            a, b, c = coefs[k - 1]
            predicted = xy[:, 0] * a + xy[:, 1] * b + c
        data[:, k] = np.minimum(np.abs(z - predicted) / residual_scales[k - 1], residual_cap)

        start_image = np.zeros(grid.shape, dtype=bool)
        start_image[rows[idxs], cols[idxs]] = True
        dist = distance_transform_edt(~start_image)
        allowed[:, k] = dist[rows, cols] <= max_dist_pixels

    lo, hi = _neighbour_pairs(grid)
    energy = _energy(current, data, lo, hi, smoothness)
    start_energy = energy
    for _ in range(max_cycles):
        improved = False
        for alpha in range(n_planes + 1):
            candidate = _expand(alpha, current, data, allowed[:, alpha], lo, hi, smoothness)
            candidate_energy = _energy(candidate, data, lo, hi, smoothness)
            if candidate_energy < energy - 1e-9:
                current, energy, improved = candidate, candidate_energy, True
        if not improved:
            break

    if return_energy:
        return current - 1, start_energy
    return current - 1


def grid_index(xy: np.ndarray, resolution_metres: float):
    """An image of pixel indices (-1 where there is no pixel), and each pixel's row and column."""
    origin = xy.min(axis=0)
    cols = np.rint((xy[:, 0] - origin[0]) / resolution_metres).astype(int)
    rows = np.rint((xy[:, 1] - origin[1]) / resolution_metres).astype(int)
    grid = np.full((rows.max() + 1, cols.max() + 1), -1, dtype=int)
    grid[rows, cols] = np.arange(len(xy))
    return grid, rows, cols


def _fit_plane(xy: np.ndarray, z: np.ndarray):
    centre = xy.mean(axis=0)
    design = np.column_stack([xy - centre, np.ones(len(xy))])
    (a, b, c), *_ = np.linalg.lstsq(design, z, rcond=None)
    return lambda pts: (pts - centre) @ (a, b) + c


def _neighbour_pairs(grid: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
    vert = (grid[:-1, :] >= 0) & (grid[1:, :] >= 0)
    horz = (grid[:, :-1] >= 0) & (grid[:, 1:] >= 0)
    lo = np.concatenate([grid[:-1, :][vert], grid[:, :-1][horz]])
    hi = np.concatenate([grid[1:, :][vert], grid[:, 1:][horz]])
    return lo, hi


def _energy(labels: np.ndarray, data: np.ndarray, lo: np.ndarray, hi: np.ndarray, smoothness: float) -> float:
    return float(data[np.arange(len(labels)), labels].sum()
                 + smoothness * np.count_nonzero(labels[lo] != labels[hi]))


def _expand(alpha: int,
            labels: np.ndarray,
            data: np.ndarray,
            allowed: np.ndarray,
            lo: np.ndarray,
            hi: np.ndarray,
            smoothness: float) -> np.ndarray:
    """
    One alpha-expansion move: every allowed pixel either keeps its label or switches to
    `alpha`, whichever labelling minimises the energy, by min-cut. Source side = keep.

    Each pairwise term E(x_p, x_q) with values A, B, C, D at (0,0), (0,1), (1,0), (1,1) is
    A + (C - A) x_p + (D - C) x_q + (B + C - A - D) (1 - x_p) x_q; the last coefficient is
    non-negative because Potts is a metric, so it becomes the edge p -> q.
    """
    movable = allowed & (labels != alpha)
    nodes = np.flatnonzero(movable)
    if len(nodes) == 0:
        return labels
    local = np.full(len(labels), -1)
    local[nodes] = np.arange(len(nodes))

    u0 = data[nodes, labels[nodes]].copy()
    u1 = data[nodes, alpha].copy()

    a, b = labels[lo], labels[hi]
    differ = smoothness * (a != b)
    a_to_alpha = smoothness * (a != alpha)
    alpha_to_b = smoothness * (b != alpha)

    both = movable[lo] & movable[hi]
    only_lo = movable[lo] & ~movable[hi]
    only_hi = ~movable[lo] & movable[hi]

    # a pair with one pixel fixed is a unary term on the other
    np.add.at(u0, local[lo[only_lo]], differ[only_lo])
    np.add.at(u1, local[lo[only_lo]], alpha_to_b[only_lo])
    np.add.at(u0, local[hi[only_hi]], differ[only_hi])
    np.add.at(u1, local[hi[only_hi]], a_to_alpha[only_hi])

    p, q = local[lo[both]], local[hi[both]]
    A, B, C = differ[both], a_to_alpha[both], alpha_to_b[both]
    np.add.at(u1, p, C - A)
    np.add.at(u1, q, -C)
    pair_w = B + C - A

    base = np.minimum(u0, u1)
    u0, u1 = u0 - base, u1 - base

    n = len(nodes)
    src, snk = n, n + 1
    ids = np.arange(n)
    rows = np.concatenate([np.full(n, src), ids, p])
    cols = np.concatenate([ids, np.full(n, snk), q])
    caps = np.rint(np.concatenate([u1, u0, pair_w]) * _ENERGY_SCALE).astype(np.int32)
    keep = caps > 0
    cap = sp.coo_matrix((caps[keep], (rows[keep], cols[keep])), shape=(n + 2, n + 2)).tocsr()

    residual = cap - maximum_flow(cap, src, snk).flow
    residual.eliminate_zeros()  # a saturated arc must not be traversable
    reached = breadth_first_order(residual, src, directed=True, return_predecessors=False)
    keep_label = np.zeros(n + 2, dtype=bool)
    keep_label[reached] = True

    new_labels = labels.copy()
    new_labels[nodes[~keep_label[:n]]] = alpha
    return new_labels
