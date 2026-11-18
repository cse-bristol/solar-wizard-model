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
from scipy import ndimage
from scipy.ndimage import distance_transform_edt
from scipy.sparse.csgraph import breadth_first_order, maximum_flow

NONE = -1
RESIDUAL_CAP = 4.0
NONE_COST = 2.0
SMOOTHNESS_PER_M = 0.5
# maximum_flow needs int32 capacities: energy is quantized to this many steps per unit.
_ENERGY_SCALE = 1000
_FOUR_CONNECTED = ndimage.generate_binary_structure(2, 1)


def assign_pixels(xyz: np.ndarray,
                  labels: np.ndarray,
                  coefs: Sequence[Tuple[float, float, float]],
                  residual_scales: Sequence[float],
                  resolution_metres: float,
                  residual_cap: float = RESIDUAL_CAP,
                  none_cost: float = NONE_COST,
                  smoothness_per_m: float = SMOOTHNESS_PER_M,
                  max_dist_m: float = 3,
                  max_cycles: int = 5,
                  extra_costs: Optional[Sequence[Optional[np.ndarray]]] = None) -> Tuple[np.ndarray, float]:
    """
    Reassign `labels` (per pixel of `xyz`: a plane index in [0, len(coefs)), or NONE), and
    return them with the energy of the starting labels. Plane k is z = a x + b y + c for
    (a, b, c) = coefs[k], with residual scale `residual_scales[k]`, and if `extra_costs[k]`
    isn't None, that cost per pixel of being on it too.

    Each plane may only claim pixels within `max_dist_m` of those it starts with, so a
    coplanar patch elsewhere on the building can't be taken over. Planes that start with
    fewer than 3 pixels are not assigned any.

    A pixel's data cost stands for its area and a boundary's smoothness cost for its
    length, so per pixel edge the smoothness is `smoothness_per_m` scaled by 1/resolution:
    otherwise boundaries would cost relatively more at coarser resolutions, and small
    planes would lose out.

    Each plane keeps only its largest 4-connected group of pixels, the others being
    unassigned, and a move is only made if it lowers the energy after that: so a plane is
    only cut in two (losing the smaller part) if that's worth its cost. A move that would
    cut a plane is retried without taking any of that plane's pixels.
    """
    n_planes = len(coefs)
    smoothness = smoothness_per_m / resolution_metres
    max_dist_pixels = max_dist_m / resolution_metres
    xy = xyz[:, :2]
    z = xyz[:, 2]
    grid, rows, cols = grid_index(xy, resolution_metres)

    # internally, label 0 is none and plane k is k + 1
    current = labels + 1
    # per label, each pixel's data cost and whether it may take that label:
    data = np.full((n_planes + 1, len(z)), none_cost)
    allowed = np.zeros((n_planes + 1, len(z)), dtype=bool)
    allowed[0] = True
    for k in range(1, n_planes + 1):
        idxs = np.flatnonzero(current == k)
        if len(idxs) < 3:
            current[idxs] = 0
            continue
        a, b, c = coefs[k - 1]
        predicted = xy[:, 0] * a + xy[:, 1] * b + c
        data[k] = np.minimum(np.abs(z - predicted) / residual_scales[k - 1], residual_cap)
        if extra_costs is not None and extra_costs[k - 1] is not None:
            data[k] += extra_costs[k - 1]

        allowed[k] = _within(rows, cols, rows[idxs], cols[idxs], max_dist_pixels)

    lo, hi = _neighbour_pairs(grid)
    start_energy = _energy(current, data, lo, hi, smoothness)
    current, _ = _largest_components(current, np.unique(current), grid, rows, cols)
    # Per label, the edges with an end it may claim - the only ones its expansions involve -
    # and the pixels at their ends, whose labels are all an expansion depends on, but for
    # the shapes of the planes there. So a rejected expansion would be
    # rejected again until one of those changes, and isn't tried till then. Changes are
    # stamped with the number of moves made so far.
    near, reach, planes_near = [None] * (n_planes + 1), [None] * (n_planes + 1), [None] * (n_planes + 1)
    rejected_at = np.full(n_planes + 1, -1)
    pixel_changed_at = np.zeros(len(z), dtype=int)
    plane_changed_at = np.zeros(n_planes + 1, dtype=int)
    moves = 0
    for _ in range(max_cycles):
        improved = False
        for alpha in range(n_planes + 1):
            if near[alpha] is None:
                near[alpha] = np.flatnonzero(allowed[alpha][lo] | allowed[alpha][hi])
                ends = allowed[alpha].copy()
                ends[lo[near[alpha]]] = ends[hi[near[alpha]]] = True
                reach[alpha] = np.flatnonzero(ends)
            elif rejected_at[alpha] >= 0 and len(reach[alpha]) and \
                    pixel_changed_at[reach[alpha]].max() <= rejected_at[alpha] and \
                    plane_changed_at[planes_near[alpha]].max(initial=0) <= rejected_at[alpha]:
                continue
            edges = near[alpha]
            candidate = _expand(alpha, current, data, allowed[alpha], lo, hi, smoothness, edges)
            candidate, cut = _connected_move(alpha, current, candidate, grid, rows, cols)
            if cut.size and _energy_change(current, candidate, data, lo, hi, smoothness) >= -1e-9:
                retry = _expand(alpha, current, data, allowed[alpha] & ~np.isin(current, cut), lo, hi,
                                smoothness, edges)
                candidate, _ = _connected_move(alpha, current, retry, grid, rows, cols)
            if _energy_change(current, candidate, data, lo, hi, smoothness) < -1e-9:
                moves += 1
                changed = np.flatnonzero(candidate != current)
                pixel_changed_at[changed] = moves
                plane_changed_at[current[changed]] = plane_changed_at[candidate[changed]] = moves
                current, improved = candidate, True
            else:
                rejected_at[alpha] = moves
                # none is never cut, so its shape doesn't matter:
                planes_near[alpha] = np.flatnonzero(np.bincount(current[reach[alpha]], minlength=n_planes + 1)[1:]) + 1
        if not improved:
            break

    return current - 1, start_energy


def _connected_move(alpha: int, current: np.ndarray, candidate: np.ndarray, grid: np.ndarray, rows: np.ndarray,
                    cols: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
    """`candidate`, an expansion of `alpha` from `current`, with each plane it changed
    keeping only its largest group of pixels; and the planes other than `alpha` that lost
    pixels by that."""
    changed = candidate != current
    if not changed.any():
        # every plane is already a single group
        return candidate, np.array([], dtype=int)
    affected = np.unique(np.concatenate([current[changed], [alpha]]))
    return _largest_components(candidate, affected[affected != alpha], grid, rows, cols, alpha)


def _largest_components(labels: np.ndarray, planes: np.ndarray, grid: np.ndarray, rows: np.ndarray,
                        cols: np.ndarray, extra: int = 0) -> Tuple[np.ndarray, np.ndarray]:
    """`labels` with each of `planes` (and `extra`) keeping only its largest 4-connected
    group of pixels, the rest being none; and those of `planes` that lost pixels."""
    labels = labels.copy()
    cut = []
    for k in np.concatenate([planes, [extra]]):
        if k == 0:
            continue
        idxs = np.flatnonzero(labels == k)
        if len(idxs) < 2:
            continue
        r, c = rows[idxs], cols[idxs]
        r0, c0 = r.min(), c.min()
        image = np.zeros((r.max() - r0 + 1, c.max() - c0 + 1), dtype=bool)
        image[r - r0, c - c0] = True
        groups, n = ndimage.label(image, _FOUR_CONNECTED)
        if n < 2:
            continue
        group = groups[r - r0, c - c0]
        labels[idxs[group != np.argmax(np.bincount(group)[1:]) + 1]] = 0
        if k != extra:
            cut.append(k)
    return labels, np.array(cut, dtype=int)


def grid_index(xy: np.ndarray, resolution_metres: float):
    """An image of pixel indices (-1 where there is no pixel), and each pixel's row and column."""
    origin = xy.min(axis=0)
    cols = np.rint((xy[:, 0] - origin[0]) / resolution_metres).astype(int)
    rows = np.rint((xy[:, 1] - origin[1]) / resolution_metres).astype(int)
    grid = np.full((rows.max() + 1, cols.max() + 1), -1, dtype=int)
    grid[rows, cols] = np.arange(len(xy))
    return grid, rows, cols


def _neighbour_pairs(grid: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
    vert = (grid[:-1, :] >= 0) & (grid[1:, :] >= 0)
    horz = (grid[:, :-1] >= 0) & (grid[:, 1:] >= 0)
    lo = np.concatenate([grid[:-1, :][vert], grid[:, :-1][horz]])
    hi = np.concatenate([grid[1:, :][vert], grid[:, 1:][horz]])
    return lo, hi


def _within(rows: np.ndarray, cols: np.ndarray, start_rows: np.ndarray, start_cols: np.ndarray,
            max_dist: float) -> np.ndarray:
    """Which pixels (`rows`, `cols`) are within `max_dist` (pixels) of a start pixel. The
    distance transform is of just the start pixels' bounding box, grown by `max_dist`."""
    pad = int(np.ceil(max_dist)) + 1
    r0, c0 = start_rows.min() - pad, start_cols.min() - pad
    start_image = np.zeros((start_rows.max() + pad + 1 - r0, start_cols.max() + pad + 1 - c0), dtype=bool)
    start_image[start_rows - r0, start_cols - c0] = True
    r, c = rows - r0, cols - c0
    inside = (r >= 0) & (r < start_image.shape[0]) & (c >= 0) & (c < start_image.shape[1])
    within = np.zeros(len(rows), dtype=bool)
    within[inside] = distance_transform_edt(~start_image)[r[inside], c[inside]] <= max_dist
    return within


def _energy(labels: np.ndarray, data: np.ndarray, lo: np.ndarray, hi: np.ndarray, smoothness: float) -> float:
    return float(data[labels, np.arange(len(labels))].sum()
                 + smoothness * np.count_nonzero(labels[lo] != labels[hi]))


def _energy_change(labels: np.ndarray, new_labels: np.ndarray, data: np.ndarray, lo: np.ndarray, hi: np.ndarray,
                   smoothness: float) -> float:
    """The energy of `new_labels` less that of `labels`, from just the pixels that differ."""
    changed = labels != new_labels
    idxs = np.flatnonzero(changed)
    if len(idxs) == 0:
        return 0.0
    edges = np.flatnonzero(changed[lo] | changed[hi])
    l, h = lo[edges], hi[edges]
    return float(data[new_labels[idxs], idxs].sum() - data[labels[idxs], idxs].sum()
                 + smoothness * (np.count_nonzero(new_labels[l] != new_labels[h])
                                 - np.count_nonzero(labels[l] != labels[h])))


def _expand(alpha: int,
            labels: np.ndarray,
            data: np.ndarray,
            allowed: np.ndarray,
            lo: np.ndarray,
            hi: np.ndarray,
            smoothness: float,
            edges: Optional[np.ndarray] = None) -> np.ndarray:
    """
    One alpha-expansion move: every allowed pixel either keeps its label or switches to
    `alpha`, whichever labelling minimises the energy, by min-cut. Source side = keep.
    `edges` (indices into `lo`/`hi`, in order) must include every edge with an allowed end;
    if None, all are looked at.

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

    # only edges with a movable end matter, and keeping them in order keeps the sums below in order
    if edges is not None:
        lo, hi = lo[edges], hi[edges]
    touching = movable[lo] | movable[hi]
    lo, hi = lo[touching], hi[touching]

    u0 = data[labels[nodes], nodes]
    u1 = data[alpha, nodes]

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
