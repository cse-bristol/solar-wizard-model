# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
"""
Changes to the set of planes that `assign_pixels` can't make, judged by its energy plus a
cost per plane (`label_cost`), so that a plane is only worth having if it explains its
pixels better than the other planes would by at least that much (as in PEaRL: Isack &
Boykov 2012):

* merging two adjacent planes, when the fit lost is worth less than the boundary and the
  plane saved - or, optionally, when the merged plane still explains their pixels well;
* removing a plane, when its pixels would cost less than a plane's worth more on the
  neighbouring planes or unassigned.

Both compare energies with the planes as they are, so should be followed by assigning
pixels again. Removal ignores how boundaries would change, as the assignment that follows
settles those.
"""
from collections import defaultdict
from typing import Callable, Dict, List, Tuple

import numpy as np

from solar_pv.constants import FLAT_ROOF_DEGREES_THRESHOLD
from solar_pv.geos import slope_deg, aspect_deg
from solar_pv.roof_detection.assign_pixels import grid_index, _neighbour_pairs, RESIDUAL_CAP, NONE_COST, \
    SMOOTHNESS_PER_M
from solar_pv.roof_detection.plane_fit import PlaneFit


def data_cost(xyz: np.ndarray, coef: Tuple[float, float, float], residual_scale: float) -> np.ndarray:
    """`assign_pixels`' cost of each pixel of `xyz` on the plane z = a x + b y + c."""
    a, b, c = coef
    residual = np.abs(xyz[:, 2] - (xyz[:, 0] * a + xyz[:, 1] * b + c))
    return np.minimum(residual / residual_scale, RESIDUAL_CAP)


def _coef(plane: dict) -> Tuple[float, float, float]:
    return plane['x_coef'], plane['y_coef'], plane['intercept']


def adjacent_planes(xyz: np.ndarray, labels: np.ndarray, resolution_metres: float) -> Dict[Tuple[int, int], int]:
    """The number of 4-neighbour pixel edges between each pair (lower id first) of planes."""
    grid, _, _ = grid_index(xyz[:, :2], resolution_metres)
    lo, hi = _neighbour_pairs(grid)
    a, b = labels[lo], labels[hi]
    between = (a >= 0) & (b >= 0) & (a != b)
    pairs = np.column_stack([np.minimum(a[between], b[between]), np.maximum(a[between], b[between])])
    if len(pairs) == 0:
        return {}
    unique, counts = np.unique(pairs, axis=0, return_counts=True)
    return {(int(p), int(q)): int(n) for (p, q), n in zip(unique, counts)}


def merge_by_energy(xyz: np.ndarray,
                    labels: np.ndarray,
                    planes: Dict[int, dict],
                    resolution_metres: float,
                    label_cost: float,
                    residual_scale,
                    mergeable: Callable[[dict, dict], bool] = lambda p, q: True,
                    good_enough_cost: float = 0.0) -> List[Tuple[int, int]]:
    """
    Greedily merge (in place) the adjacent pair of planes whose merge lowers the energy
    most, until no merge would. Only pairs that are `mergeable` are considered, and a pair
    whose merged plane costs at most `good_enough_cost` per pixel is merged even if that
    raises the energy. A merged
    plane keeps the first plane's id and has its coefficients (and slope, aspect and
    flatness) refitted; its other attributes are left for the caller to refit.
    `residual_scale(is_flat)` is the residual scale for a plane.

    Returns the (kept, merged away) plane ids.
    """
    smoothness = SMOOTHNESS_PER_M / resolution_metres
    idxs = {plane_id: np.flatnonzero(labels == plane_id) for plane_id in planes}
    costs = {plane_id: data_cost(xyz[idxs[plane_id]], _coef(plane), residual_scale(plane['is_flat'])).sum()
             for plane_id, plane in planes.items()}
    edges = adjacent_planes(xyz, labels, resolution_metres)

    def gain(pair):
        p, q = pair
        if not mergeable(planes[p], planes[q]):
            return -np.inf, None, False
        union = np.concatenate([idxs[p], idxs[q]])
        fit = _merged_fit(xyz[union], residual_scale)
        if fit is None:
            return -np.inf, None, False
        coef, is_flat, merged_cost = fit
        g = costs[p] + costs[q] + smoothness * edges[pair] + label_cost - merged_cost
        return g, fit, g > 0 or merged_cost <= good_enough_cost * len(union)

    gains = {pair: gain(pair) for pair in edges}
    merges = []
    while gains:
        acceptable = {pair: g for pair, g in gains.items() if g[2]}
        if not acceptable:
            break
        pair, (_, fit, _) = max(acceptable.items(), key=lambda kv: kv[1][0])
        keep, gone = pair
        coef, is_flat, merged_cost = fit
        plane = planes[keep]
        plane['x_coef'], plane['y_coef'], plane['intercept'] = coef
        plane['is_flat'] = is_flat
        plane['slope'] = slope_deg(coef[0], coef[1])
        plane['aspect_raw'] = aspect_deg(coef[0], coef[1])
        plane['plane_type'] = plane['plane_type'] + "_MERGED_" + planes[gone]['plane_type']
        plane['plane_id'] = plane['plane_id'] + "_MERGED_" + planes[gone]['plane_id']
        del planes[gone]
        labels[idxs[gone]] = keep
        idxs[keep] = np.concatenate([idxs[keep], idxs.pop(gone)])
        costs[keep] = merged_cost
        del costs[gone]
        merges.append((keep, gone))

        # the merged plane's boundaries are the sum of both planes':
        new_edges = defaultdict(int)
        for (p, q), n in edges.items():
            p, q = (keep if p == gone else p), (keep if q == gone else q)
            if p != q:
                new_edges[(min(p, q), max(p, q))] += n
        edges = dict(new_edges)
        gains = {pair: (gains[pair] if keep not in pair and pair in gains else gain(pair)) for pair in edges}
    return merges


def _merged_fit(xyz: np.ndarray, residual_scale):
    """
    A plane through `xyz`, and its energy. Fitted by least squares, then again to just
    the pixels it would be assigned (those cheaper on it than unassigned), so that a few
    pixels far off it don't drag it away from the rest. None if `xyz` is too few pixels.
    """
    if len(xyz) < 3:
        return None
    lr = PlaneFit().fit(xyz[:, :2], xyz[:, 2])
    is_flat = slope_deg(*lr.coef_) <= FLAT_ROOF_DEGREES_THRESHOLD
    coef = (lr.coef_[0], lr.coef_[1], lr.intercept_)
    kept = data_cost(xyz, coef, residual_scale(is_flat)) < NONE_COST
    if 3 <= np.count_nonzero(kept) < len(xyz):
        lr = PlaneFit().fit(xyz[kept, :2], xyz[kept, 2])
        is_flat = slope_deg(*lr.coef_) <= FLAT_ROOF_DEGREES_THRESHOLD
        coef = (lr.coef_[0], lr.coef_[1], lr.intercept_)
    return coef, is_flat, data_cost(xyz, coef, residual_scale(is_flat)).sum()


def remove_unprofitable(xyz: np.ndarray,
                        labels: np.ndarray,
                        planes: Dict[int, dict],
                        resolution_metres: float,
                        label_cost: float,
                        residual_scale,
                        nodata: int) -> List[int]:
    """
    Remove (in place) planes one at a time, least profitable first, while some plane's
    pixels would cost less than `label_cost` more on its neighbouring planes or
    unassigned. Their pixels are left unassigned (`nodata`). Returns the removed ids.
    """
    removed = []
    while planes:
        edges = adjacent_planes(xyz, labels, resolution_metres)
        neighbours = defaultdict(set)
        for p, q in edges:
            neighbours[p].add(q)
            neighbours[q].add(p)
        profit = {}
        for plane_id, plane in planes.items():
            idxs = np.flatnonzero(labels == plane_id)
            cost = data_cost(xyz[idxs], _coef(plane), residual_scale(plane['is_flat']))
            alternative = np.full(len(idxs), NONE_COST)
            for other in neighbours[plane_id]:
                other_plane = planes[other]
                alternative = np.minimum(alternative, data_cost(xyz[idxs], _coef(other_plane),
                                                                residual_scale(other_plane['is_flat'])))
            profit[plane_id] = (alternative - cost).sum()
        worst = min(profit, key=profit.get)
        if profit[worst] >= label_cost:
            break
        del planes[worst]
        labels[labels == worst] = nodata
        removed.append(worst)
    return removed
