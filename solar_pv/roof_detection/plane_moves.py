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
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Tuple

import numpy as np

from solar_pv.geos import slope_deg, aspect_deg
from solar_pv.roof_detection.assign_pixels import grid_index, _neighbour_pairs, RESIDUAL_CAP, NONE_COST, \
    SMOOTHNESS_PER_M


@dataclass
class Plane:
    """
    A plane while planes are refined, merged and removed: only what those use, all of which
    is kept up to date. What else is known of it (fit metrics, shape ratios and so on) is
    from when it was `proposed`, and only computed afresh once the planes are final.
    """
    coef: Tuple[float, float, float]
    is_flat: bool
    plane_type: str
    plane_id: str
    # its pixels when last refitted:
    xy: np.ndarray
    proposed: dict

    @property
    def slope(self) -> float:
        return slope_deg(self.coef[0], self.coef[1])

    @property
    def aspect_raw(self) -> float:
        return aspect_deg(self.coef[0], self.coef[1])

    def record(self) -> dict:
        """The plane as a `RoofPlane`-like dict, for recording it if it's thrown away."""
        return {**self.proposed,
                'x_coef': self.coef[0], 'y_coef': self.coef[1], 'intercept': self.coef[2],
                'slope': self.slope, 'is_flat': self.is_flat, 'aspect_raw': self.aspect_raw,
                'plane_type': self.plane_type, 'plane_id': self.plane_id, 'inliers_xy': self.xy}


def data_cost(xyz: np.ndarray, coef: Tuple[float, float, float], residual_scale: float) -> np.ndarray:
    """`assign_pixels`' cost of each pixel of `xyz` on the plane z = a x + b y + c."""
    a, b, c = coef
    residual = np.abs(xyz[:, 2] - (xyz[:, 0] * a + xyz[:, 1] * b + c))
    return np.minimum(residual / residual_scale, RESIDUAL_CAP)


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
                    planes: Dict[int, Plane],
                    resolution_metres: float,
                    label_cost: float,
                    residual_scale,
                    fit: Callable[[np.ndarray], Tuple[Tuple[float, float, float], bool]],
                    mergeable: Callable[[Plane, Plane], bool] = lambda p, q: True,
                    good_enough_share: Optional[float] = None) -> List[Tuple[int, int]]:
    """
    Greedily merge (in place) the adjacent pair of planes whose merge lowers the energy
    most, until no merge would. Only pairs that are `mergeable` are considered, and a pair
    whose merged plane explains (has a data cost of at most 1 for) at least
    `good_enough_share` of their pixels is merged even if that raises the energy. A merged plane keeps the first plane's id, and is refitted to the
    pixels of both: `fit(xyz)` is a plane's coefficients and flatness, fitted as the planes
    are refitted when refining, so that a merge is judged on the plane it would become.
    `residual_scale(is_flat)` is the residual scale for a plane.

    Returns the (kept, merged away) plane ids.
    """
    smoothness = SMOOTHNESS_PER_M / resolution_metres
    idxs = {plane_id: np.flatnonzero(labels == plane_id) for plane_id in planes}
    costs = {plane_id: data_cost(xyz[idxs[plane_id]], plane.coef, residual_scale(plane.is_flat)).sum()
             for plane_id, plane in planes.items()}
    edges = adjacent_planes(xyz, labels, resolution_metres)

    def gain(pair):
        p, q = pair
        if not mergeable(planes[p], planes[q]):
            return -np.inf, None, False
        union = np.concatenate([idxs[p], idxs[q]])
        coef, is_flat = fit(xyz[union])
        merged = data_cost(xyz[union], coef, residual_scale(is_flat))
        merged_cost = merged.sum()
        g = costs[p] + costs[q] + smoothness * edges[pair] + label_cost - merged_cost
        good_enough = good_enough_share is not None and np.mean(merged <= 1) >= good_enough_share
        return g, (coef, is_flat, merged_cost), g > 0 or good_enough

    gains = {pair: gain(pair) for pair in edges}
    merges = []
    while gains:
        acceptable = {pair: g for pair, g in gains.items() if g[2]}
        if not acceptable:
            break
        pair, (_, merged, _) = max(acceptable.items(), key=lambda kv: kv[1][0])
        keep, gone = pair
        coef, is_flat, merged_cost = merged
        plane = planes[keep]
        plane.coef, plane.is_flat = coef, is_flat
        plane.plane_type = plane.plane_type + "_MERGED_" + planes[gone].plane_type
        plane.plane_id = plane.plane_id + "_MERGED_" + planes[gone].plane_id
        del planes[gone]
        labels[idxs[gone]] = keep
        idxs[keep] = np.concatenate([idxs[keep], idxs.pop(gone)])
        plane.xy = xyz[idxs[keep], :2]
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


def remove_unprofitable(xyz: np.ndarray,
                        labels: np.ndarray,
                        planes: Dict[int, Plane],
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
            cost = data_cost(xyz[idxs], plane.coef, residual_scale(plane.is_flat))
            alternative = np.full(len(idxs), NONE_COST)
            for other in neighbours[plane_id]:
                other_plane = planes[other]
                alternative = np.minimum(alternative, data_cost(xyz[idxs], other_plane.coef,
                                                                residual_scale(other_plane.is_flat)))
            profit[plane_id] = (alternative - cost).sum()
        worst = min(profit, key=profit.get)
        if profit[worst] >= label_cost:
            break
        del planes[worst]
        labels[labels == worst] = nodata
        removed.append(worst)
    return removed
