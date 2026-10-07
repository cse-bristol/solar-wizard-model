# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
"""
Reject clutter and messy roofs, from the pixels' elevations once they have been assigned
to planes (see `assign_pixels`).

* Clutter: a small pitched plane within a flat roof is not a roof, but an obstacle
  (plant, vents, rooflights) that a plane has been fitted to. Within means little of
  its surroundings are outside the building, and a good share of them are flat roof.
  Fit quality doesn't distinguish clutter: real small roof planes fit as poorly.
* A messy flat roof: one with a large share of its area covered by obstacles - pixels
  standing clearly above it, either enclosed by it or around its edge - or with many
  separate obstacles. Obstacles touching the edge of the building aren't counted, as
  they are usually parapets.
* A messy building: one that is mostly clutter, messy flat roofs and obstacles.
"""
from dataclasses import dataclass
from typing import Dict, List

import numpy as np
from scipy import ndimage

from solar_pv.datatypes import RoofPlane
from solar_pv.roof_detection.assign_pixels import grid_index

CLUTTER_MAX_AREA_M2 = 40
CLUTTER_MAX_OUTSIDE_SHARE = 0.25
CLUTTER_MIN_FLAT_SHARE = 0.25
OBSTACLE_MIN_HEIGHT_M = 0.2
MESSY_OBSTACLE_SHARE = 0.14
MESSY_OBSTACLE_GROUPS = 5
MESSY_BUILDING_SHARE = 0.85

_FOUR_CONNECTED = ndimage.generate_binary_structure(2, 1)


@dataclass
class PlaneMess:
    pixels: int
    clutter: bool
    obstacle_pixels: int = 0
    obstacle_groups: int = 0

    @property
    def obstacle_share(self) -> float:
        return self.obstacle_pixels / self.pixels

    @property
    def messy(self) -> bool:
        return self.clutter \
            or self.obstacle_share >= MESSY_OBSTACLE_SHARE \
            or self.obstacle_groups >= MESSY_OBSTACLE_GROUPS


def reject_mess(planes: Dict[int, RoofPlane],
                labels: np.ndarray,
                xyz: np.ndarray,
                resolution_metres: float,
                debug: bool = False) -> List[RoofPlane]:
    """
    The planes that are neither clutter nor messy flat roofs, or none if the building as
    a whole is messy. `labels` gives each pixel of `xyz` its plane id, or anything not a
    key of `planes` if it has none.
    """
    mess, obstacles = plane_mess(planes, labels, xyz, resolution_metres)

    mess_pixels = sum(m.pixels for m in mess.values() if m.messy) + np.count_nonzero(obstacles)
    total_pixels = sum(m.pixels for m in mess.values()) + np.count_nonzero(obstacles)
    building_share = mess_pixels / total_pixels if total_pixels > 0 else 0

    if debug:
        for plane_id, m in mess.items():
            print(f"plane {plane_id}: {m}, obstacle share {m.obstacle_share:.2f}, messy {m.messy}")
        print(f"building mess share {building_share:.2f}")

    if building_share >= MESSY_BUILDING_SHARE:
        return []
    return [planes[plane_id] for plane_id, m in mess.items() if not m.messy]


def plane_mess(planes: Dict[int, RoofPlane],
               labels: np.ndarray,
               xyz: np.ndarray,
               resolution_metres: float):
    """Per plane, its `PlaneMess`; and a mask of the pixels that are obstacles on flat roofs."""
    grid, rows, cols = grid_index(xyz[:, :2], resolution_metres)
    # padded, so a plane's surroundings can include pixels outside the building:
    grid = np.pad(grid, 1, constant_values=-1)
    rows, cols = rows + 1, cols + 1
    present = grid >= 0
    label_img = np.full(grid.shape, -1)
    label_img[rows, cols] = _plane_index(labels, planes)
    x_img, y_img, z_img = (np.full(grid.shape, np.nan) for _ in range(3))
    x_img[rows, cols], y_img[rows, cols], z_img[rows, cols] = xyz[:, 0], xyz[:, 1], xyz[:, 2]

    # pixels with a neighbour missing are at the edge of the building:
    at_edge = present & ~ndimage.binary_erosion(present, _FOUR_CONNECTED)

    plane_ids = list(planes.keys())
    flat = np.isin(label_img, [k for k, plane_id in enumerate(plane_ids) if planes[plane_id]['is_flat']])
    mess = {}
    for k, plane_id in enumerate(plane_ids):
        mask = label_img == k
        pixels = int(np.count_nonzero(mask))
        clutter = False
        if not planes[plane_id]['is_flat'] and pixels * resolution_metres ** 2 < CLUTTER_MAX_AREA_M2:
            around = ndimage.binary_dilation(mask, _FOUR_CONNECTED) & ~mask
            outside_share = np.count_nonzero(around & ~present) / np.count_nonzero(around)
            around_present = np.count_nonzero(around & present)
            flat_share = np.count_nonzero(around & flat) / around_present if around_present else 0
            clutter = outside_share < CLUTTER_MAX_OUTSIDE_SHARE and flat_share >= CLUTTER_MIN_FLAT_SHARE
        mess[plane_id] = PlaneMess(pixels=pixels, clutter=clutter)

    good = np.zeros(grid.shape, dtype=bool)
    for k, plane_id in enumerate(plane_ids):
        if not mess[plane_id].clutter:
            good |= label_img == k

    all_obstacles = np.zeros(grid.shape, dtype=bool)
    for k, plane_id in enumerate(plane_ids):
        plane = planes[plane_id]
        if not plane['is_flat'] or mess[plane_id].clutter:
            continue
        mask = label_img == k
        enclosed = ndimage.binary_fill_holes(mask) & ~mask
        ring = ndimage.binary_dilation(mask, _FOUR_CONNECTED) & ~mask
        candidates = (enclosed | ring) & present & ~good

        height = z_img - (plane['x_coef'] * x_img + plane['y_coef'] * y_img + plane['intercept'])
        above = candidates & (np.nan_to_num(height, nan=-np.inf) > OBSTACLE_MIN_HEIGHT_M)

        groups, n_groups = ndimage.label(above, _FOUR_CONNECTED)
        edge_groups = np.unique(groups[above & at_edge])
        obstacles = above & ~np.isin(groups, edge_groups)
        mess[plane_id].obstacle_pixels = int(np.count_nonzero(obstacles))
        mess[plane_id].obstacle_groups = n_groups - len(edge_groups[edge_groups > 0])
        all_obstacles |= obstacles

    return mess, all_obstacles


def _plane_index(labels: np.ndarray, planes: Dict[int, RoofPlane]) -> np.ndarray:
    """`labels` as indexes into the list of `planes`' keys, or -1 for no plane."""
    index = np.full(labels.shape, -1)
    for k, plane_id in enumerate(planes.keys()):
        index[labels == plane_id] = k
    return index
