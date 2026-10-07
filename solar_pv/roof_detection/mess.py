# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
"""
Reject clutter and messy roofs, from the pixels' elevations once they have been assigned
to planes (see `assign_pixels`).

* Clutter: a small pitched plane within a flat roof is not a roof, but an obstacle
  (plant, vents, rooflights) that a plane has been fitted to. Within means little of
  its surroundings are outside the building, and a good share of them are flat roof.
  Fit quality doesn't distinguish clutter: real small roof planes fit as poorly.
* A messy flat roof: one with a large share of its area covered by obstacles, or with
  many separate obstacles. Obstacles are clutter standing on it; groups of unassigned
  pixels clearly above it (plant) or below it (rooflights, lightwells) and mostly
  surrounded by it; and groups of its own pixels a little above or below the rest of it
  (low plant, pipes, vents). Groups touching the edge of the building aren't counted, as
  they are usually parapets or taller parts of the building.
* Surrounded by mess: a small pitched plane whose surroundings within the building are
  mostly mess (clutter, messy planes, obstacles, and unassigned pixels away from the edge
  of the building and above the ground) is usually not a roof, but a plane fitted to part
  of the mess - even at the edge of the building, where it can't be clutter.
* A messy building: one that is mostly clutter, messy flat roofs and obstacles.
"""
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional

import numpy as np
from scipy import ndimage

from solar_pv.datatypes import RoofPlane
from solar_pv.roof_detection import rejections
from solar_pv.roof_detection.assign_pixels import grid_index

CLUTTER_MAX_AREA_M2 = 40
CLUTTER_MAX_OUTSIDE_SHARE = 0.25
CLUTTER_MIN_FLAT_SHARE = 0.25
OBSTACLE_MIN_HEIGHT_M = 0.2
# Of an obstacle's border, more than this must be the flat roof it is on: pixels on the
# line between two planes are half bordered by each.
OBSTACLE_MIN_BORDER_SHARE = 0.5
# Residual blobs (see `_residual_blobs`): at least BLOB_MIN_AREA_M2 of pixels each at least
# BLOB_MIN_RESIDUAL_M off the local median, whose summed residual is BLOB_MIN_SIGNIFICANCE
# times what BLOB_NOISE_M of noise per pixel would give. Clean flat roofs' local noise is
# 0.005-0.02m at 1m resolution.
BLOB_WINDOW_M = 5
BLOB_MIN_RESIDUAL_M = 0.06
BLOB_NOISE_M = 0.03
BLOB_MIN_SIGNIFICANCE = 4
BLOB_MIN_AREA_M2 = 2
# Of a small pitched plane's surroundings within the building, the share that must be mess
# for it to be rejected as surrounded by mess.
SURROUNDED_BY_MESS_SHARE = 0.75
MESSY_OBSTACLE_SHARE = 0.14
# A flat roof is messy with at least this many obstacles, or this many per m2 if more: a
# few obstacles on a large roof leave most of it clear.
MESSY_OBSTACLE_GROUPS = 5
MESSY_OBSTACLE_GROUPS_PER_M2 = 0.02
MESSY_BUILDING_SHARE = 0.85

_FOUR_CONNECTED = ndimage.generate_binary_structure(2, 1)


@dataclass
class PlaneMess:
    pixels: int
    area_m2: float
    clutter: bool
    obstacle_pixels: int = 0
    obstacle_groups: int = 0
    surrounded_by_mess: bool = False

    @property
    def obstacle_share(self) -> float:
        return self.obstacle_pixels / self.pixels

    @property
    def messy(self) -> bool:
        return self.clutter \
            or self.surrounded_by_mess \
            or self.obstacle_share >= MESSY_OBSTACLE_SHARE \
            or self.obstacle_groups >= max(MESSY_OBSTACLE_GROUPS, MESSY_OBSTACLE_GROUPS_PER_M2 * self.area_m2)


def reject_mess(planes: Dict[int, RoofPlane],
                labels: np.ndarray,
                xyz: np.ndarray,
                resolution_metres: float,
                above_ground: Optional[np.ndarray] = None,
                debug: bool = False,
                rejected: Optional[List[dict]] = None) -> List[RoofPlane]:
    """
    The planes that are neither clutter nor messy flat roofs, or none if the building as
    a whole is messy. `labels` gives each pixel of `xyz` its plane id, or anything not a
    key of `planes` if it has none. The rest, and the obstacles, are recorded in
    `rejected` (see `rejections`).
    """
    mess, obstacles = plane_mess(planes, labels, xyz, resolution_metres, above_ground)

    # obstacles can be pixels of a plane (see `_residual_blobs`):
    messy = obstacles | np.isin(labels, [plane_id for plane_id, m in mess.items() if m.messy])
    total_pixels = np.count_nonzero(obstacles | np.isin(labels, list(mess.keys())))
    building_share = np.count_nonzero(messy) / total_pixels if total_pixels > 0 else 0

    if debug:
        for plane_id, m in mess.items():
            print(f"plane {plane_id}: {m}, obstacle share {m.obstacle_share:.2f}, messy {m.messy}")
        print(f"building mess share {building_share:.2f}")

    messy_building = building_share >= MESSY_BUILDING_SHARE
    kept = []
    for plane_id, m in mess.items():
        if m.messy or messy_building:
            if m.clutter:
                reason = rejections.CLUTTER
            elif m.surrounded_by_mess:
                reason = rejections.SURROUNDED_BY_MESS
            elif m.messy:
                reason = rejections.MESSY_FLAT_ROOF
            else:
                reason = rejections.MESSY_BUILDING
            rejections.record(rejected, planes[plane_id], reason,
                              **asdict(m), obstacle_share=m.obstacle_share, building_mess_share=building_share)
        else:
            kept.append(planes[plane_id])
    if rejected is not None and len(planes) > 0:
        building_id = next(iter(planes.values()))['building_id']
        rejections.record_obstacles(rejected, building_id, xyz[obstacles, :2])
    return kept


def plane_mess(planes: Dict[int, RoofPlane],
               labels: np.ndarray,
               xyz: np.ndarray,
               resolution_metres: float,
               above_ground: Optional[np.ndarray] = None):
    """
    Per plane, its `PlaneMess`; and a mask of the pixels (of `xyz`) that are obstacles on
    flat roofs. `above_ground` masks the pixels of `xyz` that are above ground, if known.
    """
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
        mess[plane_id] = PlaneMess(pixels=pixels, area_m2=pixels * resolution_metres ** 2, clutter=clutter)

    def height_above(plane: RoofPlane) -> np.ndarray:
        return z_img - (plane['x_coef'] * x_img + plane['y_coef'] * y_img + plane['intercept'])

    flat_roofs = [k for k, plane_id in enumerate(plane_ids)
                  if planes[plane_id]['is_flat'] and not mess[plane_id].clutter]

    # Clutter stands on the lowest flat roofs it touches (those about as low as the lowest).
    # If it's lower than another flat roof it touches, it's the step between the two instead.
    clutter_on = {k: [] for k in flat_roofs}
    for k, plane_id in enumerate(plane_ids):
        if not mess[plane_id].clutter:
            continue
        mask = label_img == k
        around = ndimage.binary_dilation(mask, _FOUR_CONNECTED) & ~mask
        heights = {j: np.median(height_above(planes[plane_ids[j]])[mask])
                   for j in flat_roofs if np.any(around & (label_img == j))}
        if heights and min(heights.values()) > 0:
            lowest = min(heights.values())
            for j, h in heights.items():
                if h - lowest <= OBSTACLE_MIN_HEIGHT_M:
                    clutter_on[j].append((plane_id, around))

    unassigned = present & (label_img < 0)
    excluded = at_edge.copy()
    if above_ground is not None:
        # the ground within a footprint (a yard, say) isn't a depression in the roof:
        excluded[rows, cols] |= ~above_ground
    all_obstacles = np.zeros(grid.shape, dtype=bool)
    for k in flat_roofs:
        plane_id = plane_ids[k]
        mask = label_img == k
        height = np.nan_to_num(height_above(planes[plane_id]))

        raised, n_raised, raised_excluded = _enclosed_groups(
            unassigned & (height > OBSTACLE_MIN_HEIGHT_M), mask, excluded)
        sunken, n_sunken, _ = _enclosed_groups(unassigned & (height < -OBSTACLE_MIN_HEIGHT_M), mask, excluded)
        blobs, n_blobs = _residual_blobs(height, mask, resolution_metres)
        obstacles = raised | sunken | blobs
        mess[plane_id].obstacle_pixels += int(np.count_nonzero(obstacles))
        mess[plane_id].obstacle_groups += n_raised + n_sunken + n_blobs
        all_obstacles |= obstacles

        for clutter_id, around in clutter_on[k]:
            # it's then at the foot of a taller part of the building, or against a parapet:
            if np.any(around & raised_excluded):
                continue
            mess[plane_id].obstacle_pixels += mess[clutter_id].pixels
            mess[plane_id].obstacle_groups += 1

    # Rejecting a plane can leave another surrounded by mess, so repeat until none are:
    small_pitched = [k for k, plane_id in enumerate(plane_ids)
                     if not planes[plane_id]['is_flat'] and not mess[plane_id].clutter
                     and mess[plane_id].area_m2 < CLUTTER_MAX_AREA_M2]
    changed = True
    while changed:
        changed = False
        # unassigned pixels at the edge of the building are mostly a mix of roof and ground:
        messy = all_obstacles | (unassigned & ~excluded) | np.isin(
            label_img, [k for k, plane_id in enumerate(plane_ids) if mess[plane_id].messy])
        for k in small_pitched:
            m = mess[plane_ids[k]]
            if m.messy:
                continue
            mask = label_img == k
            around = ndimage.binary_dilation(mask, _FOUR_CONNECTED) & ~mask & present
            if np.any(around) and np.count_nonzero(around & messy) / np.count_nonzero(around) >= SURROUNDED_BY_MESS_SHARE:
                m.surrounded_by_mess = True
                changed = True

    return mess, all_obstacles[rows, cols]


def _enclosed_groups(candidates: np.ndarray, mask: np.ndarray, excluded: np.ndarray):
    """
    The groups of `candidates` that are obstacles on the plane `mask`, and how many there
    are; and the groups that aren't because they touch `excluded`.

    Groups are judged whole, not just where they meet the plane: otherwise a taller part
    of the building would count as many small obstacles along its foot.
    """
    groups, n_groups = ndimage.label(candidates, _FOUR_CONNECTED)
    excluded_groups = np.unique(groups[candidates & excluded])
    is_obstacle = _border_share(groups, n_groups, mask) > OBSTACLE_MIN_BORDER_SHARE
    is_obstacle[excluded_groups] = False
    is_obstacle[0] = False
    return is_obstacle[groups], int(np.count_nonzero(is_obstacle)), \
        np.isin(groups, excluded_groups[excluded_groups > 0])


def _residual_blobs(height: np.ndarray, mask: np.ndarray, resolution_metres: float):
    """
    Groups of the pixels of the plane `mask` that stand above or sink below the rest of
    it by too little to be left out of it, but together by significantly more than noise;
    and how many there are.

    Residuals are against the local median, so falls to drains and sags aren't blobs.
    Pixels at the edge of the plane aren't used, as they are often mixed with whatever is
    next to it.
    """
    window = max(3, int(round(BLOB_WINDOW_M / resolution_metres)) | 1)
    blobs = np.zeros(mask.shape, dtype=bool)
    bbox = ndimage.find_objects(mask.astype(int))[0]
    crop_blobs = blobs[bbox]
    mask, height = mask[bbox], height[bbox]
    _, (nearest_rows, nearest_cols) = ndimage.distance_transform_edt(~mask, return_indices=True)
    local = height - ndimage.median_filter(height[nearest_rows, nearest_cols], size=window, mode='nearest')
    interior = ndimage.binary_erosion(mask, _FOUR_CONNECTED)

    n_blobs = 0
    for sign in (1, -1):
        candidates = interior & (sign * local > BLOB_MIN_RESIDUAL_M)
        groups, n_groups = ndimage.label(candidates, _FOUR_CONNECTED)
        if n_groups == 0:
            continue
        idxs = np.arange(1, n_groups + 1)
        size = ndimage.sum(candidates, groups, idxs)
        significance = ndimage.sum(sign * local, groups, idxs) / (BLOB_NOISE_M * np.sqrt(size))
        is_blob = np.concatenate([[False], (size * resolution_metres ** 2 >= BLOB_MIN_AREA_M2)
                                  & (significance > BLOB_MIN_SIGNIFICANCE)])
        crop_blobs |= is_blob[groups]
        n_blobs += int(np.count_nonzero(is_blob))
    return blobs, n_blobs


def _border_share(groups: np.ndarray, n_groups: int, mask: np.ndarray) -> np.ndarray:
    """Per group label (index 0 is no group), the share of its 4-neighbour border edges that are with `mask`."""
    border = np.zeros(n_groups + 1)
    with_mask = np.zeros(n_groups + 1)
    pairs = [((slice(None, -1), slice(None)), (slice(1, None), slice(None))),
             ((slice(None), slice(None, -1)), (slice(None), slice(1, None)))]
    for a, b in pairs + [(b, a) for a, b in pairs]:
        inside, outside = groups[a], groups[b]
        crossing = (inside > 0) & (inside != outside)
        border += np.bincount(inside[crossing], minlength=n_groups + 1)
        with_mask += np.bincount(inside[crossing & mask[b]], minlength=n_groups + 1)
    return np.divide(with_mask, border, out=np.zeros_like(border), where=border > 0)


def _plane_index(labels: np.ndarray, planes: Dict[int, RoofPlane]) -> np.ndarray:
    """`labels` as indexes into the list of `planes`' keys, or -1 for no plane."""
    index = np.full(labels.shape, -1)
    for k, plane_id in enumerate(planes.keys()):
        index[labels == plane_id] = k
    return index
