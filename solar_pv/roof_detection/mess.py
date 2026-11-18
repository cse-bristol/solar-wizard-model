# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
"""
Mess on roofs: clutter, and messy flat roofs.

* Clutter: a small pitched plane within a flat roof is not a roof, but an obstacle
  (plant, vents, rooflights) that a plane has been fitted to. Within means little of
  its surroundings are outside the building, and a good share of them are flat roof.
  Fit quality doesn't distinguish clutter: real small roof planes fit as poorly. It is
  rejected once the pixels have been assigned to planes (`reject_clutter`).
* Messy flat roof: the mess cost (`MessCost`) is part of the detection energy, so the
  messy parts of flat roofs are left unassigned, and a flat roof that's mostly messy isn't
  worth a plane.
* Obstacles on the flat roofs that are kept are cut out of their roof polygons
  (`obstacle_holes`).
"""
from typing import Dict, List, Optional, Tuple

import numpy as np
from scipy import ndimage

from solar_pv.datatypes import RoofPlane
from solar_pv.roof_detection import rejections
from solar_pv.roof_detection.assign_pixels import grid_index

CLUTTER_MAX_AREA_M2 = 40
CLUTTER_MAX_OUTSIDE_SHARE = 0.25
CLUTTER_MIN_FLAT_SHARE = 0.25

# Mess as part of the detection energy (see `MessCost`): a flat plane's pixels cost
# `MESS_WEIGHT` times the share of rough pixels within `MESS_WINDOW_M` of them over
# `MESS_FREE_SHARE` more. 0 turns it off.
MESS_WEIGHT = 8.0
# A pixel is rough if the best 3x3 window containing it is planar only to within this RMS
# (m) (see `local_fits`). Clear flat roofs at 1m are mostly within 0.01m.
MESS_ROUGH_RMS = 0.02
MESS_WINDOW_M = 5.0
# Some clear flat roofs are a little rough all over, and a lone obstacle makes only a
# small share of a window rough, so only the share over this counts.
MESS_FREE_SHARE = 0.3
# Pixels this near (m) the edge of the building's pixels above ground don't count: their
# local fits are often across a parapet, gutter or wall.
MESS_EDGE_M = 2.0
# The share is of at least this share of a window's pixels, so that a few rough pixels
# aren't taken for mess where there are few to count (on small roofs, and near the edge).
MESS_MIN_COUNTED = 0.5
# With the mess cost, cut each kept flat roof's obstacles out of its roof polygon (see
# `obstacle_holes`).
MESS_HOLES = True

_FOUR_CONNECTED = ndimage.generate_binary_structure(2, 1)
# local plane fits over 3x3 windows: the x and y of each cell from the window's centre
_WINDOW_X = np.array([[-1.0, 0, 1]] * 3)
_WINDOW_Y = _WINDOW_X.T


def reject_clutter(planes: Dict[int, RoofPlane],
                   labels: np.ndarray,
                   xyz: np.ndarray,
                   resolution_metres: float,
                   rejected: Optional[List[dict]] = None) -> List[RoofPlane]:
    """
    The planes that aren't clutter. `labels` gives each pixel of `xyz` its plane id, or
    anything not a key of `planes` if it has none. Clutter is recorded in `rejected` (see
    `rejections`).
    """
    is_clutter = clutter(planes, labels, xyz, resolution_metres)
    kept = []
    for plane_id, plane in planes.items():
        if is_clutter[plane_id]:
            rejections.record(rejected, plane, rejections.CLUTTER)
        else:
            kept.append(plane)
    return kept


def clutter(planes: Dict[int, RoofPlane],
            labels: np.ndarray,
            xyz: np.ndarray,
            resolution_metres: float) -> Dict[int, bool]:
    """Per plane, whether it is clutter."""
    grid, rows, cols = grid_index(xyz[:, :2], resolution_metres)
    # padded, so a plane's surroundings can include pixels outside the building:
    grid = np.pad(grid, 1, constant_values=-1)
    rows, cols = rows + 1, cols + 1
    present = grid >= 0
    label_img = np.full(grid.shape, -1)
    label_img[rows, cols] = _plane_index(labels, planes)

    plane_ids = list(planes.keys())
    flat = np.isin(label_img, [k for k, plane_id in enumerate(plane_ids) if planes[plane_id]['is_flat']])
    out = {}
    for k, plane_id in enumerate(plane_ids):
        out[plane_id] = False
        if planes[plane_id]['is_flat']:
            continue
        mask = label_img == k
        if np.count_nonzero(mask) * resolution_metres ** 2 >= CLUTTER_MAX_AREA_M2:
            continue
        around = ndimage.binary_dilation(mask, _FOUR_CONNECTED) & ~mask
        outside_share = np.count_nonzero(around & ~present) / np.count_nonzero(around)
        around_present = np.count_nonzero(around & present)
        flat_share = np.count_nonzero(around & flat) / around_present if around_present else 0
        out[plane_id] = outside_share < CLUTTER_MAX_OUTSIDE_SHARE and flat_share >= CLUTTER_MIN_FLAT_SHARE
    return out


def _plane_index(labels: np.ndarray, planes: Dict[int, RoofPlane]) -> np.ndarray:
    """`labels` as indexes into the list of `planes`' keys, or -1 for no plane."""
    index = np.full(labels.shape, -1)
    for k, plane_id in enumerate(planes.keys()):
        index[labels == plane_id] = k
    return index


class MessCost:
    """
    Mess on flat roofs as part of the detection energy: each pixel costs more on a flat
    plane the more of the pixels around it are rough - not locally planar: plant, pipes,
    vents, rooflights, and the planes fitted to them. So assigning pixels leaves the messy
    parts of a flat roof unassigned, and a roof that's mostly messy isn't worth a plane.

    Each pixel's roughness is that of the best 3x3 window containing it, so a pixel beside
    an obstacle isn't rough; and only the share of rough pixels over `MESS_FREE_SHARE`
    within `MESS_WINDOW_M` counts, so a lone obstacle costs nothing around it. Rough pixels
    are evidence of mess whether or not they're on the plane: a plane fitted between
    obstacles is a messy roof.
    """

    def __init__(self, xyz: np.ndarray, rms: np.ndarray, above_ground: np.ndarray, resolution_metres: float):
        """`rms` is each pixel's local fit RMS (see `local_fits`), inf if it has none."""
        grid, rows, cols = grid_index(xyz[:, :2], resolution_metres)
        image = np.zeros(grid.shape, dtype=bool)
        image[rows, cols] = above_ground
        inside = ndimage.distance_transform_edt(np.pad(image, 1))[1:-1, 1:-1] * resolution_metres > MESS_EDGE_M
        known = above_ground & np.isfinite(rms) & inside[rows, cols]
        rough, counted = np.zeros(grid.shape), np.zeros(grid.shape)
        rough[rows, cols] = known & (rms > MESS_ROUGH_RMS)
        counted[rows, cols] = known
        window = _disk(MESS_WINDOW_M / resolution_metres).astype(float)
        share = ndimage.convolve(rough, window, mode='constant') / np.maximum(
            ndimage.convolve(counted, window, mode='constant'), max(MESS_MIN_COUNTED * window.sum(), 1))
        self.pixel_cost = MESS_WEIGHT * np.maximum(share[rows, cols] - MESS_FREE_SHARE, 0)
        # the pixels the mess cost has taken off a flat plane that explained them
        self.left_out = np.zeros(len(xyz), dtype=bool)

    def cost(self, coef: Tuple[float, float, float], idxs: np.ndarray) -> np.ndarray:
        """Per pixel, its extra cost on a flat plane (`plane_moves.MessCostFn`): the same on any."""
        return self.pixel_cost


def obstacle_holes(xyz: np.ndarray, above_ground: np.ndarray, idxs: np.ndarray, coef: Tuple[float, float, float],
                   resolution_metres: float) -> np.ndarray:
    """
    Which pixels of `xyz` are obstacles on the flat plane `coef` whose pixels are `idxs`, to
    be cut out of its roof polygon: its rough pixels, and the pixels it encloses (plant,
    rooflights, clutter, and the messy parts the mess cost left out).

    Roughness is of the plane's pixels less the plane, with the plane extended over every
    other pixel, so the walls, parapets and planes around it don't make it rough.
    """
    out = np.zeros(len(xyz), dtype=bool)
    if len(idxs) == 0:
        return out
    grid, rows, cols = grid_index(xyz[:, :2], resolution_metres)
    # enough around the plane to tell the building's edge from the edge of the crop:
    pad = int(np.ceil(MESS_EDGE_M / resolution_metres)) + 2
    r, c = rows[idxs], cols[idxs]
    r0, c0 = max(r.min() - pad, 0), max(c.min() - pad, 0)
    crop = (slice(r0, r.max() + pad + 1), slice(c0, c.max() + pad + 1))
    above = np.zeros(grid.shape, dtype=bool)
    above[rows, cols] = above_ground
    grid, above = np.pad(grid[crop], pad, constant_values=-1), np.pad(above[crop], pad)
    on = np.zeros(grid.shape, dtype=bool)
    on[r - r0 + pad, c - c0 + pad] = True

    a, b, intercept = coef
    residual = np.zeros(grid.shape)
    pixels = xyz[grid[on]]
    residual[on] = pixels[:, 2] - (a * pixels[:, 0] + b * pixels[:, 1] + intercept)

    # as for the mess cost, a pixel near the building's edge isn't rough:
    inside = ndimage.distance_transform_edt(above) * resolution_metres > MESS_EDGE_M
    rough = on & inside & (_best_window_rms(residual) > MESS_ROUGH_RMS)
    enclosed = ndimage.binary_fill_holes(on) & ~on & above
    out[grid[rough | enclosed]] = True
    return out


def _best_window_rms(z: np.ndarray) -> np.ndarray:
    """Per cell of `z`, the least RMS residual of a plane fitted to a 3x3 window containing it."""
    ones = np.ones((3, 3))
    sum_z, sum_z2 = ndimage.correlate(z, ones, mode='constant'), ndimage.correlate(z * z, ones, mode='constant')
    sum_xz, sum_yz = ndimage.correlate(z, _WINDOW_X, mode='constant'), ndimage.correlate(z, _WINDOW_Y, mode='constant')
    # the window's x and y are orthogonal to each other and to the constant, and sum to 6 squared
    sse = sum_z2 - sum_z * sum_z / 9 - sum_xz * sum_xz / 6 - sum_yz * sum_yz / 6
    return ndimage.minimum_filter(np.sqrt(np.maximum(sse, 0) / 9), size=3, mode='constant', cval=np.inf)


def _disk(radius: float) -> np.ndarray:
    k = int(radius)
    dy, dx = np.mgrid[-k:k + 1, -k:k + 1]
    return dy * dy + dx * dx <= radius * radius
