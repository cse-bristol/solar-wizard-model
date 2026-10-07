import json

# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import logging
import os
from contextlib import contextmanager
from os.path import join

import time
import math
import traceback
import zlib
from typing import List, Dict, Tuple, Optional
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, as_completed

from psycopg2.extras import DictCursor, execute_values, Json
from psycopg2.sql import SQL, Identifier
import numpy as np
import shapely
from shapely import STRtree
from scipy import ndimage
from shapely import wkt

from solar_pv.geos import deg_diff, slope_deg, aspect_deg, azimuth_deg
from solar_pv.db_funcs import count, sql_command, connect, connection
from solar_pv.postgis import pixels_for_buildings
from solar_pv import tables, stage
from solar_pv.constants import RANSAC_LARGE_BUILDING, \
    RANSAC_LARGE_MAX_TRIALS, RANSAC_SMALL_MAX_TRIALS, RANSAC_SMALL_BUILDING, \
    RANSAC_MEDIUM_MAX_TRIALS, ROOFDET_MAX_MAE, ROOFDET_MAX_CPUS, ROOFDET_MAX_AREA, \
    AZIMUTH_ALIGNMENT_THRESHOLD, FLAT_ROOF_AZIMUTH_ALIGNMENT_THRESHOLD, FLAT_ROOF_DEGREES_THRESHOLD
from solar_pv.roof_detection import rejections
from solar_pv.roof_detection.detect_messy_roofs import detect_messy_roofs
from solar_pv.roof_detection.mess import reject_mess
from solar_pv.roof_detection.detsac import DETSACRegressorForLIDAR
from solar_pv.roof_detection.assign_pixels import assign_pixels, grid_index, NONE
from solar_pv.roof_detection.merge_adjacent import merge_adjacent, set_plane_fit
from solar_pv.roof_detection.premade_planes import create_planes
from solar_pv.roof_detection.ransac import RANSACRegressorForLIDAR, _FaceIndex, closest_azimuth, \
    _min_thinness_ratio
from solar_pv.roof_detection.plane_fit import PlaneFit
from solar_pv.roof_detection.plane_skew import plane_skew
from solar_pv.roof_detection.plane_moves import merge_by_energy, remove_unprofitable, data_cost
from solar_pv.roof_detection.segment_planes import segment_planes, local_fits
from solar_pv.roof_polygons.roof_polygons import create_roof_polygons, _pixel_region
from solar_pv.datatypes import RoofDetBuilding, RoofPlane, RoofPolygon
from solar_pv.util import get_cpu_count

_SEMAPHORE_TIMEOUT_S = 120
_SEMAPHORE_MAX_CONN = 50
_PG_CONN_SEMAPHORE = mp.Semaphore(_SEMAPHORE_MAX_CONN)


# Max distance (m) of a pixel from a plane for it to count as an inlier. Also the unit of
# elevation residual in `assign_pixels`, per plane.
_RESIDUAL_THRESHOLD = 0.25
_FLAT_ROOF_RESIDUAL_THRESHOLD = 0.1
# Max rounds of refitting planes to their pixels and reassigning pixels to the planes.
_MAX_REFINE_ROUNDS = 10
# Refining stops once a round lowers the energy by less than this share.
_MIN_REFINE_ENERGY_FALL = 0.01
# RANSAC's defaults for its shape and aspect checks:
_MIN_CONVEX_HULL_RATIO = 0.65
_MAX_ASPECT_CIRCULAR_MEAN_DEGREES = 90
_MAX_ASPECT_CIRCULAR_SD = 1.5
# A pixel is locally planar if the best 3x3 window containing it fits a plane this well (m):
_MAX_LOCAL_RMS = 0.1
_MIN_SMALL_REGION_PLANAR_SHARE = 0.5
# Regions at least this big that fit their plane closely skip the aspect checks (see `_implausible`):
_MIN_UNGATED_ASPECT_PIXELS = 40
# As `segment_planes`' default:
_MAX_NORMAL_ANGLE_DEGREES = 12
# The energy (in `assign_pixels`' units) a plane must save to be worth having.
_LABEL_COST = 8.0
# Planes facing the same way are merged if the merged plane's energy per pixel is at most
# this - that is, it explains their pixels to within about its residual scale - even if
# merging raises the energy: at 1m, a slightly warped roof face fits two planes better.
_GOOD_ENOUGH_MERGE_COST = 1.0
# Regions re-proposed from poorly explained pixels can be smaller than the first ones, as
# the energy decides whether they're worth keeping (see `plane_moves`).
_MIN_REPROPOSED_PIXELS = 4
# Rounds of refitting a plane to its pixels within its residual scale, and the share of its
# pixels that must be (else its least-squares fit is kept):
_TRIM_ROUNDS = 3
_MIN_TRIM_SHARE = 0.5
# Max rounds of proposing new candidate planes from the pixels no plane explains well.
_MAX_PROPOSAL_ROUNDS = 2


def _roof_det_cpu_count():
    """Use 3/4s of available CPUs for roof plane detection"""
    return min(int(get_cpu_count() * 0.75), ROOFDET_MAX_CPUS)


def detect_roofs(pg_uri: str,
                 job_id: int,
                 max_roof_slope_degrees: int,
                 min_roof_area_m: int,
                 min_roof_degrees_from_north: int,
                 flat_roof_degrees: int,
                 min_dist_to_edge_m: float,
                 resolution_metres: float,
                 roof_polygon_method: str = "legacy",
                 roof_detection_method: str = "legacy",
                 workers: int = _roof_det_cpu_count()) -> None:

    if stage.get_stage(pg_uri, job_id) >= stage.Stage.DETECT_ROOFS:
        logging.info("Not detecting roof planes, already detected.")
        return
    _clear(pg_uri, job_id)

    buildings_with_areas = _buildings_with_areas(pg_uri, job_id)
    building_count = len(buildings_with_areas)
    
    logging.info(f"{building_count} buildings to process")
    logging.info(f"Using {workers} processes for roof plane detection")
    start_time = time.time()

    params = {
        "max_roof_slope_degrees": max_roof_slope_degrees,
        "min_roof_area_m": min_roof_area_m,
        "min_roof_degrees_from_north": min_roof_degrees_from_north,
        "flat_roof_degrees": flat_roof_degrees,
        "min_dist_to_edge_m": min_dist_to_edge_m,
        "resolution_metres": resolution_metres,
        "method": roof_polygon_method,
    }

    work_queue = _create_adaptive_batches(buildings_with_areas)

    executor = ProcessPoolExecutor(max_workers=workers)
    try:
        futures = []
        for batch_building_ids in work_queue:
            futures.append(executor.submit(_handle_building_batch,
                                           pg_uri, job_id, batch_building_ids, params,
                                           roof_detection_method))

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                executor.shutdown(cancel_futures=True)
                raise e
    finally:
        executor.shutdown()

    _mark_buildings_with_no_planes(pg_uri, job_id)
    logging.info(f"roof plane detection for {building_count} roofs took {round(time.time() - start_time, 2)} s.")
    stage.set_stage(pg_uri, job_id, stage.Stage.DETECT_ROOFS)


def _handle_building_batch(pg_uri: str, job_id: int, building_ids: List[str], params: dict,
                           roof_detection_method: str = "legacy"):
    start_time = time.time()
    buildings = _load(pg_uri, job_id, building_ids)

    polygons = []
    rejected = []
    for building_id, building in buildings.items():
        try:
            t0 = time.time()
            # seeded per building so that runs are reproducible, and runs with different
            # methods start from the same RANSAC planes:
            random_state = np.random.RandomState(zlib.crc32(str(building_id).encode()))
            found = _detect_building_roof_planes(building, building_id, params['resolution_metres'],
                                                 method=roof_detection_method, random_state=random_state,
                                                 rejected=rejected)
            t1 = time.time()
            if t1 - t0 > 7200:
                print(f"very slow plane detection: {building_id} took {round(t1 - t0, 2)} s")
                _write_test_data(job_id, building)
        except Exception as e:
            print(f"Exception during roof plane detection for building_id {building_id}:")
            traceback.print_exception(e)
            _write_test_data(job_id, building)
            raise e

        if len(found) > 0:
            building_polygons = create_roof_polygons(building_id, building['polygon'], found, **params,
                                                     rejected=rejected, pixels_xyz=_pixels_xyz(building['pixels']))
            _set_skew(building_polygons, building['pixels'], params['resolution_metres'])
            polygons.extend(building_polygons)

    try:
        _save_planes(pg_uri, job_id, polygons)
        _save_rejected(pg_uri, job_id, rejected, params['resolution_metres'])
    except Exception as e:
        print(f"Exception when saving roof planes:")
        traceback.print_exception(e)
        raise e

    batch_time = round(time.time() - start_time, 2)
    print(f"batch of {len(building_ids)} buildings took {batch_time} s.")


def _detect_building_roof_planes(building: RoofDetBuilding,
                                 building_id: str,
                                 resolution_metres: float,
                                 debug: bool = False,
                                 method: str = "legacy",
                                 random_state: np.random.RandomState = None,
                                 rejected: Optional[List[dict]] = None) -> List[RoofPlane]:
    """
    Find roof planes with DETSAC then RANSAC, then tidy up which pixels belong to which
    plane. `method` "legacy" merges leftover pixels and planes greedily
    (`merge_adjacent`); "mincut" assigns pixels to planes by multi-label min-cut on
    their elevation residuals (`assign_pixels`), with `merge_adjacent` only merging planes.
    "segment" replaces DETSAC and RANSAC with region growing on local plane fits
    (`segment_planes`), then assigns pixels as "mincut" does (see `_segment_and_assign`).

    `random_state` is for RANSAC; if None, numpy's global random state is used.
    Planes thrown away are recorded in `rejected` (see `rejections`).
    """
    pixels_in_building = building['pixels']
    polygon = building['polygon']
    max_ground_height = building['max_ground_height']

    xyz = np.array([[pixel["x"], pixel["y"], pixel["elevation"]] for pixel in pixels_in_building])
    aspect = np.array([pixel["aspect"] for pixel in pixels_in_building])
    slope = np.array([pixel["slope"] for pixel in pixels_in_building])
    z = xyz[:, 2]
    mask = z > max_ground_height if max_ground_height else np.ones(aspect.shape)

    min_points_per_plane = min(8, int(8 / resolution_metres))  # 8 for 2m, 8 for 1m, 16 for 0.5m
    total_points_in_building = len(aspect)

    if method == "segment":
        above_ground = mask.astype(bool)
        # as DETSAC and RANSAC's `min_points_per_plane_perc`:
        small_pixels = int(total_points_in_building * 0.008)
        return _segment_and_assign(xyz, aspect, polygon, above_ground, building_id, resolution_metres,
                                   min_points_per_plane, small_pixels, debug=debug, rejected=rejected)
    premade_planes = create_planes(xyz, aspect, slope, resolution_metres)
    skip_planes = set()
    xy = xyz[:, :2]
    z = xyz[:, 2]

    labels_nodata = -1
    labels = np.full(z.shape, labels_nodata, dtype=int)
    planes: Dict[int, RoofPlane] = {}

    plane_idx = 0
    while np.count_nonzero(mask) > min_points_per_plane:
        detsac = DETSACRegressorForLIDAR(residual_threshold=_RESIDUAL_THRESHOLD,
                                         flat_roof_residual_threshold=_FLAT_ROOF_RESIDUAL_THRESHOLD,
                                         min_points_per_plane=min_points_per_plane,
                                         resolution_metres=resolution_metres)
        detsac.fit(xy, z,
                   aspect=aspect,
                   mask=mask,
                   polygon=polygon,
                   premade_planes=premade_planes,
                   skip_planes=skip_planes,
                   total_points_in_building=total_points_in_building,
                   debug=debug)

        if detsac.finished:
            break

        if detsac.success:
            inlier_mask = detsac.inlier_mask_

            # don't keep bad planes - their inliers become candidates for being
            # merged into other planes (and the planes will still have been added to
            # skip_planes, so we don't retry them)
            if detsac.plane_properties["score"] < ROOFDET_MAX_MAE:
                planes[plane_idx] = detsac.plane_properties
                planes[plane_idx]["building_id"] = building_id
                labels[inlier_mask] = plane_idx
                plane_idx += 1
                mask[inlier_mask] = 0
            else:
                rejections.record(rejected, {**detsac.plane_properties, "building_id": building_id},
                                  rejections.HIGH_MAE)

    total_pixels = len(aspect)
    if total_pixels > RANSAC_LARGE_BUILDING / resolution_metres:
        max_trials = RANSAC_LARGE_MAX_TRIALS
    elif total_pixels < RANSAC_SMALL_BUILDING / resolution_metres:
        max_trials = RANSAC_SMALL_MAX_TRIALS
    else:
        max_trials = RANSAC_MEDIUM_MAX_TRIALS

    # We only fall back to RANSAC if a decent fraction of pixels are left to find:
    pixels_required_for_ransac = max(min_points_per_plane * 5, total_points_in_building // 4)

    while np.count_nonzero(mask) > pixels_required_for_ransac:
        ransac = RANSACRegressorForLIDAR(residual_threshold=_RESIDUAL_THRESHOLD,
                                         flat_roof_residual_threshold=_FLAT_ROOF_RESIDUAL_THRESHOLD,
                                         max_trials=max_trials,
                                         random_state=random_state,
                                         max_slope=75,
                                         min_slope=0,
                                         min_points_per_plane=min_points_per_plane,
                                         resolution_metres=resolution_metres)
        ransac.fit(xy, z,
                   aspect=aspect,
                   mask=mask,
                   polygon=polygon,
                   skip_planes=skip_planes,
                   total_points_in_building=total_points_in_building,
                   debug=debug)

        if ransac.finished:
            break

        if ransac.success:
            inlier_mask = ransac.inlier_mask_

            # don't keep bad planes - their inliers become candidates for being
            # merged into other planes (and the planes will still have been added to
            # skip_planes, so we don't retry them)
            if ransac.plane_properties["score"] < ROOFDET_MAX_MAE:
                planes[plane_idx] = ransac.plane_properties
                planes[plane_idx]["building_id"] = building_id
                labels[inlier_mask] = plane_idx
                plane_idx += 1
                mask[inlier_mask] = 0
            else:
                rejections.record(rejected, {**ransac.plane_properties, "building_id": building_id},
                                  rejections.HIGH_MAE)

    if len(planes) == 0:
        return []

    if debug:
        print("Finished detecting roofs")

    if method == "mincut":
        above_ground = z > max_ground_height if max_ground_height else np.ones(z.shape, dtype=bool)
        return _assign_and_merge(
            xyz, aspect, labels, planes, above_ground, resolution_metres, labels_nodata, debug=debug,
            rejected=rejected)
    elif method != "legacy":
        raise ValueError(f"Unknown roof detection method {method}")

    # label all the outlying pixels with individual IDs:
    outliers = np.count_nonzero(labels[mask == 1])
    labels[mask == 1] = range(plane_idx + 1, outliers + plane_idx + 1)

    merged_planes, new_labels = merge_adjacent(xy, z, aspect, labels, planes, resolution_metres, labels_nodata, debug=debug)

    if debug:
        print("Merged planes and outliers")

    non_messy_planes = detect_messy_roofs(merged_planes, new_labels, xy, resolution_metres, debug=debug,
                                          rejected=rejected)

    if debug:
        print("Finished detecting mess")

    return non_messy_planes


def _assign_and_merge(xyz: np.ndarray,
                      aspect: np.ndarray,
                      labels: np.ndarray,
                      planes: Dict[int, RoofPlane],
                      above_ground: np.ndarray,
                      resolution_metres: float,
                      labels_nodata: int,
                      debug: bool = False,
                      rejected: Optional[List[dict]] = None) -> List[RoofPlane]:
    """
    Assign pixels to planes, merge planes, assign again with the merged planes, then
    reject clutter and mess. Each time pixels are assigned, planes are refitted to (and
    their metrics recomputed from) them. The final orientations come from just the pixels
    near each plane (see `_trim_plane_coefficients`).

    Assigning first means planes separated only by unassigned pixels are adjacent when
    `merge_adjacent` looks for planes to merge.
    """
    xy, z = xyz[:, :2], xyz[:, 2]
    labels = _assign(xyz, aspect, labels, planes, above_ground, resolution_metres, labels_nodata, rejected)
    if debug:
        print(f"Assigned pixels to {len(planes)} planes")
    # merge_adjacent fails if no pixel has a label
    if len(planes) == 0:
        return []

    planes, labels = merge_adjacent(xy, z, aspect, labels, planes, resolution_metres, labels_nodata, debug=debug)
    if debug:
        print(f"Merged, now have {len(planes)} planes")

    labels = _assign(xyz, aspect, labels, planes, above_ground, resolution_metres, labels_nodata, rejected)
    if debug:
        print(f"Reassigned pixels to {len(planes)} planes")
    for plane_id, plane in planes.items():
        _trim_plane_coefficients(plane, xyz[labels == plane_id])

    return reject_mess(planes, labels, xyz, resolution_metres, above_ground=above_ground, debug=debug,
                       rejected=rejected)


def _assign(xyz: np.ndarray,
            aspect: np.ndarray,
            labels: np.ndarray,
            planes: Dict[int, RoofPlane],
            above_ground: np.ndarray,
            resolution_metres: float,
            labels_nodata: int,
            rejected: Optional[List[dict]] = None,
            min_pixels: int = 3,
            largest_component: bool = False,
            return_energy: bool = False,
            full_fit: bool = True):
    """
    Reassign `labels` with `assign_pixels`, and refit `planes` to them (in place). Planes
    left with fewer than `min_pixels` are removed, and recorded in `rejected`. If
    `largest_component`, planes keep only their largest 4-connected group of pixels. If
    `return_energy`, also returns `assign_pixels`' energy for the starting labels. If not
    `full_fit`, only the planes' coefficients and what derives from them are refitted
    (see `_set_plane_coefficients`).
    """
    plane_ids = list(planes.keys())
    idx_labels = np.full(np.count_nonzero(above_ground), NONE)
    for k, plane_id in enumerate(plane_ids):
        idx_labels[labels[above_ground] == plane_id] = k

    residual_scales = [_FLAT_ROOF_RESIDUAL_THRESHOLD if planes[plane_id]['is_flat'] else _RESIDUAL_THRESHOLD
                       for plane_id in plane_ids]
    idx_labels, energy = assign_pixels(xyz[above_ground], idx_labels, len(plane_ids), resolution_metres,
                                       residual_scales=residual_scales, return_energy=True)

    new_labels = np.full(labels.shape, labels_nodata)
    candidate_idxs = np.flatnonzero(above_ground)
    if largest_component:
        grid, rows, cols = grid_index(xyz[above_ground, :2], resolution_metres)
    for k, plane_id in enumerate(plane_ids):
        in_plane = idx_labels == k
        if largest_component and in_plane.any():
            image = np.zeros(grid.shape, dtype=bool)
            image[rows[in_plane], cols[in_plane]] = True
            groups, _ = ndimage.label(image)
            sizes = np.bincount(groups[rows[in_plane], cols[in_plane]])
            in_plane &= groups[rows, cols] == np.argmax(sizes)
        idxs = candidate_idxs[in_plane]
        if len(idxs) < max(min_pixels, 3):
            rejections.record(rejected, planes.pop(plane_id), rejections.TOO_FEW_PIXELS, pixels=len(idxs))
            continue
        new_labels[idxs] = plane_id
        planes[plane_id]['res'] = resolution_metres
        if full_fit:
            set_plane_fit(planes[plane_id], xyz[idxs, :2], xyz[idxs, 2], aspect[idxs], resolution_metres)
        else:
            _set_plane_coefficients(planes[plane_id], xyz[idxs, :2], xyz[idxs, 2])
    if return_energy:
        return new_labels, energy
    return new_labels


def _segment_and_assign(xyz: np.ndarray,
                        aspect: np.ndarray,
                        polygon,
                        above_ground: np.ndarray,
                        building_id: str,
                        resolution_metres: float,
                        min_pixels: int,
                        small_pixels: int,
                        debug: bool = False,
                        rejected: Optional[List[dict]] = None) -> List[RoofPlane]:
    """
    Find roof planes as PEaRL does (Isack & Boykov 2012): propose candidate planes, then
    alternately refit the planes to their pixels and reassign pixels to the planes
    (`_refine`), merging and removing planes when that lowers the energy of the assignment
    plus `_LABEL_COST` per plane (`plane_moves`), and proposing new candidates from the
    pixels no plane explains well - then reject clutter and mess.

    Candidates are the regions of `segment_planes` that RANSAC would accept as candidate
    planes (see `_propose`). The regions over-segment: a region only has to be good enough
    to start from, as refining lets a plane grow into pixels its region missed (by up to
    `assign_pixels`' `max_dist_m` a round), and merging joins regions split by noise.
    """
    labels_nodata = -1
    labels = np.full(len(xyz), labels_nodata)
    if np.count_nonzero(above_ground) < min_pixels:
        return []
    normals = np.zeros((len(xyz), 3))
    rms = np.full(len(xyz), np.inf)
    normals[above_ground], rms[above_ground] = local_fits(xyz[above_ground], resolution_metres)
    locally_planar = rms <= _MAX_LOCAL_RMS
    face_index = _FaceIndex.of(polygon)
    planes: Dict[int, RoofPlane] = {}

    def propose(candidates: np.ndarray, first_id: int, min_region_pixels: int) -> int:
        return _propose(xyz, aspect, candidates, labels, planes, (normals, rms), face_index, building_id,
                        resolution_metres, min_region_pixels, small_pixels, first_id, rejected)

    # planes may shrink below `min_pixels` while refining: whether they're worth keeping is
    # for `_merge_and_remove` to judge
    def refine():
        return _refine(xyz, aspect, labels, planes, above_ground, resolution_metres, labels_nodata,
                       3, face_index, debug=debug, rejected=rejected)

    next_id = propose(above_ground, 0, min_pixels)
    if debug:
        print(f"Segmented into {len(planes)} plausible regions")
    for round_ in range(_MAX_PROPOSAL_ROUNDS + 1):
        if len(planes) == 0:
            return []
        labels = refine()
        if _merge_and_remove(xyz, labels, planes, above_ground, resolution_metres, labels_nodata, rejected, debug):
            labels = refine()
        if round_ == _MAX_PROPOSAL_ROUNDS:
            break
        poorly_explained = above_ground & ~_explained(xyz, labels, planes, normals, locally_planar)
        before = next_id
        next_id = propose(poorly_explained, next_id, _MIN_REPROPOSED_PIXELS)
        if debug:
            print(f"Proposal round {round_}: {next_id - before} new candidates")
        if next_id == before:
            break
    if len(planes) == 0:
        return []

    return reject_mess(planes, labels, xyz, resolution_metres, above_ground=above_ground, debug=debug,
                       rejected=rejected)


def _propose(xyz: np.ndarray,
             aspect: np.ndarray,
             candidates: np.ndarray,
             labels: np.ndarray,
             planes: Dict[int, RoofPlane],
             local: Tuple[np.ndarray, np.ndarray],
             face_index: _FaceIndex,
             building_id: str,
             resolution_metres: float,
             min_pixels: int,
             small_pixels: int,
             first_id: int,
             rejected: Optional[List[dict]] = None) -> int:
    """
    Add (in place) the regions of `segment_planes` among the `candidates` pixels that
    RANSAC would accept as candidate planes to `planes`, taking their pixels in `labels`,
    with ids from `first_id`. Returns the next unused id. `local` is each pixel's local
    fit (see `local_fits`).

    Regions need `min_pixels`. On rough surfaces (vegetation, clutter) region growing finds
    many small regions, which fit their plane no worse than real small planes do, as
    regions only take pixels close to their plane - but few of their pixels are locally
    planar. So regions of fewer than `small_pixels` must be mostly locally planar.
    """
    candidate_idxs = np.flatnonzero(candidates)
    if len(candidate_idxs) < min_pixels:
        return first_id
    normals, rms = local
    locally_planar = rms <= _MAX_LOCAL_RMS
    regions = segment_planes(xyz[candidate_idxs], resolution_metres, min_pixels=min_pixels,
                             local=(normals[candidate_idxs], rms[candidate_idxs]))
    plane_id = first_id
    for region in np.unique(regions[regions >= 0]):
        idxs = candidate_idxs[regions == region]
        plane = {"building_id": building_id, "plane_type": "segmented", "plane_id": f"segmented_{plane_id}",
                 "res": resolution_metres}
        set_plane_fit(plane, xyz[idxs, :2], xyz[idxs, 2], aspect[idxs], resolution_metres)
        reason = _implausible(plane, face_index)
        if not reason and len(idxs) < small_pixels and locally_planar[idxs].mean() < _MIN_SMALL_REGION_PLANAR_SHARE:
            reason = rejections.SMALL_POOR_FIT
        if reason:
            rejections.record(rejected, plane, reason)
            continue
        planes[plane_id] = plane
        labels[idxs] = plane_id
        plane_id += 1
    return plane_id


def _explained(xyz: np.ndarray, labels: np.ndarray, planes: Dict[int, RoofPlane], normals: np.ndarray,
               locally_planar: np.ndarray) -> np.ndarray:
    """
    Which pixels are within the residual scale of the plane they're assigned to, and if
    locally planar, face the same way as it (as `segment_planes` requires of a region's
    pixels): a small plane a plane has absorbed, like the side of a dormer, can be close
    enough to it to be within its residual scale.
    """
    explained = np.zeros(len(xyz), dtype=bool)
    cos_max = np.cos(np.radians(_MAX_NORMAL_ANGLE_DEGREES))
    for plane_id, plane in planes.items():
        idxs = np.flatnonzero(labels == plane_id)
        a, b, c = plane['x_coef'], plane['y_coef'], plane['intercept']
        cost = data_cost(xyz[idxs], (a, b, c), _residual_scale(plane))
        unit = np.array([-a, -b, 1.0]) / np.sqrt(a * a + b * b + 1)
        facing = ~locally_planar[idxs] | (normals[idxs] @ unit >= cos_max)
        explained[idxs[(cost <= 1) & facing]] = True
    return explained


def _mergeable(p: RoofPlane, q: RoofPlane) -> bool:
    """
    Whether two planes could be the same roof face: both flat, or facing the same way.
    Energy alone doesn't separate a roof face split by noise from, say, a dormer's side
    and the roof it's on, at 1m: small planes' energies differ by little either way.
    """
    if p['is_flat'] or q['is_flat']:
        return p['is_flat'] and q['is_flat']
    return deg_diff(p['aspect_raw'], q['aspect_raw']) <= AZIMUTH_ALIGNMENT_THRESHOLD


def _merge_and_remove(xyz: np.ndarray,
                      labels: np.ndarray,
                      planes: Dict[int, RoofPlane],
                      above_ground: np.ndarray,
                      resolution_metres: float,
                      labels_nodata: int,
                      rejected: Optional[List[dict]] = None,
                      debug: bool = False) -> bool:
    """Merge, then remove, planes (in place) where that lowers the energy including
    `_LABEL_COST` per plane. Returns whether anything changed."""
    scale = lambda is_flat: _FLAT_ROOF_RESIDUAL_THRESHOLD if is_flat else _RESIDUAL_THRESHOLD
    ground = ~above_ground
    labels[ground] = labels_nodata
    merges = merge_by_energy(xyz, labels, planes, resolution_metres, _LABEL_COST, scale, _mergeable,
                             good_enough_cost=_GOOD_ENOUGH_MERGE_COST)
    kept = {plane_id: dict(plane) for plane_id, plane in planes.items()}
    removed = remove_unprofitable(xyz, labels, planes, resolution_metres, _LABEL_COST, scale, labels_nodata)
    for plane_id in removed:
        rejections.record(rejected, kept[plane_id], rejections.UNPROFITABLE)
    if debug:
        print(f"Merged {len(merges)} pairs of planes, removed {len(removed)}")
    return bool(merges or removed)


def _implausible(plane: RoofPlane, face_index: _FaceIndex) -> Optional[str]:
    """
    Why RANSAC wouldn't accept `plane` as a candidate - not aligned with the building,
    a poor fit, a poor shape, or (if pitched) its pixels' aspects disagree with it - or
    None if it would. Sets the plane's `aspect`.

    The shape checks only apply to regions that fit their plane poorly: a region that
    fits closely is planar whatever its shape, and clean flat roofs are often concave
    (wrapping round a raised part of the building) or long and thin. Nor do the aspect
    checks apply to large regions that fit closely: the pixels' aspects come from 3x3
    windows, which on faces only a few pixels wide (sawtooth and multi-ridge roofs) mostly
    straddle a ridge or valley.
    """
    plane['aspect'] = _snapped_aspect(plane, face_index)
    if plane['aspect'] is None:
        return rejections.NO_CLOSE_ASPECT
    if plane['score'] >= ROOFDET_MAX_MAE:
        return rejections.HIGH_MAE
    good_fit = plane['mae'] <= _residual_scale(plane) / 2
    if not good_fit:
        if plane['cv_hull_ratio'] < _MIN_CONVEX_HULL_RATIO:
            return rejections.CONVEX_HULL_RATIO
        if plane['thinness_ratio'] < _min_thinness_ratio(len(plane['inliers_xy'])):
            return rejections.THINNESS_RATIO
    if not plane['is_flat'] and not (good_fit and len(plane['inliers_xy']) >= _MIN_UNGATED_ASPECT_PIXELS):
        if deg_diff(plane['aspect_circ_mean'], plane['aspect_raw']) > _MAX_ASPECT_CIRCULAR_MEAN_DEGREES:
            return rejections.CIRCULAR_MEAN
        if plane['aspect_circ_sd'] > _MAX_ASPECT_CIRCULAR_SD:
            return rejections.CIRCULAR_SD
    return None


def _residual_scale(plane: RoofPlane) -> float:
    return _FLAT_ROOF_RESIDUAL_THRESHOLD if plane['is_flat'] else _RESIDUAL_THRESHOLD


def _refine(xyz: np.ndarray,
            aspect: np.ndarray,
            labels: np.ndarray,
            planes: Dict[int, RoofPlane],
            above_ground: np.ndarray,
            resolution_metres: float,
            labels_nodata: int,
            min_pixels: int,
            face_index: _FaceIndex,
            debug: bool = False,
            rejected: Optional[List[dict]] = None) -> np.ndarray:
    """
    Reassign pixels and refit `planes` (in place) until the labels stop changing or the
    energy stops falling, then re-snap each plane's aspect to the building's faces
    (keeping its old one if no face is close now).

    Refitting is by least squares, not the energy `assign_pixels` minimises, so it can
    raise the energy and the rounds can cycle rather than converge.
    """
    last_energy = np.inf
    for round_ in range(_MAX_REFINE_ROUNDS):
        new_labels, energy = _assign(xyz, aspect, labels, planes, above_ground, resolution_metres, labels_nodata,
                                     rejected, min_pixels=min_pixels, largest_component=True, return_energy=True,
                                     full_fit=False)
        changed = np.count_nonzero(new_labels != labels)
        labels = new_labels
        if debug:
            print(f"Refine round {round_}: {len(planes)} planes, {changed} pixels changed, energy {energy:.1f}")
        if changed == 0 or len(planes) == 0 or energy > last_energy * (1 - _MIN_REFINE_ENERGY_FALL):
            break
        last_energy = energy
    for plane_id, plane in planes.items():
        idxs = np.flatnonzero(labels == plane_id)
        set_plane_fit(plane, xyz[idxs, :2], xyz[idxs, 2], aspect[idxs], resolution_metres)
        _trim_plane_coefficients(plane, xyz[idxs])
        snapped = _snapped_aspect(plane, face_index)
        if snapped is not None:
            plane['aspect'] = snapped
    return labels


def _trim_plane_coefficients(plane: dict, xyz: np.ndarray) -> None:
    """
    Refit the plane's coefficients (and slope, aspect and flatness) to just its pixels
    within its residual scale of it. Pixels up to about twice that from a plane are
    assigned to it, and at 1m those are mostly mixed ridge, eave or gutter pixels, which
    tilt a least-squares fit on a plane only a few pixels deep. The fit metrics are left as
    they are, over all its pixels.
    """
    xy, z = xyz[:, :2], xyz[:, 2]
    for _ in range(_TRIM_ROUNDS):
        residual = z - (xy[:, 0] * plane['x_coef'] + xy[:, 1] * plane['y_coef'] + plane['intercept'])
        inliers = np.abs(residual) <= _residual_scale(plane)
        if np.count_nonzero(inliers) < max(3, _MIN_TRIM_SHARE * len(z)):
            break
        _set_plane_coefficients(plane, xy[inliers], z[inliers])
    plane['inliers_xy'] = xy


def _snapped_aspect(plane: RoofPlane, face_index: _FaceIndex) -> Optional[float]:
    """
    As DETSAC does: the building face azimuth (or a right angle to one) nearest the
    plane's aspect, else nearest its pixels' mean aspect; or for flat roofs nearest south.

    Faces are looked for within 1m of the plane's pixels, then 3m and 10m, as in
    `get_potential_aspects` - but a wider search is also tried if no nearer face is close
    in angle, not only if there are no nearer faces: a plane's pixels may stop short of
    the eaves, where the nearest faces are those of another part of the building.
    """
    points = shapely.points(plane['inliers_xy'])
    points_tree = STRtree(points)
    for distance in (1.0, 3.0, 10.0):
        nearby = face_index.faces_near(points, points_tree, distance)
        if len(nearby) == 0:
            continue
        azimuths = _right_angles(face_index, nearby)
        if plane['is_flat']:
            snapped = closest_azimuth(azimuths, 180, FLAT_ROOF_AZIMUTH_ALIGNMENT_THRESHOLD)
        else:
            snapped = closest_azimuth(azimuths, plane['aspect_raw'], AZIMUTH_ALIGNMENT_THRESHOLD)
            if snapped is None:
                snapped = closest_azimuth(azimuths, plane['aspect_circ_mean'], AZIMUTH_ALIGNMENT_THRESHOLD)
        if snapped is not None:
            return snapped
    return None


def _right_angles(face_index: _FaceIndex, faces) -> List[int]:
    """The azimuths of `faces`, and those at right angles to them (as `get_potential_aspects`)."""
    base = [int(azimuth_deg(face_index.line_segments[i].coords[0], face_index.line_segments[i].coords[1]))
            for i in faces]
    return list({(az + turn) % 360 for az in base for turn in (0, 90, 180, 270)})


def _set_plane_coefficients(plane: dict, xy: np.ndarray, z: np.ndarray) -> None:
    """The part of `set_plane_fit` that `assign_pixels` needs between rounds of refining,
    without the fit metrics, shape ratios and aspect statistics."""
    lr = PlaneFit().fit(xy, z)
    plane['x_coef'], plane['y_coef'] = lr.coef_
    plane['intercept'] = lr.intercept_
    plane['slope'] = slope_deg(lr.coef_[0], lr.coef_[1])
    plane['is_flat'] = plane['slope'] <= FLAT_ROOF_DEGREES_THRESHOLD
    plane['aspect_raw'] = aspect_deg(lr.coef_[0], lr.coef_[1])
    plane['inliers_xy'] = xy


def _load(pg_uri: str,
          job_id: int,
          building_ids: List[str],
          force_load: bool = False) -> Dict[str, RoofDetBuilding]:
    """
    Load LIDAR pixel data for roof plane detection. page_size is number of
    buildings rather than pixels to prevent splitting a building's pixels across
    pages.
    """
    with semaphore_connection(pg_uri, cursor_factory=DictCursor) as pg_conn:
        elevation_table = f"{tables.schema(job_id)}.{tables.ELEVATION}"
        aspect_table = f"{tables.schema(job_id)}.{tables.ASPECT}"
        slope_table = f"{tables.schema(job_id)}.{tables.SLOPE}"

        by_building_id = pixels_for_buildings(pg_conn, job_id, 0, len(building_ids),
                                       [elevation_table, aspect_table, slope_table],
                                       building_ids, force_load=force_load)
        
        # TODO The things in building_ids that aren't in list(by_building_id.keys()) need marking as no coverage.
        #      Currently the outdated lidar checker misses them because there are pixels nearby.
        buildings = _load_building_polygons(pg_conn, job_id, list(by_building_id.keys()))
        
        loaded = {}
        for building in buildings:
            building_id = building["building_id"]
            pixels = by_building_id[building_id]
            loaded[building_id] = {}
            loaded[building_id]["building_id"] = building_id
            loaded[building_id]["polygon"] = wkt.loads(building["polygon"])
            loaded[building_id]["pixels"] = pixels
            loaded[building_id]["min_ground_height"] = building["min_ground_height"]
            loaded[building_id]["max_ground_height"] = building["max_ground_height"]
        return loaded


def _load_building_polygons(pg_conn, job_id, building_ids: List[str]) -> List[dict]:
    buildings = sql_command(
            pg_conn,
            """
            SELECT building_id, ST_AsText(geom_27700) AS polygon, min_ground_height, max_ground_height
            FROM {buildings}
            WHERE building_id = ANY( %(building_ids)s )""",
            {"building_ids": building_ids},
            buildings=Identifier(tables.schema(job_id), tables.BUILDINGS_TABLE),
            result_extractor=lambda rows: rows)

    return buildings


def _buildings_with_areas(pg_uri: str, job_id: int) -> List[Tuple[str, float]]:
    """Get all building IDs with their areas, sorted by area (largest first) for adaptive batching"""
    with connection(pg_uri, cursor_factory=DictCursor) as pg_conn:
        return sql_command(
            pg_conn,
            """SELECT building_id, ST_Area(geom_27700) as area
               FROM {buildings} 
               WHERE exclusion_reason IS NULL
               AND ST_Area(geom_27700) <= %(max_area)s
               ORDER BY ST_Area(geom_27700) DESC;""",
            bindings={"max_area": ROOFDET_MAX_AREA},
            buildings=Identifier(tables.schema(job_id), tables.BUILDINGS_TABLE),
            result_extractor=lambda rows: [(row['building_id'], row['area']) for row in rows])


def _create_adaptive_batches(buildings_with_areas: List[Tuple[str, float]]) -> List[List[str]]:
    """
    Create batches with sizes that adapt based on building area - larger buildings get smaller batches.

    buildings_with_areas must be sorted in descending order of area for this to work.
    """
    batches = []
    current_batch_size = None
    current_batch = []
    
    for building_id, area in buildings_with_areas:
        if area > 10000:
            batch_size = 1
        elif area > 2000:
            batch_size = 5
        elif area > 500:
            batch_size = 10
        else:
            batch_size = 500
        
        if (current_batch_size is not None and batch_size != current_batch_size) or len(current_batch) >= batch_size:
            batches.append(current_batch)
            current_batch = []
        
        current_batch.append(building_id)
        current_batch_size = batch_size
        
    if current_batch:
        batches.append(current_batch)
    
    return batches


def _pixels_xyz(pixels: List[dict]) -> np.ndarray:
    return np.array([[pixel["x"], pixel["y"], pixel["elevation"]] for pixel in pixels])


def _set_skew(polygons: List[RoofPolygon], pixels: List[dict], resolution_metres: float) -> None:
    """
    Each polygon's skew against the pixels whose centres are in its grown polygon - the
    polygon that is output, and that the horizon is averaged over.
    """
    xyz = np.array([[pixel["x"], pixel["y"], pixel["elevation"]] for pixel in pixels])
    for polygon in polygons:
        inside = shapely.contains_xy(polygon['roof_geom_27700'], xyz[:, 0], xyz[:, 1])
        polygon['skew'] = plane_skew(xyz[inside],
                                     (polygon['x_coef'], polygon['y_coef'], polygon['intercept']),
                                     resolution_metres)


def _save_planes(pg_uri: str, job_id: int, planes: List[RoofPolygon]):
    if len(planes) == 0:
        return

    for plane in planes:
        # TODO maybe don't have to save inliers_xy as it's only needed for dev_roof_polygons?
        plane['inliers_xy'] = plane['inliers_xy'].tolist()
        plane.setdefault('not_usable_reason', None)
        plane['meta'] = Json({
            "sd": plane["sd"],
            "score": plane["score"],
            "aspect_circ_mean": plane["aspect_circ_mean"],
            "aspect_circ_sd": plane["aspect_circ_sd"],
            "thinness_ratio": plane["thinness_ratio"],
            "cv_hull_ratio": plane["cv_hull_ratio"],
            "plane_type": plane["plane_type"],
            "aspect_raw": plane["aspect_raw"],
            "r2": plane["r2"],
            "mae": plane["mae"],
            "mse": plane["mse"],
            "rmse": plane["rmse"],
            "msle": plane["msle"],
            "mape": plane["mape"],
            "skew": plane.get("skew"),
        })
        plane['roof_geom_27700'] = plane['roof_geom_27700'].wkt
        plane['roof_geom_raw_27700'] = shapely.multipolygons(shapely.get_parts(plane['roof_geom_raw_27700'])).wkt

    with semaphore_connection(pg_uri) as pg_conn, pg_conn.cursor() as cursor:
        execute_values(cursor, SQL("""
            INSERT INTO {roof_polygons} (
                building_id, 
                roof_geom_27700, 
                roof_geom_raw_27700, 
                x_coef, 
                y_coef, 
                intercept, 
                slope, 
                panel_slope,
                aspect,
                is_flat, 
                usable, 
                not_usable_reason,
                inliers_xy,
                meta
            ) VALUES %s;
        """).format(
            roof_polygons=Identifier(tables.schema(job_id), tables.ROOF_POLYGON_TABLE),
        ), argslist=planes,
           template="""(%(building_id)s, 
                        %(roof_geom_27700)s, 
                        %(roof_geom_raw_27700)s, 
                        %(x_coef)s,
                        %(y_coef)s, 
                        %(intercept)s, 
                        %(slope)s, 
                        %(panel_slope)s,
                        %(aspect)s, 
                        %(is_flat)s, 
                        %(usable)s, 
                        %(not_usable_reason)s,
                        %(inliers_xy)s,
                        %(meta)s )""")

        pg_conn.commit()


def _save_rejected(pg_uri: str, job_id: int, rejected: List[dict], resolution_metres: float):
    if len(rejected) == 0:
        return

    rows = []
    for r in rejected:
        meta = {k: _json_safe(r.get(k)) for k in _REJECTED_META_FIELDS if r.get(k) is not None}
        meta.update({k: _json_safe(v) for k, v in r['rejection_details'].items()})
        geom = _pixel_region(r['inliers_xy'], resolution_metres)
        rows.append({
            "building_id": r['building_id'],
            "rejection_reason": r['rejection_reason'],
            "geom_27700": shapely.multipolygons(shapely.get_parts(geom)).wkt,
            "slope": _json_safe(r.get('slope')),
            "aspect": _json_safe(r.get('aspect')),
            "is_flat": _json_safe(r.get('is_flat')),
            "meta": Json(meta),
        })

    with semaphore_connection(pg_uri) as pg_conn, pg_conn.cursor() as cursor:
        execute_values(cursor, SQL("""
            INSERT INTO {rejected_planes} (building_id, rejection_reason, geom_27700, slope, aspect, is_flat, meta)
            VALUES %s;
        """).format(
            rejected_planes=Identifier(tables.schema(job_id), tables.REJECTED_PLANE_TABLE),
        ), argslist=rows,
           template="""(%(building_id)s, %(rejection_reason)s, %(geom_27700)s, %(slope)s, %(aspect)s,
                        %(is_flat)s, %(meta)s)""")
        pg_conn.commit()


_REJECTED_META_FIELDS = ("plane_type", "plane_id", "score", "sd", "r2", "mae", "rmse",
                         "aspect_raw", "aspect_circ_mean", "aspect_circ_sd", "thinness_ratio", "cv_hull_ratio")


def _json_safe(v):
    """numpy scalars as python ones, so psycopg2 and json can take them."""
    return v.item() if isinstance(v, np.generic) else v


def _mark_buildings_with_no_planes(pg_uri: str, job_id: int):
    with connection(pg_uri) as pg_conn:
        sql_command(
            pg_conn,
            """
            UPDATE {buildings} b
            SET exclusion_reason = 'NO_ROOF_PLANES_DETECTED'
            WHERE
                NOT EXISTS (SELECT FROM {roof_polygons} rp WHERE rp.building_id = b.building_id)
                AND b.exclusion_reason IS NULL;
                
            -- Update building.exclusion_reason for any buildings that have roof planes but no
            -- usable ones:
            UPDATE {buildings} b
            SET exclusion_reason = 'ALL_ROOF_PLANES_UNUSABLE'
            WHERE
                NOT EXISTS (SELECT FROM {roof_polygons} rp WHERE rp.usable AND rp.building_id = b.building_id)
                AND b.exclusion_reason IS NULL;
            """,
            roof_polygons=Identifier(tables.schema(job_id), tables.ROOF_POLYGON_TABLE),
            buildings=Identifier(tables.schema(job_id), tables.BUILDINGS_TABLE))


def _clear(pg_uri: str, job_id: int):
    with connection(pg_uri) as pg_conn:
        sql_command(
            pg_conn,
            """
            TRUNCATE TABLE {roof_polygons};
            TRUNCATE TABLE {rejected_planes};
            """,
            roof_polygons=Identifier(tables.schema(job_id), tables.ROOF_POLYGON_TABLE),
            rejected_planes=Identifier(tables.schema(job_id), tables.REJECTED_PLANE_TABLE))



def _write_test_data(job_id: int, building: RoofDetBuilding):
    """Write data for building in the format that the RANSAC tests expect"""
    debug_data_dir = os.environ.get("DEBUG_DATA_DIR")
    os.makedirs(debug_data_dir, exist_ok=True)
    if debug_data_dir:
        fname = join(debug_data_dir, f"{job_id}_{building['building_id']}.json")
        with open(fname, 'w') as f:
            json.dump(building, f, default=str)
        print(f"Wrote debug data to {fname}")
    else:
        print(json.dumps(building, default=str))


@contextmanager
def semaphore_connection(pg_uri: str, **kwargs):
    _PG_CONN_SEMAPHORE.acquire(timeout=_SEMAPHORE_TIMEOUT_S)
    try:
        pg_conn = connect(pg_uri, **kwargs)
        try:
            yield pg_conn
        finally:
            pg_conn.close()
    finally:
        _PG_CONN_SEMAPHORE.release()
