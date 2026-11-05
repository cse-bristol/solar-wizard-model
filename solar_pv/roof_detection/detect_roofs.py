import json

# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import logging
import os
from contextlib import contextmanager
from os.path import join

import time
import traceback
from typing import List, Dict, Tuple, Optional
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, as_completed

from psycopg2.extras import DictCursor, execute_values, Json
from psycopg2.sql import SQL, Identifier
import numpy as np
import shapely
from shapely import STRtree
from shapely import wkt

from solar_pv.geos import deg_diff, slope_deg, aspect_deg
from solar_pv.db_funcs import count, sql_command, connect, connection
from solar_pv.postgis import pixels_for_buildings
from solar_pv import tables, stage
from solar_pv.constants import ROOFDET_MAX_CPUS, ROOFDET_MAX_AREA, \
    AZIMUTH_ALIGNMENT_THRESHOLD, FLAT_ROOF_AZIMUTH_ALIGNMENT_THRESHOLD, FLAT_ROOF_DEGREES_THRESHOLD
from solar_pv.roof_detection import rejections
from solar_pv.roof_detection.mess import reject_mess
from solar_pv.roof_detection.assign_pixels import assign_pixels, NONE
from solar_pv.roof_detection.footprint import FaceIndex, closest_azimuth
from solar_pv.roof_detection.plane_metrics import set_plane_fit, set_fit_metrics, min_thinness_ratio
from solar_pv.roof_detection.plane_fit import PlaneFit
from solar_pv.roof_detection.plane_skew import plane_skew
from solar_pv.roof_detection.plane_moves import merge_by_energy, remove_unprofitable, data_cost, Plane
from solar_pv.roof_detection.segment_planes import segment_planes, local_fits
from solar_pv.roof_polygons.roof_polygons import create_roof_polygons, _pixel_region
from solar_pv.datatypes import RoofDetBuilding, RoofPlane, RoofPolygon
from solar_pv.util import get_cpu_count

_SEMAPHORE_TIMEOUT_S = 120
_SEMAPHORE_MAX_CONN = 50
_PG_CONN_SEMAPHORE = mp.Semaphore(_SEMAPHORE_MAX_CONN)


# The residual scale (m) of a pitched or flat plane: the furthest a pixel may be from a
# region's plane as it grows, and the unit of elevation residual in `assign_pixels`.
_RESIDUAL_THRESHOLD = 0.25
_FLAT_ROOF_RESIDUAL_THRESHOLD = 0.1
# Max rounds of refitting planes to their pixels and reassigning pixels to the planes.
_MAX_REFINE_ROUNDS = 10
# Refining stops once a round lowers the energy by less than this share.
_MIN_REFINE_ENERGY_FALL = 0.01
# A poorly fitting region's pixels must cover at least this share of their convex hull:
_MIN_CONVEX_HULL_RATIO = 0.65
# A pixel is locally planar, and its normal reliable when growing regions, if the best 3x3
# window containing it fits a plane this well (m):
_MAX_LOCAL_RMS = 0.1
_MIN_SMALL_REGION_PLANAR_SHARE = 0.5
# Max angle between a pixel's normal and a region's (when growing regions) or a plane's:
_MAX_NORMAL_ANGLE_DEGREES = 12
# A pitched region is implausible if fewer than this share of its locally planar pixels
# face the same way as it, if it has at least `_MIN_FACING_PIXELS` of them (see `_facing_share`):
_MIN_FACING_SHARE = 0.5
_MIN_FACING_PIXELS = 3
# The widest search for faces near a plane (see `_snapped_aspect`):
_FOOTPRINT_SEARCH_M = 10.0
# A pitched region not aligned with any of the building's faces is kept only if it's at
# least this steep and big, and its locally planar pixels corroborate it (see `_implausible`).
# Its pixels' normals are within `_MAX_NORMAL_ANGLE_DEGREES` of its own, which a flat
# surface's pixels also are if it's not much steeper than that; shallow regions accepted
# within large flat roofs can cut them in two when refining, and a plane keeps only its
# largest part; and many smaller regions are clutter:
_MIN_OFF_FOOTPRINT_SLOPE = 15.0
_MIN_OFF_FOOTPRINT_PIXELS = 12
# The energy (in `assign_pixels`' units) a plane must save to be worth having.
# TODO: this is per plane at any resolution, while the smoothness and mess checks scale
#  with it, so at 0.5m a plane costs a quarter as much per m2 as at 1m (and 4x at 2m).
#  Decide whether it's per area (divide by res^2) or per plane, and scale segment's minimum
#  region pixels to match; that needs a sample at another resolution. Tuned at 1m.
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
# A finished plane is refitted to just its pixels within this (m) of it, if less than its
# residual scale: at 1m, the pixels 0.1-0.25m off a pitched plane are mostly mixed ridge, eave
# or gutter pixels. Refining keeps to the residual scale, as fitting planes this tightly
# there splits more faces.
_FINAL_TRIM_RESIDUAL = 0.1
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
    }

    work_queue = _create_adaptive_batches(buildings_with_areas)

    executor = ProcessPoolExecutor(max_workers=workers)
    try:
        futures = []
        for batch_building_ids in work_queue:
            futures.append(executor.submit(_handle_building_batch,
                                           pg_uri, job_id, batch_building_ids, params))

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


def _handle_building_batch(pg_uri: str, job_id: int, building_ids: List[str], params: dict):
    start_time = time.time()
    buildings = _load(pg_uri, job_id, building_ids)

    polygons = []
    rejected = []
    for building_id, building in buildings.items():
        try:
            t0 = time.time()
            found = _detect_building_roof_planes(building, building_id, params['resolution_metres'],
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
                                                     rejected=rejected)
            _set_skew(building_polygons, building['pixels'], params['resolution_metres'])
            _set_footprint_aspect_gap(building_polygons, building['polygon'])
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
                                 rejected: Optional[List[dict]] = None) -> List[RoofPlane]:
    """
    Find the building's roof planes (see `_segment_and_assign`), from its pixels above
    ground. Planes thrown away are recorded in `rejected` (see `rejections`).
    """
    pixels = building['pixels']
    max_ground_height = building['max_ground_height']
    xyz = _pixels_xyz(pixels)
    aspect = np.array([pixel["aspect"] for pixel in pixels])
    z = xyz[:, 2]
    above_ground = z > max_ground_height if max_ground_height else np.ones(len(z), dtype=bool)

    min_pixels = min(8, int(8 / resolution_metres))  # 4 for 2m, 8 for 1m, 8 for 0.5m
    small_pixels = int(len(z) * 0.008)
    return _segment_and_assign(xyz, aspect, building['polygon'], above_ground, building_id, resolution_metres,
                               min_pixels, small_pixels, debug=debug, rejected=rejected)


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
    pixels no plane explains well - then finish the planes (`_finalise`) and reject clutter
    and mess. Until they're finished, planes are only what those steps use (`Plane`).

    New candidates don't come from the pixels of planes removed since the last proposal (as
    unprofitable, or left with too few pixels): their pixels are poorly explained, but a
    region grown from them is mostly removed again.

    Candidates are the regions of `segment_planes` that pass the gates in `_propose`. The
    regions over-segment: a region only has to be good enough
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
    face_index = FaceIndex.of(polygon)
    planes: Dict[int, Plane] = {}

    def propose(candidates: np.ndarray, first_id: int, min_region_pixels: int) -> int:
        return _propose(xyz, aspect, candidates, labels, planes, (normals, rms), face_index, building_id,
                        resolution_metres, min_region_pixels, small_pixels, first_id, rejected)

    # planes may shrink below `min_pixels` while refining: whether they're worth keeping is
    # for `_merge_and_remove` to judge
    def refine(removed: np.ndarray) -> np.ndarray:
        """`_refine`, marking in `removed` the pixels of the planes it drops for having too few."""
        start, plane_ids = labels, set(planes)
        new_labels = _refine(xyz, labels, planes, above_ground, resolution_metres, labels_nodata, debug=debug,
                             rejected=rejected)
        removed[np.isin(start, list(plane_ids - set(planes)))] = True
        return new_labels

    next_id = propose(above_ground, 0, min_pixels)
    if debug:
        print(f"Segmented into {len(planes)} plausible regions")
    for round_ in range(_MAX_PROPOSAL_ROUNDS + 1):
        if len(planes) == 0:
            return []
        removed = np.zeros(len(xyz), dtype=bool)
        labels = refine(removed)
        assigned = labels != labels_nodata
        if _merge_and_remove(xyz, labels, planes, resolution_metres, labels_nodata, rejected, debug):
            # merging only relabels pixels, so those it unassigned were removed planes':
            removed |= assigned & (labels == labels_nodata)
            labels = refine(removed)
        if round_ == _MAX_PROPOSAL_ROUNDS:
            break
        candidates = above_ground & ~removed & ~_explained(xyz, labels, planes, normals, locally_planar)
        before = next_id
        next_id = propose(candidates, next_id, _MIN_REPROPOSED_PIXELS)
        if debug:
            print(f"Proposal round {round_}: {next_id - before} new candidates")
        if next_id == before:
            break
    if len(planes) == 0:
        return []
    reliable_normals = np.where(locally_planar[:, None], normals, np.nan)
    return reject_mess(_finalise(xyz, aspect, labels, planes, face_index, resolution_metres, reliable_normals),
                       labels, xyz, resolution_metres, above_ground=above_ground, debug=debug, rejected=rejected)


def _propose(xyz: np.ndarray,
             aspect: np.ndarray,
             candidates: np.ndarray,
             labels: np.ndarray,
             planes: Dict[int, Plane],
             local: Tuple[np.ndarray, np.ndarray],
             face_index: FaceIndex,
             building_id: str,
             resolution_metres: float,
             min_pixels: int,
             small_pixels: int,
             first_id: int,
             rejected: Optional[List[dict]] = None) -> int:
    """
    Add (in place) the regions of `segment_planes` among the `candidates` pixels that are
    plausible as candidate planes to `planes`, taking their pixels in `labels`, with ids
    from `first_id`. Returns the next unused id. `local` is each pixel's local
    fit (see `local_fits`). A region is judged on its least-squares plane, but its plane
    in `planes` is fitted as refining fits planes (see `_fit`).

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
    regions = segment_planes(xyz[candidate_idxs], resolution_metres,
                             max_normal_angle_degrees=_MAX_NORMAL_ANGLE_DEGREES,
                             max_distance=_RESIDUAL_THRESHOLD,
                             max_flat_distance=_FLAT_ROOF_RESIDUAL_THRESHOLD,
                             max_normal_rms=_MAX_LOCAL_RMS,
                             min_pixels=min_pixels,
                             local=(normals[candidate_idxs], rms[candidate_idxs]))
    plane_id = first_id
    for region in np.unique(regions[regions >= 0]):
        idxs = candidate_idxs[regions == region]
        plane = {"building_id": building_id, "plane_type": "segmented", "plane_id": f"segmented_{plane_id}",
                 "res": resolution_metres, "inliers_xy": xyz[idxs, :2]}
        if len(idxs) < small_pixels and locally_planar[idxs].mean() < _MIN_SMALL_REGION_PLANAR_SHARE:
            reason = rejections.SMALL_POOR_FIT
        else:
            set_plane_fit(plane, xyz[idxs, :2], xyz[idxs, 2], aspect[idxs], resolution_metres)
            reason = _implausible(plane, face_index, normals[idxs][locally_planar[idxs]])
        if reason:
            rejections.record(rejected, plane, reason)
            continue
        coef, is_flat = _fit(xyz[idxs])
        planes[plane_id] = Plane(coef=coef, is_flat=is_flat, plane_type=plane['plane_type'],
                                 plane_id=plane['plane_id'], xy=plane['inliers_xy'], proposed=plane)
        labels[idxs] = plane_id
        plane_id += 1
    return plane_id


def _explained(xyz: np.ndarray, labels: np.ndarray, planes: Dict[int, Plane], normals: np.ndarray,
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
        a, b, c = plane.coef
        cost = data_cost(xyz[idxs], plane.coef, _residual_scale(plane.is_flat))
        unit = np.array([-a, -b, 1.0]) / np.sqrt(a * a + b * b + 1)
        facing = ~locally_planar[idxs] | (normals[idxs] @ unit >= cos_max)
        explained[idxs[(cost <= 1) & facing]] = True
    return explained


def _mergeable(p: Plane, q: Plane) -> bool:
    """
    Whether two planes could be the same roof face: both flat, or facing the same way.
    Energy alone doesn't separate a roof face split by noise from, say, a dormer's side
    and the roof it's on, at 1m: small planes' energies differ by little either way.
    """
    if p.is_flat or q.is_flat:
        return p.is_flat and q.is_flat
    return deg_diff(p.aspect_raw, q.aspect_raw) <= AZIMUTH_ALIGNMENT_THRESHOLD


def _merge_and_remove(xyz: np.ndarray,
                      labels: np.ndarray,
                      planes: Dict[int, Plane],
                      resolution_metres: float,
                      labels_nodata: int,
                      rejected: Optional[List[dict]] = None,
                      debug: bool = False) -> bool:
    """Merge, then remove, planes (in place) where that lowers the energy including
    `_LABEL_COST` per plane. Returns whether anything changed."""
    merges = merge_by_energy(xyz, labels, planes, resolution_metres, _LABEL_COST, _residual_scale, _fit, _mergeable,
                             good_enough_cost=_GOOD_ENOUGH_MERGE_COST)
    kept = dict(planes)
    removed = remove_unprofitable(xyz, labels, planes, resolution_metres, _LABEL_COST, _residual_scale, labels_nodata)
    for plane_id in removed:
        rejections.record(rejected, kept[plane_id].record(), rejections.UNPROFITABLE)
    if debug:
        print(f"Merged {len(merges)} pairs of planes, removed {len(removed)}")
    return bool(merges or removed)


def _implausible(plane: RoofPlane, face_index: FaceIndex, reliable_normals: np.ndarray) -> Optional[str]:
    """
    Why `plane` isn't plausible as a candidate - not aligned with the building, a poor
    shape, or (if pitched) its locally planar pixels, whose normals are in
    `reliable_normals`, mostly face another way - or None if it is. Sets the plane's `aspect`:
    snapped to the building's faces if one is close to it (see `_snapped_aspect`), else its own.

    A plane not aligned with any of the building's faces keeps its own aspect if it's steep
    and big enough, and enough of its pixels are locally planar to corroborate it (see
    `_facing_share`): building footprints derived from LiDAR follow the roof's edges only
    roughly. Otherwise disagreeing with the footprint is still taken as a sign of clutter.

    The shape checks only apply to regions that fit their plane poorly: a region that
    fits closely is planar whatever its shape, and clean flat roofs are often concave
    (wrapping round a raised part of the building) or long and thin. Regions don't need a
    check of the fit itself: they only grow within `segment_planes`' `max_distance` of
    their plane.

    Pixels' normals are checked rather than the spread of their raster aspects. Those come
    from the 3x3 window centred on each pixel, which at 1m, on faces only a few pixels
    deep, mostly straddles a ridge, eave or valley; a locally planar pixel's normal comes
    from a window wholly on one face.
    """
    plane['aspect'], aligned = _snapped_aspect(plane, face_index)
    if not aligned and (plane['slope'] < _MIN_OFF_FOOTPRINT_SLOPE or len(plane['inliers_xy']) < _MIN_OFF_FOOTPRINT_PIXELS
                        or len(reliable_normals) < _MIN_FACING_PIXELS):
        return rejections.NO_CLOSE_ASPECT
    if plane['aspect'] is None:
        plane['aspect'] = plane['aspect_raw']
    good_fit = plane['mae'] <= _residual_scale(plane['is_flat']) / 2
    if not good_fit:
        if plane['cv_hull_ratio'] < _MIN_CONVEX_HULL_RATIO:
            return rejections.CONVEX_HULL_RATIO
        if plane['thinness_ratio'] < min_thinness_ratio(len(plane['inliers_xy'])):
            return rejections.THINNESS_RATIO
    if not plane['is_flat']:
        share = _facing_share(plane, reliable_normals)
        if share is not None and share < _MIN_FACING_SHARE:
            return rejections.NORMALS_DISAGREE
    return None


def _facing_share(plane: RoofPlane, reliable_normals: np.ndarray) -> Optional[float]:
    """
    The share of the plane's locally planar pixels whose own normal is within
    `_MAX_NORMAL_ANGLE_DEGREES` of the plane's, or None if too few are locally planar to say.
    """
    if len(reliable_normals) < _MIN_FACING_PIXELS:
        return None
    a, b = plane['x_coef'], plane['y_coef']
    unit = np.array([-a, -b, 1.0]) / np.sqrt(a * a + b * b + 1)
    return float(np.mean(reliable_normals @ unit >= np.cos(np.radians(_MAX_NORMAL_ANGLE_DEGREES))))


def _residual_scale(is_flat: bool) -> float:
    return _FLAT_ROOF_RESIDUAL_THRESHOLD if is_flat else _RESIDUAL_THRESHOLD


def _refine(xyz: np.ndarray,
            labels: np.ndarray,
            planes: Dict[int, Plane],
            above_ground: np.ndarray,
            resolution_metres: float,
            labels_nodata: int,
            debug: bool = False,
            rejected: Optional[List[dict]] = None) -> np.ndarray:
    """
    Reassign pixels and refit `planes` (in place) until the labels stop changing or the
    energy stops falling.

    Refitting is by least squares, not the energy `assign_pixels` minimises, so it can
    raise the energy and the rounds can cycle rather than converge.
    """
    last_energy = np.inf
    for round_ in range(_MAX_REFINE_ROUNDS):
        new_labels, energy = _reassign(xyz, labels, planes, above_ground, resolution_metres, labels_nodata,
                                       rejected)
        changed = np.count_nonzero(new_labels != labels)
        labels = new_labels
        if debug:
            print(f"Refine round {round_}: {len(planes)} planes, {changed} pixels changed, energy {energy:.1f}")
        if changed == 0 or len(planes) == 0 or energy > last_energy * (1 - _MIN_REFINE_ENERGY_FALL):
            break
        last_energy = energy
    return labels


def _reassign(xyz: np.ndarray,
              labels: np.ndarray,
              planes: Dict[int, Plane],
              above_ground: np.ndarray,
              resolution_metres: float,
              labels_nodata: int,
              rejected: Optional[List[dict]] = None) -> Tuple[np.ndarray, float]:
    """
    Reassign `labels` to the planes as they are, each keeping only its largest 4-connected
    group of pixels (see `assign_pixels`), and refit `planes` to them (in place,
    see `_fit`). Planes left with fewer than 3 pixels are removed, and recorded in
    `rejected`. Also returns `assign_pixels`' energy for the starting labels.
    """
    plane_ids = list(planes.keys())
    idx_labels = np.full(np.count_nonzero(above_ground), NONE)
    for k, plane_id in enumerate(plane_ids):
        idx_labels[labels[above_ground] == plane_id] = k

    idx_labels, energy = assign_pixels(xyz[above_ground], idx_labels, [planes[i].coef for i in plane_ids],
                                       [_residual_scale(planes[i].is_flat) for i in plane_ids], resolution_metres)

    new_labels = np.full(labels.shape, labels_nodata)
    candidate_idxs = np.flatnonzero(above_ground)
    for k, plane_id in enumerate(plane_ids):
        idxs = candidate_idxs[idx_labels == k]
        if len(idxs) < 3:
            rejections.record(rejected, planes.pop(plane_id).record(), rejections.TOO_FEW_PIXELS, pixels=len(idxs))
            continue
        new_labels[idxs] = plane_id
        plane = planes[plane_id]
        plane.coef, plane.is_flat = _fit(xyz[idxs])
        plane.xy = xyz[idxs, :2]
    return new_labels, energy


def _fit(xyz: np.ndarray, max_trim_residual: float = np.inf) -> Tuple[Tuple[float, float, float], bool]:
    """
    A plane through `xyz` and whether it's flat: fitted by least squares, then again (for up
    to `_TRIM_ROUNDS`) to just the pixels within its residual scale of it (or
    `max_trim_residual`, if less), if there are enough of them. Pixels up to about twice that from a plane are assigned to it, and at
    1m those are mostly mixed ridge, eave or gutter pixels, which tilt a least-squares fit
    on a plane only a few pixels deep.
    """
    xy, z = xyz[:, :2], xyz[:, 2]
    coef, is_flat = _least_squares(xy, z)
    for _ in range(_TRIM_ROUNDS):
        residual = z - (xy[:, 0] * coef[0] + xy[:, 1] * coef[1] + coef[2])
        inliers = np.abs(residual) <= min(_residual_scale(is_flat), max_trim_residual)
        if np.count_nonzero(inliers) < max(3, _MIN_TRIM_SHARE * len(z)):
            break
        coef, is_flat = _least_squares(xy[inliers], z[inliers])
    return coef, is_flat


def _least_squares(xy: np.ndarray, z: np.ndarray) -> Tuple[Tuple[float, float, float], bool]:
    lr = PlaneFit().fit(xy, z)
    return (lr.coef_[0], lr.coef_[1], lr.intercept_), slope_deg(lr.coef_[0], lr.coef_[1]) <= FLAT_ROOF_DEGREES_THRESHOLD


def _finalise(xyz: np.ndarray,
              aspect: np.ndarray,
              labels: np.ndarray,
              planes: Dict[int, Plane],
              face_index: FaceIndex,
              resolution_metres: float,
              reliable_normals: np.ndarray) -> Dict[int, RoofPlane]:
    """
    Each of `planes` as a `RoofPlane`, fitted to its pixels with everything that derives
    from them (see `set_plane_fit`) but its orientation from just its pixels near it (see
    `_fit` and `_FINAL_TRIM_RESIDUAL`), and its aspect snapped to the building's faces. A
    pitched plane with no face close keeps its own aspect, and a flat one faces south.

    Its fit metrics are of that plane, and its `facing_share` (see `_facing_share`) is from
    `reliable_normals`, each pixel's local normal if it's locally planar, else NaN.
    """
    finished = {}
    for plane_id, plane in planes.items():
        idxs = np.flatnonzero(labels == plane_id)
        roof_plane = {"building_id": plane.proposed["building_id"], "plane_type": plane.plane_type,
                      "plane_id": plane.plane_id, "res": resolution_metres}
        set_plane_fit(roof_plane, xyz[idxs, :2], xyz[idxs, 2], aspect[idxs], resolution_metres)
        coef, is_flat = _fit(xyz[idxs], _FINAL_TRIM_RESIDUAL)
        roof_plane['x_coef'], roof_plane['y_coef'], roof_plane['intercept'] = coef
        roof_plane['slope'] = slope_deg(coef[0], coef[1])
        roof_plane['is_flat'] = is_flat
        roof_plane['aspect_raw'] = aspect_deg(coef[0], coef[1])
        xy, z = xyz[idxs, :2], xyz[idxs, 2]
        set_fit_metrics(roof_plane, z, xy[:, 0] * coef[0] + xy[:, 1] * coef[1] + coef[2])
        normals = reliable_normals[idxs]
        roof_plane['facing_share'] = _facing_share(roof_plane, normals[~np.isnan(normals[:, 0])])
        snapped, _ = _snapped_aspect(roof_plane, face_index)
        if snapped is not None:
            roof_plane['aspect'] = snapped
        else:
            roof_plane['aspect'] = 180 if is_flat else roof_plane['aspect_raw']
        finished[plane_id] = roof_plane
    return finished


def _snapped_aspect(plane: RoofPlane, face_index: FaceIndex) -> Tuple[Optional[float], bool]:
    """
    The building face azimuth (or a right angle to one) nearest the plane's aspect, or for
    flat roofs nearest south, if one is close; and whether the plane is aligned with the
    building: a face is close to its aspect or, if it's pitched, to its pixels' mean aspect.

    Faces are looked for within 1m of the plane's pixels, then 3m and 10m. A wider search
    is tried if no nearer face is close in angle, not only if there are no nearer faces:
    a plane's pixels may stop short of the eaves, where the nearest faces are those of
    another part of the building.

    The mean aspect is from the aspect raster, which is noisy on shallow faces, and on faces
    a few pixels deep comes mostly from windows straddling a ridge, eave or valley (see
    `_implausible`): it's evidence that a plane follows the building, but not of which way
    it faces, so it isn't snapped to.
    """
    if plane['is_flat']:
        target, threshold = 180, FLAT_ROOF_AZIMUTH_ALIGNMENT_THRESHOLD
    else:
        target, threshold = plane['aspect_raw'], AZIMUTH_ALIGNMENT_THRESHOLD
    mean_aligned = False
    points = shapely.points(plane['inliers_xy'])
    points_tree = STRtree(points)
    for distance in (1.0, 3.0, _FOOTPRINT_SEARCH_M):
        nearby = face_index.faces_near(points, points_tree, distance)
        if len(nearby) == 0:
            continue
        azimuths = _right_angles(face_index, nearby)
        snapped = closest_azimuth(azimuths, target, threshold)
        if snapped is not None:
            return snapped, True
        if not plane['is_flat'] and \
                closest_azimuth(azimuths, plane['aspect_circ_mean'], AZIMUTH_ALIGNMENT_THRESHOLD) is not None:
            mean_aligned = True
    return None, mean_aligned


def _right_angles(face_index: FaceIndex, faces) -> List[int]:
    """The azimuths of `faces`, and those at right angles to them."""
    base = [face_index.azimuths[i] for i in faces]
    return list({(az + turn) % 360 for az in base for turn in (0, 90, 180, 270)})


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

        by_building_id = pixels_for_buildings(pg_conn, job_id, 0, len(building_ids),
                                       [elevation_table, aspect_table],
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


def _set_footprint_aspect_gap(planes: List[RoofPlane], polygon) -> None:
    """
    Each pitched plane's `footprint_aspect_gap`: the angle (degrees) between the aspect of
    its plane fit and the nearest of the building's face azimuths and their right angles,
    over the faces within `_FOOTPRINT_SEARCH_M` of its pixels (None if there are none).
    It's None for flat planes, whose aspect means little.
    """
    face_index = FaceIndex.of(polygon)
    for plane in planes:
        plane['footprint_aspect_gap'] = None
        if plane['is_flat'] or len(plane['inliers_xy']) == 0:
            continue
        points = shapely.points(plane['inliers_xy'])
        nearby = face_index.faces_near(points, STRtree(points), _FOOTPRINT_SEARCH_M)
        if len(nearby) > 0:
            fitted = aspect_deg(plane['x_coef'], plane['y_coef'])
            plane['footprint_aspect_gap'] = min(deg_diff(fitted, az) for az in _right_angles(face_index, nearby))


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
            "footprint_aspect_gap": plane.get("footprint_aspect_gap"),
            "facing_share": plane["facing_share"],
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
    """Write data for building in the format that the roof detection tests expect"""
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
