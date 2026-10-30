# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import json
import os
from os.path import join

import time
import traceback
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Optional, cast, Tuple

import math
import numpy as np
import shapely
from shapely import affinity, ops, set_precision
from shapely.geometry import Polygon, CAP_STYLE, JOIN_STYLE, MultiPolygon
from shapely.validation import make_valid

from solar_pv.geos import square, largest_polygon, get_grid_cells, \
    de_zigzag, slope_deg, aspect_deg
from solar_pv.constants import ROOFDET_GOOD_SCORE
from solar_pv.datatypes import RoofPlane, RoofPolygon
from solar_pv.roof_detection import rejections
from solar_pv.roof_polygons.occupancy import tile_polygon
from solar_pv.roof_polygons.split_evenly import split_evenly

# Noding nearly coincident lines (a meeting line along the edge two polygons share) in
# floating point can leave a gap in a ring, so that polygonizing merges the faces either
# side of it.
_NODING_GRID_M = 1e-6

# Mincut polygons: module constants, so that `fast_roof_detection --set` can tune them.
# Tile edges of boundary are charged this much each, in tiles of area (see `tile_polygon`).
PERIMETER_LAMBDA = 0.6
# What it takes for the line between two planes to count as following the edge they share
# (see `_follows_shared_edge`):
MEETING_MIN_SHARED_TILES = 3
MEETING_MAX_LINE_DIST_TILES = 1.5
MEETING_MIN_ON_LINE = 0.9
MEETING_MAX_MISCLASSIFIED = 0.1
# A line between two planes is fitted, and applies, only alongside the edge they share and
# this far past its ends: beyond them, a plane can carry on across where the line would
# run, e.g. a cross gable past the valley it makes with the main roof.
LINE_END_TILES = 0.5
# The looser on-line test for the line where two planes meet when its direction is stable,
# e.g. a ridge whose pixel assignment wobbles to either side of it:
STABLE_MAX_LINE_DIST_TILES = 2
STABLE_MIN_ON_LINE = 0.8
MAX_DIRECTION_SENSITIVITY_DEG = 10
# Half-width of the strip along a line's shared edge where the line alone decides which of
# the two planes owns what (see `_MeetingLine.core`). 0 = no core.
CORE_HALF_WIDTH_TILES = 2
# Planes that touch without meeting along their intersection (e.g. at a step) are split
# along one of their grid axes, if that follows the edge they share (see `_step_line`).
STEP_LINES = True
# De-zigzag tolerance (see `_smooth`). 0 = no de-zigzagging.
SMOOTH_TOLERANCE_TILES = 0.5
# Boundary segments at least this long are kept as they are when de-zigzagging.
SMOOTH_MIN_KEPT_TILES = 2.5


def create_roof_polygons(building_id: str,
                         building_geom: Polygon,
                         planes: List[RoofPlane],
                         max_roof_slope_degrees: int,
                         min_roof_area_m: int,
                         min_roof_degrees_from_north: int,
                         flat_roof_degrees: int,
                         min_dist_to_edge_m: float,
                         resolution_metres: float,
                         method: str = "legacy",
                         rejected: Optional[List[dict]] = None,
                         pixels_xyz: Optional[np.ndarray] = None) -> List[RoofPolygon]:
    """
    Add roof polygons and other related fields to the dicts in `planes`. Planes that end
    up with no polygon are recorded in `rejected` (see `rejections`). `pixels_xyz` is the
    building's pixels, which the legacy method keeps its polygons off where they don't fit
    the plane (see `_carve_off_plane`).
    """
    t0 = time.time()
    polygons = _create_roof_polygons(
        building_geom,
        planes,
        max_roof_slope_degrees=max_roof_slope_degrees,
        min_roof_area_m=min_roof_area_m,
        min_roof_degrees_from_north=min_roof_degrees_from_north,
        flat_roof_degrees=flat_roof_degrees,
        min_dist_to_edge_m=min_dist_to_edge_m,
        resolution_metres=resolution_metres,
        method=method,
        rejected=rejected,
        pixels_xyz=pixels_xyz)
    t1 = time.time()
    if t1 - t0 > 1200:
        print(f"very slow polygon creation: {building_id} took {round(t1 - t0, 2)} s")
        _write_test_data(_to_test_data(building_id, planes, building_geom))
    return polygons


def _create_roof_polygons(building_geom: Polygon,
                          planes: List[RoofPlane],
                          max_roof_slope_degrees: int,
                          min_roof_area_m: int,
                          min_roof_degrees_from_north: int,
                          flat_roof_degrees: int,
                          min_dist_to_edge_m: float,
                          resolution_metres: float,
                          method: str = "legacy",
                          debug: bool = False,
                          rejected: Optional[List[dict]] = None,
                          pixels_xyz: Optional[np.ndarray] = None) -> List[RoofPolygon]:

    if len(planes) == 0:
        return []

    extra = {}
    if method == "mincut":
        make_polygons = _create_roof_polygons_mincut
    elif method == "legacy":
        make_polygons = _create_roof_polygons_legacy
        extra = {"pixels_xyz": pixels_xyz}
    else:
        raise ValueError(f"Unknown roof polygon method {method}")

    try:
        polygons = make_polygons(building_geom,
                                 planes,
                                 max_roof_slope_degrees=max_roof_slope_degrees,
                                 min_roof_area_m=min_roof_area_m,
                                 min_roof_degrees_from_north=min_roof_degrees_from_north,
                                 flat_roof_degrees=flat_roof_degrees,
                                 min_dist_to_edge_m=min_dist_to_edge_m,
                                 resolution_metres=resolution_metres,
                                 debug=debug,
                                 **extra)
    except Exception as e:
        building_id = planes[0]['building_id']
        print(f"Exception during roof polygon creation for building_id {building_id}:")
        traceback.print_exception(e)
        _write_test_data(_to_test_data(planes[0]['building_id'], planes, building_geom))
        raise e

    # Legacy polygon creation merges planes, so a plane missing from its output may
    # have been merged rather than lost:
    if method == "mincut":
        kept = {id(p) for p in polygons}
        for plane in planes:
            if id(plane) not in kept:
                rejections.record(rejected, plane, rejections.NO_POLYGON)
    return polygons


def _create_roof_polygons_legacy(building_geom: Polygon,
                                 planes: List[RoofPlane],
                                 max_roof_slope_degrees: int,
                                 min_roof_area_m: int,
                                 min_roof_degrees_from_north: int,
                                 flat_roof_degrees: int,
                                 min_dist_to_edge_m: float,
                                 resolution_metres: float,
                                 debug: bool = False,
                                 pixels_xyz: Optional[np.ndarray] = None) -> List[RoofPolygon]:
    plane_polys: List[RoofPolygon] = []
    for plane in planes:
        plane = cast(RoofPolygon, plane)
        plane['panel_slope'] = flat_roof_degrees if plane['is_flat'] else plane['slope']

        raw_roof_poly, roof_poly = _make_polygon(plane, building_geom, min_dist_to_edge_m, resolution_metres,
                                                 pixels_xyz=pixels_xyz)

        if roof_poly and not roof_poly.is_empty and raw_roof_poly and not raw_roof_poly.is_empty:
            plane['roof_geom_27700'] = roof_poly
            plane['roof_geom_raw_27700'] = raw_roof_poly
            plane_polys.append(plane)

    if debug:
        print(f"Made {len(plane_polys)} initial roof polygons")

    plane_polys = _merge_touching(plane_polys, building_geom, min_dist_to_edge_m, resolution_metres,
                                  pixels_xyz=pixels_xyz)

    if debug:
        print(f"Merged touching, now have {len(plane_polys)} roof polygons")

    for p in plane_polys:
        p['roof_geom_27700'] = largest_polygon(set_precision(p['roof_geom_27700'], 0.01))

    _remove_overlaps(plane_polys, debug=debug)

    if debug:
        print(f"Removed overlaps")

    for plane in plane_polys:
        roof_poly = plane['roof_geom_27700']
        plane['roof_geom_raw_27700'] = largest_polygon(plane['roof_geom_raw_27700'].intersection(roof_poly))

        # Arbitrarily use the area of roof_geom_27700 rather than roof_geom_raw_27700...
        _set_usability(plane, plane['roof_geom_27700'],
                       max_roof_slope_degrees, min_roof_area_m, min_roof_degrees_from_north)

    return [p for p in plane_polys if _is_valid(p)]


def _create_roof_polygons_mincut(building_geom: Polygon,
                                 planes: List[RoofPlane],
                                 max_roof_slope_degrees: int,
                                 min_roof_area_m: int,
                                 min_roof_degrees_from_north: int,
                                 flat_roof_degrees: int,
                                 min_dist_to_edge_m: float,
                                 resolution_metres: float,
                                 debug: bool = False) -> List[RoofPolygon]:
    """
    Each plane's inlier pixels are tile-quantized by min-cut on a grid aligned to its own
    aspect, within a legal area: the shrunk building minus areas claimed by
    neighbouring planes (see `_exclusions`). Tiles straddling the legal edge are promoted
    and then re-clipped to it, so where two planes share an edge along a line between
    them (see `_meeting_lines`), their boundary comes out as that straight line. Any
    remaining overlaps are split the same way, or else by nearest pixel, and the
    boundaries not along such lines or the building's edge are then de-zigzagged (see
    `_smooth`).

    `roof_geom_raw_27700` is the plane's pixels, as squares, within its polygon, plus any
    of a neighbour's pixels on its side of a line within the line's core (see
    `_MeetingLine`): its area is what the plane's area (and so its kWp, kWh and min-area
    check) is based on.
    """
    tile_m = resolution_metres
    building_shrunk = building_geom.buffer(
        -min_dist_to_edge_m, cap_style=CAP_STYLE.square, join_style=JOIN_STYLE.mitre)
    if building_shrunk.is_empty:
        return []

    plane_polys: List[RoofPolygon] = []
    for plane in planes:
        plane = cast(RoofPolygon, plane)
        plane['panel_slope'] = flat_roof_degrees if plane['is_flat'] else plane['slope']
        plane_polys.append(plane)

    # Planes aren't merged here: that's for roof detection (`merge_adjacent`), which
    # judges it by the fit of the merged plane.
    regions = [_pixel_region(p['inliers_xy'], resolution_metres) for p in plane_polys]
    owned_xy = [p['inliers_xy'] for p in plane_polys]

    lines = _meeting_lines(plane_polys, owned_xy, regions, tile_m)
    exclusions, additions = _exclusions(lines, regions, tile_m)
    raw_regions = _raw_regions(lines, regions)
    for i, plane in enumerate(plane_polys):
        region = shapely.union_all([regions[i]] + additions[i])
        legal = _legal_area(building_shrunk, lines, exclusions[i], i)

        tiled = tile_polygon(region, legal, plane['aspect'], region.centroid, tile_m, PERIMETER_LAMBDA)
        roof_poly = tiled.intersection(legal) if tiled is not None else regions[i].intersection(legal)

        plane['roof_geom_raw_27700'] = raw_regions[i]
        plane['roof_geom_27700'] = roof_poly

    valid = [i for i, p in enumerate(plane_polys) if _is_valid(p)]
    new_index = {old: new for new, old in enumerate(valid)}
    plane_polys = [plane_polys[i] for i in valid]
    valid_lines = {(new_index[i], new_index[j]): line for (i, j), line in lines.items()
                   if i in new_index and j in new_index}
    _split_overlaps(plane_polys, valid_lines, [owned_xy[i] for i in valid], tile_m)
    _smooth(plane_polys, valid_lines, building_shrunk, tile_m)

    for plane in plane_polys:
        roof_poly = largest_polygon(plane['roof_geom_27700'])
        plane['roof_geom_27700'] = roof_poly
        if roof_poly is None or roof_poly.is_empty:
            continue
        plane['roof_geom_raw_27700'] = _polygonal(plane['roof_geom_raw_27700'].intersection(roof_poly))
        _set_usability(plane, plane['roof_geom_raw_27700'],
                       max_roof_slope_degrees, min_roof_area_m, min_roof_degrees_from_north)

    return [p for p in plane_polys if _is_valid(p)]


def _pixel_region(pixels_xy: np.ndarray, resolution_metres: float):
    halfr = resolution_metres / 2
    x, y = pixels_xy[:, 0], pixels_xy[:, 1]
    return shapely.union_all(shapely.box(x - halfr, y - halfr, x + halfr, y + halfr)).simplify(0)


@dataclass
class _MeetingLine:
    """
    A line between two planes, restricted to where it applies (`zone`): near the edge
    they share, and alongside it rather than past its ends (see `_alongside`). `side_1` is
    the half-plane on plane 1's side of the line. Within `core`, a strip either side of the
    line alongside the shared edge, the line alone decides which of the two planes owns
    what, whichever plane the pixels were assigned to.
    """
    side_1: Polygon
    zone: Polygon
    core: Optional[Polygon] = None


def _meeting_lines(planes: List[RoofPlane], owned_xy: List[np.ndarray], regions: list, tile_m: float,
                   zone_tiles: float = 2) -> Dict[Tuple[int, int], _MeetingLine]:
    """
    For pairs of planes (i, j), i < j, that share an edge running along a line between
    them: that line. It's the line where the two planes meet (see `_intersection_line`)
    if that follows the shared edge, else, with `STEP_LINES`, a line along one of the
    planes' grid axes (see `_step_line`).

    Only the pixels of each plane edge-adjacent to the other count as shared, so planes
    separated by unassigned pixels, or touching only at a corner, share nothing.
    """
    gradients = [_gradient(p) for p in planes]
    adjacent_dist = tile_m / 2 + 1e-6
    lines = {}
    for i in range(len(regions)):
        for j in range(i + 1, len(regions)):
            if regions[i].distance(regions[j]) > 1e-6:
                continue
            shared_i = owned_xy[i][shapely.dwithin(regions[j], shapely.points(owned_xy[i]), adjacent_dist)]
            shared_j = owned_xy[j][shapely.dwithin(regions[i], shapely.points(owned_xy[j]), adjacent_dist)]
            if len(shared_i) == 0 or len(shared_j) == 0:
                continue
            shared = np.concatenate([shared_i, shared_j])

            # fit the pair the same way round whatever order the planes are in, as ties
            # in the fit are broken by which plane is first
            first, second = (i, j) if _order_key(owned_xy[i]) <= _order_key(owned_xy[j]) else (j, i)
            line = _intersection_line(planes[first], planes[second], gradients[first], gradients[second],
                                      owned_xy[first], owned_xy[second], shared, tile_m)
            if line is None and STEP_LINES:
                line = _step_line(planes[first], planes[second], owned_xy[first], owned_xy[second], shared, tile_m)
            if line is None:
                continue
            centre, normal, offset, direction = line
            if first != i:
                direction = -direction

            zone = _pixel_region(shared, tile_m).buffer(
                zone_tiles * tile_m, cap_style=CAP_STYLE.square, join_style=JOIN_STYLE.mitre)
            # trimmed only along the line: across it, the zone reaches as far as the buffer does
            zone = zone.intersection(_strip(centre, normal, offset, shared, tile_m, zone.length / tile_m))
            core = _strip(centre, normal, offset, shared, tile_m, CORE_HALF_WIDTH_TILES) \
                if CORE_HALF_WIDTH_TILES > 0 else None
            lines[(i, j)] = _MeetingLine(side_1=_half_plane(centre, normal, offset, direction, zone),
                                         zone=zone, core=core)
    return lines


def _intersection_line(plane1: RoofPlane, plane2: RoofPlane, gradient1: np.ndarray, gradient2: np.ndarray,
                       pixels1_xy: np.ndarray, pixels2_xy: np.ndarray, shared_xy: np.ndarray, tile_m: float,
                       min_gradient_diff: float = 0.05) -> Optional[Tuple[np.ndarray, np.ndarray, float, float]]:
    """
    The line where two roof planes meet, near the pixels they share, if it follows the
    edge they share (see `_follows_shared_edge`); None if the planes are near-parallel.
    Its direction is perpendicular to the difference of their gradients; its offset comes
    from `_fit_line`.

    Where the direction is stable (see `_direction_sensitivity`), the shared pixels may lie
    further from it: e.g. along a ridge, the pixel assignment can wobble to either side of
    a line that is still right. Where it isn't, e.g. planes facing nearly the same way at
    a step, the direction is mostly noise, and a line that doesn't closely follow the
    shared edge is probably wrong.
    """
    normal = gradient1 - gradient2
    norm = np.linalg.norm(normal)
    if norm < min_gradient_diff:
        return None
    line = _fit_line(normal / norm, pixels1_xy, pixels2_xy, shared_xy, tile_m)
    if line is None:
        return None
    if _direction_sensitivity(plane1, plane2) <= MAX_DIRECTION_SENSITIVITY_DEG:
        max_line_dist_tiles, min_on_line = STABLE_MAX_LINE_DIST_TILES, STABLE_MIN_ON_LINE
    else:
        max_line_dist_tiles, min_on_line = MEETING_MAX_LINE_DIST_TILES, MEETING_MIN_ON_LINE
    if not _follows_shared_edge(line, shared_xy, tile_m, max_line_dist_tiles, min_on_line):
        return None
    return line[:4]


def _step_line(plane1: RoofPlane, plane2: RoofPlane, pixels1_xy: np.ndarray, pixels2_xy: np.ndarray,
               shared_xy: np.ndarray, tile_m: float) -> Optional[Tuple[np.ndarray, np.ndarray, float, float]]:
    """
    For planes that touch without meeting along their intersection, e.g. side by side at
    a step: the line along one of either plane's grid axes (see `tile_polygon`) that best
    separates their pixels, if it follows the edge they share.
    """
    best = None
    for normal in _grid_axes(plane1['aspect']) + _grid_axes(plane2['aspect']):
        line = _fit_line(normal, pixels1_xy, pixels2_xy, shared_xy, tile_m)
        if line is not None and _follows_shared_edge(line, shared_xy, tile_m) \
                and (best is None or line[4] < best[4]):
            best = line
    return best[:4] if best is not None else None


def _grid_axes(aspect: float) -> List[np.ndarray]:
    a = math.radians(aspect)
    along = np.array([math.sin(a), math.cos(a)])
    return [along, np.array([-along[1], along[0]])]


def _direction_sensitivity(plane1: RoofPlane, plane2: RoofPlane,
                           aspect_error: float = 5, slope_error: float = 2) -> float:
    """
    How far, in degrees, the line where two planes meet turns if either plane's aspect or
    slope is out by a little.
    """
    def direction(aspect1, slope1, aspect2, slope2) -> float:
        n = _gradient({'aspect': aspect1, 'slope': max(slope1, 0)}) \
            - _gradient({'aspect': aspect2, 'slope': max(slope2, 0)})
        return math.degrees(math.atan2(n[1], n[0]))

    a1, s1, a2, s2 = plane1['aspect'], plane1['slope'], plane2['aspect'], plane2['slope']
    base = direction(a1, s1, a2, s2)
    worst = 0.0
    for e in (aspect_error, -aspect_error):
        for d in (direction(a1 + e, s1, a2, s2), direction(a1, s1, a2 + e, s2)):
            worst = max(worst, _line_angle_diff(base, d))
    for e in (slope_error, -slope_error):
        for d in (direction(a1, s1 + e, a2, s2), direction(a1, s1, a2, s2 + e)):
            worst = max(worst, _line_angle_diff(base, d))
    return worst


def _line_angle_diff(a: float, b: float) -> float:
    diff = abs(a - b) % 180
    return min(diff, 180 - diff)


def _follows_shared_edge(line, shared_xy: np.ndarray, tile_m: float,
                         max_line_dist_tiles: float = None, min_on_line: float = None) -> bool:
    """
    Whether the shared pixels extend at least `MEETING_MIN_SHARED_TILES` along the line,
    and `min_on_line` of them lie within `max_line_dist_tiles` of it. Planes that are
    adjacent without meeting along the line - e.g. side by side, at a step - fail this.
    """
    max_line_dist_tiles = MEETING_MAX_LINE_DIST_TILES if max_line_dist_tiles is None else max_line_dist_tiles
    min_on_line = MEETING_MIN_ON_LINE if min_on_line is None else min_on_line
    centre, normal, offset = line[:3]
    along = (shared_xy - centre) @ np.array([-normal[1], normal[0]])
    dist = np.abs((shared_xy - centre) @ normal - offset)
    return along.max() - along.min() >= (MEETING_MIN_SHARED_TILES - 1) * tile_m \
        and np.mean(dist <= max_line_dist_tiles * tile_m) >= min_on_line


def _strip(centre: np.ndarray, normal: np.ndarray, offset: float, shared_xy: np.ndarray, tile_m: float,
           half_width_tiles: float) -> Polygon:
    """The strip `half_width_tiles` either side of the line, alongside the shared pixels (see `_alongside`)."""
    along_dir = np.array([-normal[1], normal[0]])
    a0, a1 = _alongside(centre, along_dir, shared_xy, tile_m)
    p0 = centre + offset * normal
    w = half_width_tiles * tile_m
    return Polygon([p0 + a0 * along_dir - w * normal, p0 + a1 * along_dir - w * normal,
                    p0 + a1 * along_dir + w * normal, p0 + a0 * along_dir + w * normal])


def _alongside(centre: np.ndarray, along_dir: np.ndarray, shared_xy: np.ndarray, tile_m: float) -> Tuple[float, float]:
    """The range along a line, relative to `centre`, of the shared pixels plus `LINE_END_TILES` each end."""
    along = (shared_xy - centre) @ along_dir
    return along.min() - LINE_END_TILES * tile_m, along.max() + LINE_END_TILES * tile_m


def _order_key(pixels_xy: np.ndarray) -> Tuple[float, float, int]:
    """An order for planes that doesn't depend on the order they're listed in."""
    x, y = pixels_xy.mean(axis=0)
    return float(x), float(y), len(pixels_xy)


def _exclusions(lines: Dict[Tuple[int, int], _MeetingLine], regions: list,
                tile_m: float) -> Tuple[List[List[Tuple[int, Polygon]]], List[list]]:
    """
    For each plane, the areas it may not occupy, each with the plane it's for, and areas
    to add to its region.

    Along an edge shared with a neighbour along a line between them, the line takes
    precedence over which plane owns the pixels: each plane is excluded from the
    neighbour's side of it and gets the neighbour's pixels on its own side. Elsewhere, a
    plane is excluded from other planes' regions eroded by half a tile. A plane's own
    region eroded by half a tile is never excluded, except within a line's core.
    """
    square_mitre = dict(cap_style=CAP_STYLE.square, join_style=JOIN_STYLE.mitre)
    eroded = [r.buffer(-tile_m / 2, **square_mitre) for r in regions]

    exclusions = [[] for _ in regions]
    additions = [[] for _ in regions]
    for i in range(len(regions)):
        for j in range(i + 1, len(regions)):
            exclusions[i].append((j, eroded[j]))
            exclusions[j].append((i, eroded[i]))
            line = lines.get((i, j))
            if line is not None:
                exclusions[i].append((j, line.zone.difference(line.side_1).difference(eroded[i])))
                exclusions[j].append((i, line.zone.intersection(line.side_1).difference(eroded[j])))
                additions[i].append(regions[j].intersection(line.zone).intersection(line.side_1))
                additions[j].append(regions[i].intersection(line.zone).difference(line.side_1))
                if line.core is not None:
                    exclusions[i].append((j, line.core.difference(line.side_1)))
                    exclusions[j].append((i, line.core.intersection(line.side_1)))
    return exclusions, additions


def _legal_area(building_shrunk, lines: Dict[Tuple[int, int], _MeetingLine],
                exclusions: List[Tuple[int, Polygon]], i: int):
    """
    The area plane `i` may occupy: the shrunk building minus its `exclusions`, except
    that within the core of a line it has with a neighbour, its own side of the line is
    excluded only by other planes.
    """
    legal = building_shrunk.difference(shapely.union_all([g for _, g in exclusions]))
    for (a, b), line in lines.items():
        if line.core is None or i not in (a, b):
            continue
        j = b if i == a else a
        own_side = line.core.intersection(line.side_1) if i == a else line.core.difference(line.side_1)
        others = shapely.union_all([g for k, g in exclusions if k != j])
        legal = legal.union(own_side.intersection(building_shrunk).difference(others))
    return legal


def _raw_regions(lines: Dict[Tuple[int, int], _MeetingLine], regions: list) -> list:
    """
    Each plane's pixels plus, within the core of each of its lines, the neighbour's
    pixels on its side: those fit the plane about as well, being so close to where the
    planes meet.
    """
    raw = list(regions)
    for (i, j), line in lines.items():
        if line.core is None:
            continue
        raw[i] = raw[i].union(regions[j].intersection(line.core).intersection(line.side_1))
        raw[j] = raw[j].union(regions[i].intersection(line.core).difference(line.side_1))
    return raw


def _gradient(plane: RoofPlane) -> np.ndarray:
    """
    The plane's (dz/dx, dz/dy), from slope and aspect. Aspect is snapped to the building's
    orientation where close, so lines where planes meet come out parallel to its walls.
    """
    aspect = math.radians(plane['aspect'])
    return -math.tan(math.radians(plane['slope'])) * np.array([math.sin(aspect), math.cos(aspect)])


def _split_overlaps(planes: List[RoofPolygon], lines: Dict[Tuple[int, int], _MeetingLine],
                    owned_xy: List[np.ndarray], tile_m: float) -> None:
    """
    Make the planes' polygons disjoint. Where two overlap, the overlap goes to each side
    of the line where they meet if they have one (see `_meeting_lines`), otherwise to the
    plane with the nearer pixel.

    All polygons are rebuilt from the faces of one noding of all their boundaries (plus the
    meeting lines), so neighbouring polygons share edges exactly; splitting them with
    separate overlay operations can leave slivers of overlap from floating-point error.
    """
    polys = np.empty(len(planes), dtype=object)
    polys[:] = [_polygonal(p['roof_geom_27700']) for p in planes]
    overlapping = set()
    edges = [p.boundary for p in polys]
    tree = shapely.STRtree(polys)
    for i, j in zip(*tree.query(polys, predicate='intersects')):
        if i >= j:
            continue
        overlap = _polygonal(polys[i].intersection(polys[j]))
        if overlap.area == 0:
            continue
        overlapping.add((i, j))
        line = lines.get((i, j))
        if line is not None:
            edges.append(line.side_1.exterior.intersection(overlap.envelope.buffer(tile_m)))

    if not overlapping:
        return

    faces = shapely.get_parts(shapely.polygonize(shapely.get_parts(shapely.union_all(edges, grid_size=_NODING_GRID_M))))
    points = shapely.point_on_surface(faces)
    covered_by = shapely.STRtree(points).query(polys, predicate='contains')

    face_owners = defaultdict(list)
    for plane_idx, face_idx in zip(*covered_by):
        face_owners[face_idx].append(plane_idx)

    owned_faces = [[] for _ in planes]
    for face_idx, owners in face_owners.items():
        # the pairwise rule isn't transitive, so the order of comparison matters
        owners = sorted(owners, key=lambda k: _order_key(owned_xy[k]))
        owner = owners[0]
        for other in owners[1:]:
            owner = _overlap_owner(min(owner, other), max(owner, other), lines, points[face_idx], owned_xy)
        owned_faces[owner].append(faces[face_idx])

    for plane, plane_faces in zip(planes, owned_faces):
        plane['roof_geom_27700'] = shapely.union_all(plane_faces) if plane_faces else Polygon()


def _overlap_owner(i: int, j: int, lines: Dict[Tuple[int, int], _MeetingLine], point,
                   owned_xy: List[np.ndarray]) -> int:
    line = lines.get((i, j))
    if line is not None and line.zone.contains(point):
        return i if line.side_1.contains(point) else j
    d_i, d_j = _nearest_dist(owned_xy[i], point), _nearest_dist(owned_xy[j], point)
    if math.isclose(d_i, d_j):
        # on a pixel grid, ties are common
        return i if _order_key(owned_xy[i]) <= _order_key(owned_xy[j]) else j
    return i if d_i < d_j else j


def _smooth(planes: List[RoofPolygon], lines: Dict[Tuple[int, int], _MeetingLine],
            building_shrunk, tile_m: float) -> None:
    """
    De-zigzag the planes' polygons, except along the building's edge and the lines
    between planes, which are already straight where they should be. With no
    `SMOOTH_TOLERANCE_TILES`, just drop near-collinear vertices, which a line clipped in
    pieces leaves along it.

    Each arc of the noded boundaries (between the polygons, and between them and the
    rest of the building) is smoothed once (see `_smooth_arc`), and the polygons rebuilt
    from the faces of the smoothed arcs, so that neighbours still share edges exactly.
    """
    polys = [_polygonal(p['roof_geom_27700']) for p in planes]
    if not polys:
        return
    fixed = shapely.union_all([building_shrunk.boundary] +
                              [line.side_1.exterior.intersection(line.zone) for line in lines.values()])
    shapely.prepare(fixed)
    noded = shapely.union_all([p.boundary for p in polys] + [building_shrunk.boundary], grid_size=_NODING_GRID_M)
    arcs = shapely.get_parts(shapely.line_merge(noded))
    smoothed = [shapely.linestrings(_smooth_arc(np.asarray(arc.coords), fixed, tile_m)) for arc in arcs]
    faces = shapely.get_parts(shapely.polygonize(shapely.get_parts(
        shapely.union_all(smoothed, grid_size=_NODING_GRID_M))))

    # each face goes to whichever of the polygons, or the rest of the building, covered
    # most of it before smoothing
    covered = shapely.union_all(polys)
    tree = shapely.STRtree(polys)
    owned_faces = [[] for _ in planes]
    for face in faces:
        best, best_area = None, face.area - face.intersection(covered).area
        for k in tree.query(face, predicate='intersects'):
            area = face.intersection(polys[k]).area
            if area > best_area:
                best, best_area = k, area
        if best is not None:
            owned_faces[best].append(face)

    for plane, plane_faces in zip(planes, owned_faces):
        plane['roof_geom_27700'] = shapely.union_all(plane_faces) if plane_faces else Polygon()


def _smooth_arc(coords: np.ndarray, fixed, tile_m: float) -> np.ndarray:
    """
    De-zigzag an arc: its segments along `fixed` lines or at least `SMOOTH_MIN_KEPT_TILES`
    long are kept, and between them, each segment is replaced by its midpoint, which
    straightens a staircase, and the result simplified to `SMOOTH_TOLERANCE_TILES`. The
    ends of an open arc stay put, as other arcs meet there.
    """
    n = len(coords) - 1
    if n < 2 or SMOOTH_TOLERANCE_TILES <= 0:
        return _drop_collinear(coords)
    closed = _same_point(coords[0], coords[-1])
    starts, ends = shapely.points(coords[:-1]), shapely.points(coords[1:])
    mids = (coords[1:] + coords[:-1]) / 2
    on_fixed = shapely.dwithin(fixed, shapely.points(mids), 1e-4) \
        & shapely.dwithin(fixed, starts, 1e-4) & shapely.dwithin(fixed, ends, 1e-4)
    seg_len = np.linalg.norm(coords[1:] - coords[:-1], axis=1)
    kept = on_fixed | (seg_len >= SMOOTH_MIN_KEPT_TILES * tile_m)
    if kept.all():
        return _drop_collinear(coords)

    pts, pinned = [], []

    def add(p, pin: bool):
        if pts and _same_point(pts[-1], p):
            pinned[-1] = pinned[-1] or pin
        else:
            pts.append(p)
            pinned.append(pin)

    if not closed:
        add(coords[0], True)
    for k in range(n):
        if kept[k]:
            add(coords[k], True)
            add(coords[k + 1], True)
        else:
            add(mids[k], False)
    if not closed:
        add(coords[-1], True)
    pts, pinned = np.array(pts), np.array(pinned)
    tolerance = SMOOTH_TOLERANCE_TILES * tile_m

    if closed:
        if _same_point(pts[0], pts[-1]):
            pts, pinned = pts[:-1], pinned[:-1]
        if not pinned.any():
            if len(pts) < 3:
                return _drop_collinear(coords)
            ring = shapely.simplify(shapely.linearrings(pts), tolerance)
            return np.asarray(ring.coords) if not ring.is_empty and len(ring.coords) >= 4 else _drop_collinear(coords)
        # start and end at a pinned point
        s = int(np.flatnonzero(pinned)[0])
        pts = np.vstack([pts[s:], pts[:s], pts[s:s + 1]])
        pinned = np.concatenate([pinned[s:], pinned[:s], [True]])

    out = [pts[0]]
    idx = np.flatnonzero(pinned)
    for a, b in zip(idx[:-1], idx[1:]):
        chain = pts[a:b + 1]
        if len(chain) > 2:
            chain = np.asarray(shapely.simplify(shapely.linestrings(chain), tolerance).coords)
        out.extend(chain[1:])
    return _drop_collinear(np.array(out))


def _drop_collinear(coords: np.ndarray) -> np.ndarray:
    """Drop vertices within 0.1mm of a straight line between their neighbours; keeps the ends."""
    simplified = np.asarray(shapely.simplify(shapely.linestrings(coords), 1e-4).coords)
    return simplified if len(simplified) >= 2 else coords


def _same_point(a: np.ndarray, b: np.ndarray) -> bool:
    # not np.allclose, whose relative tolerance is metres at BNG coordinates
    return bool(np.all(np.abs(a - b) < 1e-9))


def _polygonal(geom) -> MultiPolygon:
    """Just the polygons: overlays can also produce collapsed lines and points."""
    return MultiPolygon([g for g in shapely.get_parts(geom) if g.geom_type == 'Polygon'])


def _fit_line(normal: np.ndarray,
              pixels1_xy: np.ndarray,
              pixels2_xy: np.ndarray,
              shared_xy: np.ndarray,
              tile_m: float,
              search_tiles: float = 3) -> Optional[Tuple[np.ndarray, np.ndarray, float, float, int]]:
    """
    The line with unit normal `normal` that best separates two planes' pixels near the
    pixels they share, as (centre, normal, offset of the line from centre along the
    normal, +/-1 for which side of it plane 1 is on, number of pixels clearly on the wrong
    side). None if it separates them poorly.

    The offset is the 1D threshold along the normal that misclassifies fewest pixels.
    Only pixels alongside the shared pixels count (see `_alongside`).
    """
    centre = shared_xy.mean(axis=0)
    along_dir = np.array([-normal[1], normal[0]])
    a0, a1 = _alongside(centre, along_dir, shared_xy, tile_m)
    near = _pixel_region(shared_xy, tile_m).buffer(search_tiles * tile_m)

    def nearby(xy: np.ndarray) -> np.ndarray:
        along = (xy - centre) @ along_dir
        return xy[shapely.contains_xy(near, xy[:, 0], xy[:, 1]) & (along >= a0) & (along <= a1)]

    xy1, xy2 = nearby(pixels1_xy), nearby(pixels2_xy)

    s = np.concatenate([(xy1 - centre) @ normal, (xy2 - centre) @ normal])
    is1 = np.concatenate([np.ones(len(xy1), dtype=bool), np.zeros(len(xy2), dtype=bool)])
    order = np.argsort(s)
    s, is1 = s[order], is1[order]

    # errors if the threshold sits before index k, with plane 1 above: 1s below + 2s above
    ones_below = np.concatenate([[0], np.cumsum(is1)])
    twos_below = np.concatenate([[0], np.cumsum(~is1)])
    err_p1_above = ones_below + (twos_below[-1] - twos_below)
    err_p1_below = twos_below + (ones_below[-1] - ones_below)
    k_above, k_below = int(np.argmin(err_p1_above)), int(np.argmin(err_p1_below))
    k, direction = (k_above, 1.0) if err_p1_above[k_above] <= err_p1_below[k_below] else (k_below, -1.0)
    lo = s[k - 1] if k > 0 else s[0] - tile_m
    hi = s[k] if k < len(s) else s[-1] + tile_m
    offset = (lo + hi) / 2

    # A poor separation means the planes don't meet along a line here (e.g. a step).
    # Pixels within a tile of the line fit both planes about equally, so don't count.
    wrong_side = (direction * (s - offset) < 0) == is1
    clearly_wrong = wrong_side & (np.abs(s - offset) > tile_m)
    if np.count_nonzero(clearly_wrong & is1) > MEETING_MAX_MISCLASSIFIED * len(xy1) \
            or np.count_nonzero(clearly_wrong & ~is1) > MEETING_MAX_MISCLASSIFIED * len(xy2):
        return None
    return centre, normal, offset, direction, int(np.count_nonzero(clearly_wrong))


def _half_plane(centre: np.ndarray, normal: np.ndarray, offset: float, direction: float, zone) -> Polygon:
    """The half-plane on the `direction` side of the line, big enough to cover `zone`."""
    reach = 10 * max(zone.length, 1.0) + 100
    along = np.array([-normal[1], normal[0]]) * reach
    p0 = centre + offset * normal
    away = direction * normal * reach
    return Polygon([p0 - along, p0 + along, p0 + along + away, p0 - along + away])


def _nearest_dist(pixels_xy: np.ndarray, geom) -> float:
    return float(shapely.distance(geom, shapely.points(pixels_xy)).min())


def _set_usability(plane: RoofPolygon,
                   area_geom,
                   max_roof_slope_degrees: int,
                   min_roof_area_m: int,
                   min_roof_degrees_from_north: int) -> None:
    area = area_geom.area / math.cos(math.radians(plane['slope']))

    if plane['slope'] > max_roof_slope_degrees:
        plane['usable'] = False
        plane['not_usable_reason'] = "SLOPE"
    elif plane['aspect'] < min_roof_degrees_from_north:
        plane['usable'] = False
        plane['not_usable_reason'] = "ASPECT"
    elif plane['aspect'] > 360 - min_roof_degrees_from_north:
        plane['usable'] = False
        plane['not_usable_reason'] = "ASPECT"
    elif area < min_roof_area_m:
        plane['usable'] = False
        plane['not_usable_reason'] = "AREA"
    else:
        plane['usable'] = True


def _is_valid(plane: RoofPolygon) -> bool:
    return plane['roof_geom_27700'] \
           and not plane['roof_geom_27700'].is_empty \
           and plane['roof_geom_raw_27700'] \
           and not plane['roof_geom_raw_27700'].is_empty


def _make_polygon(plane: RoofPlane,
                  building_geom: Polygon,
                  min_dist_to_edge_m: float,
                  resolution_metres: float,
                  max_area_diff: float = 5,
                  max_area_diff_pct: float = 0.35,
                  pixels_xyz: Optional[np.ndarray] = None) -> Tuple[Optional[Polygon], Optional[Polygon]]:
    # Make the initial roof polygon (basically by just drawing round
    # all the pixels and then de-zigzagging)
    halfr = resolution_metres / 2
    r = resolution_metres
    pixels = [square(xy[0] - halfr, xy[1] - halfr, r) for xy in plane['inliers_xy']]
    geom = ops.unary_union(pixels)
    geom = de_zigzag(geom)
    raw_roof_poly = largest_polygon(geom)
    raw_roof_poly = _constrain_to_building(building_geom, raw_roof_poly, min_dist_to_edge_m)

    if not raw_roof_poly or raw_roof_poly.is_empty:
        return None, None

    # If a bbox rotated to match the aspect is close enough area-wise to the initial
    # polygon, just use that:
    rotated: Polygon = affinity.rotate(raw_roof_poly, plane['aspect'], origin=raw_roof_poly.centroid)
    bbox = rotated.envelope
    area_diff = bbox.area - rotated.area
    if area_diff < max_area_diff and area_diff / rotated.area < max_area_diff_pct:
        roof_poly = affinity.rotate(bbox, -plane['aspect'], origin=raw_roof_poly.centroid)
    else:
        # Otherwise grid it based on a grid oriented to the aspect and then de-zigzag
        roof_poly = _grid_polygon(raw_roof_poly, plane['aspect'], grid_size=1.0)
        roof_poly = de_zigzag(roof_poly)

    roof_poly = _constrain_to_building(building_geom, roof_poly, min_dist_to_edge_m)
    if pixels_xyz is not None and roof_poly is not None:
        roof_poly = _carve_off_plane(plane, roof_poly, pixels_xyz, resolution_metres)

    return largest_polygon(raw_roof_poly), largest_polygon(roof_poly)


# As roof detection's inlier thresholds:
_CARVE_THRESHOLD = 0.25
_FLAT_CARVE_THRESHOLD = 0.1
_MERGE_MAX_MAE = ROOFDET_GOOD_SCORE


def _carve_off_plane(plane: RoofPolygon, poly, pixels_xyz: np.ndarray, resolution_metres: float) -> Optional[Polygon]:
    """
    `poly` without the pixels inside it that are neither the plane's own nor
    within its residual threshold of it. Growing and squaring up a polygon otherwise lets
    it spill onto neighbouring surfaces: other roof faces, walls, the ground, plant.
    """
    x, y, z = pixels_xyz[:, 0], pixels_xyz[:, 1], pixels_xyz[:, 2]
    inside = shapely.contains_xy(poly, x, y)
    if not inside.any():
        return poly
    residual = np.abs(z - (plane['x_coef'] * x + plane['y_coef'] * y + plane['intercept']))
    threshold = _FLAT_CARVE_THRESHOLD if plane['is_flat'] else _CARVE_THRESHOLD
    off = inside & (residual > threshold)
    if not off.any():
        return poly
    own = shapely.contains_xy(_pixel_region(plane['inliers_xy'], resolution_metres).buffer(-resolution_metres / 4), x, y)
    off &= ~own
    if not off.any():
        return poly
    carved = poly.difference(_pixel_region(pixels_xyz[off, :2], resolution_metres))
    return largest_polygon(set_precision(carved, 0.01)) if not carved.is_empty else None


def _grid_polygon(roof_poly: Polygon, aspect: float, grid_size: float):
    # TODO maybe this should require that the grid cell intersection with roof_poly
    #      is over a certain amount?
    centroid = roof_poly.centroid

    # Rotate the roof area CCW by aspect, to be gridded easily:
    plane_points = affinity.rotate(roof_poly, aspect, origin=centroid)

    # grid_size = math.sqrt(resolution_metres ** 2 * 2.0)
    grid = get_grid_cells(plane_points, grid_size, grid_size, 0, 0, grid_start='bounds')
    grid = affinity.rotate(MultiPolygon(grid), -aspect, origin=centroid).geoms
    roof_poly = ops.unary_union(grid)
    roof_poly = make_valid(roof_poly)
    return roof_poly


def _constrain_to_building(building_geom: Polygon,
                           roof_poly: Polygon,
                           min_dist_to_edge_m: float) -> Optional[Polygon]:
    if not roof_poly or roof_poly.is_empty:
        return None

    neg_buffer = -min_dist_to_edge_m
    building_geom_shrunk = building_geom.buffer(
        neg_buffer, cap_style=CAP_STYLE.square, join_style=JOIN_STYLE.mitre)
    roof_poly = roof_poly.intersection(building_geom_shrunk)
    roof_poly = largest_polygon(roof_poly)
    return roof_poly


# def remove_tendrils(roof_poly: Polygon, buf: float = 0.6) -> Optional[Polygon]:
#     splitter = roof_poly.buffer(-buf, cap_style=CAP_STYLE.square, join_style=JOIN_STYLE.mitre, resolution=1)
#     splitter = splitter.buffer(buf, cap_style=CAP_STYLE.square, join_style=JOIN_STYLE.mitre, resolution=1)
#     splitter = largest_polygon(splitter).exterior
#     roof_poly = largest_polygon(split_with(roof_poly, splitter))
#     return roof_poly


def _merge_touching(planes: List[RoofPolygon],
                    building_geom: Polygon,
                    min_dist_to_edge_m: float,
                    resolution_metres: float,
                    max_slope_diff: int = 4,
                    pixels_xyz: Optional[np.ndarray] = None) -> List[RoofPolygon]:
    """
    Merge any planes that
    a) have the same aspect
    b) have a slope that differs less than `max_slope_diff`
    c) intersect each other
    d) are not flat
    e) if `pixels_xyz` is given, are one surface: a plane fitted to both their pixels
       fits them to within `_MERGE_MAX_MAE` (else e.g. parallel roofs either side of a
       step would merge). The merged plane is then refitted to them.
    """
    z_at = _z_lookup(pixels_xyz) if pixels_xyz is not None else None
    merged = []
    _by_aspect = defaultdict(list)
    for plane in planes:
        aspect = plane['aspect']
        _by_aspect[aspect].append(plane)

    for plane_group in _by_aspect.values():
        if len(plane_group) == 1:
            merged.append(plane_group[0])
            continue

        checked = set()
        for plane in plane_group:
            if id(plane) in checked:
                continue
            checked.add(id(plane))
            if plane['is_flat']:
                if _is_valid(plane):
                    merged.append(plane)
                continue

            slope = plane['slope']
            poly = plane['roof_geom_27700']
            mergeable = []
            for p in plane_group:
                if id(p) not in checked \
                        and p['is_flat'] is False \
                        and abs(p['slope'] - slope) <= max_slope_diff \
                        and p['roof_geom_27700'].intersects(poly) \
                        and (z_at is None or _coplanar(plane, p, z_at)):
                    mergeable.append(p)

            if len(mergeable) > 0:
                for to_merge in mergeable:
                    checked.add(id(to_merge))

                plane['inliers_xy'] = np.concatenate([p['inliers_xy'] for p in mergeable] + [plane['inliers_xy']])
                if z_at is not None:
                    _refit(plane, z_at)
                raw_roof_poly, roof_poly = _make_polygon(plane, building_geom, min_dist_to_edge_m, resolution_metres,
                                                         pixels_xyz=pixels_xyz)
                plane['roof_geom_27700'] = roof_poly
                plane['roof_geom_raw_27700'] = raw_roof_poly

            if _is_valid(plane):
                merged.append(plane)

    return merged


def _z_lookup(pixels_xyz: np.ndarray):
    z_by_xy = {(x, y): z for x, y, z in pixels_xyz}
    return lambda xy: np.array([z_by_xy[(x, y)] for x, y in xy])


def _lstsq_plane(xy: np.ndarray, z: np.ndarray) -> Tuple[np.ndarray, float]:
    centre = xy.mean(axis=0)
    a = np.column_stack([xy - centre, np.ones(len(xy))])
    coef, *_ = np.linalg.lstsq(a, z, rcond=None)
    mae = float(np.abs(z - a @ coef).mean())
    return np.array([coef[0], coef[1], coef[2] - coef[0] * centre[0] - coef[1] * centre[1]]), mae


def _coplanar(p1: RoofPolygon, p2: RoofPolygon, z_at) -> bool:
    xy = np.concatenate([p1['inliers_xy'], p2['inliers_xy']])
    _, mae = _lstsq_plane(xy, z_at(xy))
    return mae <= _MERGE_MAX_MAE


def _refit(plane: RoofPolygon, z_at) -> None:
    (a, b, c), mae = _lstsq_plane(plane['inliers_xy'], z_at(plane['inliers_xy']))
    plane['x_coef'], plane['y_coef'], plane['intercept'] = a, b, c
    plane['slope'] = slope_deg(a, b)
    plane['aspect_raw'] = aspect_deg(a, b)
    plane['panel_slope'] = plane['slope']


def _remove_overlaps(roof_polygons: List[RoofPolygon], debug: bool = False) -> None:
    # TODO maybe have an rtree here?
    for i, rp1 in enumerate(roof_polygons):
        for j, rp2 in enumerate(roof_polygons):
            if i > j:
                p1, p2 = split_evenly(rp1['roof_geom_27700'], rp2['roof_geom_27700'], debug=debug)
                rp1['roof_geom_27700'] = p1
                rp2['roof_geom_27700'] = p2


def _to_test_data(building_id: str, planes: List[dict], building_geom: Polygon) -> dict:
    planes_ = []
    for plane in planes:
        plane = plane.copy()
        plane["inliers_xy"] = list(map(tuple, plane["inliers_xy"]))
        if "inliers" in plane:
            del plane["inliers"]
        planes_.append(plane)

    return {
        "planes": planes_,
        "building_geom": building_geom.wkt,
        "building_id": building_id,
    }


def _write_test_data(test_data):
    """Write test data for building in the format that the roof polygon tests expect"""
    debug_data_dir = os.environ.get("DEBUG_DATA_DIR")
    if debug_data_dir:
        fname = join(debug_data_dir, f"roof_poly_{test_data['building_id']}.json")
        with open(fname, 'w') as f:
            json.dump(test_data, f, sort_keys=True, default=str)
        print(f"Wrote debug data to {fname}")
    else:
        print(json.dumps(test_data, sort_keys=True, default=str))
