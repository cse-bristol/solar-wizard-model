# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import json
import os
from os.path import join

import time
import traceback
from collections import defaultdict
from dataclasses import dataclass, field
from itertools import combinations
from typing import Dict, List, Optional, cast, Tuple

import math
import numpy as np
import shapely
from shapely.geometry import Polygon, CAP_STYLE, JOIN_STYLE, MultiPolygon, LineString, Point

from solar_pv.geos import largest_polygon
from solar_pv.datatypes import RoofPlane, RoofPolygon
from solar_pv.roof_detection import rejections
from solar_pv.roof_polygons.occupancy import tile_polygon

# Noding nearly coincident lines (a meeting line along the edge two polygons share) in
# floating point can leave a gap in a ring, so that polygonizing merges the faces either
# side of it.
_NODING_GRID_M = 1e-6

# Module constants, so that `fast_roof_detection --set` can tune them.
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
# the two planes owns what (see `_MeetingLine.core`).
CORE_HALF_WIDTH_TILES = 2
# Lines around a junction of three planes run to a node, where all three are at the same
# height (see `_join_at_nodes`), if it moves no more than `NODE_MAX_SENSITIVITY_TILES`
# when a plane's aspect is 5° or its slope 2° out, and each line needs moving no more
# than `NODE_MAX_SHIFT_TILES` sideways, and its end no more than `NODE_MAX_PAST_END_TILES`
# past, or `NODE_MAX_SHORT_TILES` short of, the shared pixels' end.
NODE_MAX_SENSITIVITY_TILES = 1
NODE_MAX_SHIFT_TILES = 1
NODE_MAX_PAST_END_TILES = 3
NODE_MAX_SHORT_TILES = 1
# A line whose shared pixels end this near the building's edge runs to it (see
# `_extend_to_walls`); further for a line where the planes meet, e.g. a hip, whose pixels
# often stop well short of the corner it runs to.
WALL_MAX_TILES = 1.5
MEETING_WALL_MAX_TILES = 3
# De-zigzag tolerance (see `_smooth`).
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
                         rejected: Optional[List[dict]] = None) -> List[RoofPolygon]:
    """
    Add roof polygons and other related fields to the dicts in `planes`, and return those
    that got one. Planes that end up with no polygon are recorded in `rejected` (see
    `rejections`).
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
        rejected=rejected)
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
                          rejected: Optional[List[dict]] = None) -> List[RoofPolygon]:

    if len(planes) == 0:
        return []

    try:
        polygons = _make_roof_polygons(building_geom,
                                       planes,
                                       max_roof_slope_degrees=max_roof_slope_degrees,
                                       min_roof_area_m=min_roof_area_m,
                                       min_roof_degrees_from_north=min_roof_degrees_from_north,
                                       flat_roof_degrees=flat_roof_degrees,
                                       min_dist_to_edge_m=min_dist_to_edge_m,
                                       resolution_metres=resolution_metres)
    except Exception as e:
        building_id = planes[0]['building_id']
        print(f"Exception during roof polygon creation for building_id {building_id}:")
        traceback.print_exception(e)
        _write_test_data(_to_test_data(planes[0]['building_id'], planes, building_geom))
        raise e

    kept = {id(p) for p in polygons}
    for plane in planes:
        if id(plane) not in kept:
            rejections.record(rejected, plane, rejections.NO_POLYGON)
    return polygons


def _make_roof_polygons(building_geom: Polygon,
                        planes: List[RoofPlane],
                        max_roof_slope_degrees: int,
                        min_roof_area_m: int,
                        min_roof_degrees_from_north: int,
                        flat_roof_degrees: int,
                        min_dist_to_edge_m: float,
                        resolution_metres: float) -> List[RoofPolygon]:
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

    # Planes aren't merged here: that's for roof detection, which judges it by the fit of
    # the merged plane.
    regions = [_pixel_region(p['inliers_xy'], resolution_metres) for p in plane_polys]
    owned_xy = [p['inliers_xy'] for p in plane_polys]

    lines = _meeting_lines(plane_polys, owned_xy, regions, building_shrunk, tile_m)
    exclusions, additions = _exclusions(lines, regions, tile_m)
    raw_regions = _raw_regions(lines, regions)
    for i, plane in enumerate(plane_polys):
        region = shapely.union_all([regions[i]] + additions[i])
        legal = _legal_area(building_shrunk, lines, exclusions[i], i)

        tiled = tile_polygon(region, legal, plane['aspect'], region.centroid, tile_m, PERIMETER_LAMBDA)
        roof_poly = tiled.intersection(legal) if tiled is not None else region.intersection(legal)
        # Tiles along a line at an angle to the plane's grid can leave notches along it,
        # which de-zigzagging turns into gaps between the planes.
        own_cores = _polygonal(region.intersection(legal)).intersection(_own_cores(lines, i))
        roof_poly = _polygonal(_polygonal(roof_poly).union(_polygonal(own_cores)))

        plane['roof_geom_raw_27700'] = raw_regions[i]
        plane['roof_geom_27700'] = roof_poly
    _fill_nodes(plane_polys, lines, building_shrunk, tile_m)

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
    what, whichever plane the pixels were assigned to. `rays`: for each end at a node (see
    `_join_at_nodes`), the node, and the unit vector and distance from it along the line.
    """
    side_1: Polygon
    zone: Polygon
    core: Polygon
    rays: List[Tuple[np.ndarray, np.ndarray, float]] = field(default_factory=list)


def _meeting_lines(planes: List[RoofPlane], owned_xy: List[np.ndarray], regions: list, building_shrunk,
                   tile_m: float, zone_tiles: float = 2) -> Dict[Tuple[int, int], _MeetingLine]:
    """
    For pairs of planes (i, j), i < j, that share an edge running along a line between
    them: that line. It's the line where the two planes meet (see `_intersection_line`)
    if that follows the shared edge, else a line along one of the
    planes' grid axes (see `_step_line`).

    Only the pixels of each plane edge-adjacent to the other count as shared, so planes
    separated by unassigned pixels, or touching only at a corner, share nothing.

    A line applies alongside the shared pixels and `LINE_END_TILES` past their ends, except
    that it runs to a node where it meets other lines at a junction (see `_join_at_nodes`),
    or else to a wall that it nearly reaches (see `_extend_to_walls`).
    """
    lines, shared = _find_lines(planes, owned_xy, regions, tile_m)
    _join_at_nodes(lines, shared, planes, owned_xy, building_shrunk, tile_m)
    _extend_to_walls(lines, owned_xy, regions, building_shrunk, tile_m, zone_tiles)
    restricted = {pair: _restrict(line, tile_m, zone_tiles) for pair, line in lines.items()}
    _clip_at_nodes(restricted)
    return restricted


@dataclass
class _Line:
    """
    A line between planes i < j, through `point` with unit `normal`, plane i being on its
    `direction` side. It applies from `start` to `end` along it (see `_Line.along`),
    relative to `point`. `meets`: it's where the planes meet, rather than a step line.
    `at_node`: whether its start and end are at nodes (see `_join_at_nodes`).
    """
    point: np.ndarray
    normal: np.ndarray
    direction: float
    shared_xy: np.ndarray
    start: float
    end: float
    meets: bool
    at_node: List[bool] = field(default_factory=lambda: [False, False])

    @property
    def along(self) -> np.ndarray:
        return np.array([-self.normal[1], self.normal[0]])

    def at(self, along: float) -> np.ndarray:
        return self.point + along * self.along

    def pixel_range(self) -> Tuple[float, float]:
        a = (self.shared_xy - self.point) @ self.along
        return float(a.min()), float(a.max())

    def move(self, point: np.ndarray, normal: np.ndarray) -> None:
        """Move the line, keeping where along it its ends are."""
        start, end = self.at(self.start), self.at(self.end)
        if normal @ self.normal < 0:
            normal = -normal
        self.point, self.normal = point, normal
        self.start, self.end = (start - point) @ self.along, (end - point) @ self.along


def _find_lines(planes: List[RoofPlane], owned_xy: List[np.ndarray], regions: list,
                tile_m: float) -> Tuple[Dict[Tuple[int, int], _Line], Dict[Tuple[int, int], np.ndarray]]:
    """The lines between pairs of planes, and every touching pair's shared pixels."""
    gradients = [_gradient(p) for p in planes]
    adjacent_dist = tile_m / 2 + 1e-6
    lines = {}
    shared_by_pair = {}
    for i in range(len(regions)):
        for j in range(i + 1, len(regions)):
            if regions[i].distance(regions[j]) > 1e-6:
                continue
            shared_i = owned_xy[i][shapely.dwithin(regions[j], shapely.points(owned_xy[i]), adjacent_dist)]
            shared_j = owned_xy[j][shapely.dwithin(regions[i], shapely.points(owned_xy[j]), adjacent_dist)]
            shared_by_pair[(i, j)] = shared = np.concatenate([shared_i, shared_j])
            if len(shared_i) == 0 or len(shared_j) == 0:
                continue

            # fit the pair the same way round whatever order the planes are in, as ties
            # in the fit are broken by which plane is first
            first, second = (i, j) if _order_key(owned_xy[i]) <= _order_key(owned_xy[j]) else (j, i)
            line = _intersection_line(planes[first], planes[second], gradients[first], gradients[second],
                                      owned_xy[first], owned_xy[second], shared, tile_m)
            meets = line is not None
            if line is None:
                line = _step_line(planes[first], planes[second], owned_xy[first], owned_xy[second], shared, tile_m)
            if line is None:
                continue
            centre, normal, offset, direction = line
            if first != i:
                direction = -direction
            start, end = _alongside(centre, np.array([-normal[1], normal[0]]), shared, tile_m)
            lines[(i, j)] = _Line(point=centre + offset * normal, normal=normal, direction=direction,
                                  shared_xy=shared, start=start, end=end, meets=meets)
    return lines, shared_by_pair


def _join_at_nodes(lines: Dict[Tuple[int, int], _Line], shared_by_pair: Dict[Tuple[int, int], np.ndarray],
                   planes: List[RoofPlane], owned_xy: List[np.ndarray], building_shrunk, tile_m: float) -> None:
    """
    Make the lines around a junction of three planes run to one point, the node, rather
    than each ending near its own shared pixels' end: so their polygons meet there, in a
    clean Y, instead of leaving slivers and clusters of vertices between them.

    The node is where all three planes are at the same height (see `_triple_point`). It's
    placed where two of the three pairs meet along lines (not step lines) whose ends are
    near it, and the third pair touches too. If the third pair's planes touch along an
    edge too short for a line of their own, but meet along their intersection from the
    node, that line is added (see `_line_from_node`), or between the two nodes they meet
    at (see `_line_between_nodes`). Each line is moved to pass through its nodes, keeping
    its direction (both its nodes lie on it, as both are on the planes' intersection).
    Where four or more planes meet, the nodes that share a line's end are merged.

    The lines' offsets otherwise still come from separating the planes' pixels (see
    `_fit_line`): only lines that end at a node take their offset from the planes' heights.
    """
    heights = [_mean_height(p) for p in planes]
    if any(h is None for h in heights):
        return
    gradients = [_gradient(p) for p in planes]
    origin = np.mean([m for m, _ in heights], axis=0)

    meeting = defaultdict(set)
    for (i, j), line in lines.items():
        if line.meets:
            meeting[i].add(j)
            meeting[j].add(i)
    triples = sorted({tuple(sorted((i, j, k))) for i in meeting for j, k in combinations(sorted(meeting[i]), 2)})

    claims = []
    added = {}
    unjoined = defaultdict(list)
    for triple in triples:
        pairs = list(combinations(triple, 2))
        meets = [p for p in pairs if p in lines and lines[p].meets]
        other = [p for p in pairs if p not in meets]
        if other and other[0] not in shared_by_pair:
            continue
        node = _triple_point(planes, gradients, heights, origin, meets[:2], tile_m)
        if node is None or not building_shrunk.buffer(tile_m / 2).contains(Point(node)):
            continue
        ends = [_node_end(lines[p], node, tile_m) for p in meets]
        if any(e is None for e in ends):
            continue
        claim = [(p, e) for p, e in zip(meets, ends)]
        if other and other[0] in lines:
            end = _node_end(lines[other[0]], node, tile_m)
            if end is not None:
                claim.append((other[0], end))
        elif other and len(shared_by_pair[other[0]]) > 0:
            line = added.get(other[0]) or _line_from_node(
                node, other[0], planes, gradients, owned_xy, shared_by_pair[other[0]], tile_m)
            if line is not None:
                added[other[0]] = line
                claim.append((other[0], 0))
            else:
                unjoined[other[0]].append(len(claims))
        claims.append((node, claim))
    # a pair whose planes meet at two nodes, e.g. at either end of a short ridge between hips
    for pair, cs in unjoined.items():
        if pair in added or len(cs) != 2:
            continue
        line = _line_between_nodes(claims[cs[0]][0], claims[cs[1]][0], pair, owned_xy, shared_by_pair[pair], tile_m)
        if line is not None:
            added[pair] = line
            claims[cs[0]][1].append((pair, 0))
            claims[cs[1]][1].append((pair, 1))
    lines.update(added)

    # merge nodes that share a line's end
    parent = list(range(len(claims)))

    def root(a):
        while parent[a] != a:
            parent[a] = parent[parent[a]]
            a = parent[a]
        return a

    first_claim = {}
    for c, (_, claim) in enumerate(claims):
        for line_end in claim:
            if line_end in first_claim:
                parent[root(c)] = root(first_claim[line_end])
            else:
                first_claim[line_end] = c
    groups = defaultdict(list)
    for c in range(len(claims)):
        groups[root(c)].append(c)

    node_ends = defaultdict(dict)
    for members in groups.values():
        node = np.mean([claims[c][0] for c in members], axis=0)
        if max(np.linalg.norm(claims[c][0] - node) for c in members) > NODE_MAX_SHIFT_TILES * tile_m:
            continue
        for c in members:
            for pair, end in claims[c][1]:
                node_ends[pair][end] = node

    for pair, ends in node_ends.items():
        line = lines[pair]
        if len(ends) == 2:
            direction = ends[1] - ends[0]
            length = np.linalg.norm(direction)
            if length < tile_m:
                continue
            normal = np.array([direction[1], -direction[0]]) / length
            if _line_angle_diff(math.degrees(math.atan2(*normal)), math.degrees(math.atan2(*line.normal))) > 5:
                continue
            line.move(ends[0], normal)
        else:
            (end, node), = ends.items()
            line.move(node, line.normal)
        for end, node in ends.items():
            along = float((node - line.point) @ line.along)
            if end == 0:
                line.start = along
            else:
                line.end = along
            line.at_node[end] = True
        if line.end - line.start < tile_m / 2:
            del lines[pair]


def _mean_height(plane: RoofPlane) -> Optional[Tuple[np.ndarray, float]]:
    """The plane's pixels' mean position, and its height there."""
    if plane.get('x_coef') is None or plane.get('intercept') is None:
        return None
    m = plane['inliers_xy'].mean(axis=0)
    return m, float(plane['x_coef'] * m[0] + plane['y_coef'] * m[1] + plane['intercept'])


def _plane_offset(gradients, heights, origin, i: int, j: int) -> Tuple[np.ndarray, float]:
    """(a, b): planes i and j are at the same height where a . (xy - origin) = b."""
    (mi, zi), (mj, zj) = heights[i], heights[j]
    return gradients[i] - gradients[j], zj - zi + gradients[i] @ (mi - origin) - gradients[j] @ (mj - origin)


def _triple_point(planes: List[RoofPlane], gradients: List[np.ndarray], heights, origin: np.ndarray,
                  pairs: List[Tuple[int, int]], tile_m: float) -> Optional[np.ndarray]:
    """
    Where three planes, those in `pairs`, are at the same height. Each plane is taken with
    its snapped gradient (see `_gradient`), as the lines are, through its pixels' mean
    height. None if it moves more than `NODE_MAX_SENSITIVITY_TILES` when a plane's aspect
    is 5° or its slope 2° out, as it does when two of the lines are near-parallel.
    """
    def solve(gs):
        rows = [_plane_offset(gs, heights, origin, i, j) for i, j in pairs]
        a = np.array([r[0] for r in rows])
        if abs(np.linalg.det(a)) < 1e-9:
            return None
        return origin + np.linalg.solve(a, np.array([r[1] for r in rows]))

    node = solve(gradients)
    if node is None:
        return None
    for k in {k for pair in pairs for k in pair}:
        plane = planes[k]
        for aspect_error, slope_error in ((5, 0), (-5, 0), (0, 2), (0, -2)):
            gs = list(gradients)
            gs[k] = _gradient({'aspect': plane['aspect'] + aspect_error,
                               'slope': max(plane['slope'] + slope_error, 0)})
            moved = solve(gs)
            if moved is None or np.linalg.norm(moved - node) > NODE_MAX_SENSITIVITY_TILES * tile_m:
                return None
    return node


def _node_end(line: _Line, node: np.ndarray, tile_m: float) -> Optional[int]:
    """Which end of the line (0: start, 1: end) the node is at, or None if it's too far from either."""
    if abs((node - line.point) @ line.normal) > NODE_MAX_SHIFT_TILES * tile_m:
        return None
    t = (node - line.point) @ line.along
    lo, hi = line.pixel_range()
    end, past = (0, lo - t) if lo - t > t - hi else (1, t - hi)
    if -NODE_MAX_SHORT_TILES * tile_m <= past <= NODE_MAX_PAST_END_TILES * tile_m:
        return end
    return None


def _line_from_node(node: np.ndarray, pair: Tuple[int, int], planes: List[RoofPlane], gradients: List[np.ndarray],
                    owned_xy: List[np.ndarray], shared_xy: np.ndarray, tile_m: float) -> Optional[_Line]:
    """
    For two planes that touch along an edge too short for a line of their own (see
    `_follows_shared_edge`), but meet at a node: the line where they meet, starting at the
    node, if its direction is stable (see `_direction_sensitivity`) and each plane owns
    most of the pixels on its side of it. Planes at a step fail this.
    """
    i, j = pair
    normal = gradients[i] - gradients[j]
    norm = np.linalg.norm(normal)
    if norm < 0.05 or _direction_sensitivity(planes[i], planes[j]) > MAX_DIRECTION_SENSITIVITY_DEG:
        return None
    normal = normal / norm
    along_dir = np.array([-normal[1], normal[0]])
    along = (shared_xy - node) @ along_dir
    if abs(along.mean()) < 1e-9:
        return None
    if along.mean() < 0:
        normal, along_dir, along = -normal, -along_dir, -along
    if along.min() > NODE_MAX_PAST_END_TILES * tile_m \
            or np.mean(np.abs((shared_xy - node) @ normal) <= MEETING_MAX_LINE_DIST_TILES * tile_m) < MEETING_MIN_ON_LINE:
        return None
    end = along.max() + LINE_END_TILES * tile_m

    # Each plane must own most of the pixels on its side of the line, near it: two planes
    # that touch only across the node, with other planes between them either side, don't.
    s, owner = [], []
    for k, xy in enumerate(owned_xy):
        a, d = (xy - node) @ along_dir, (xy - node) @ normal
        near = (a >= 0) & (a <= max(end, 2 * tile_m)) & (np.abs(d) <= 2 * tile_m)
        s.append(d[near])
        owner.append(np.full(np.count_nonzero(near), k))
    s, owner = np.concatenate(s), np.concatenate(owner)
    direction = 1.0 if np.sum(s[owner == i] > 0) >= np.sum(s[owner == i] < 0) else -1.0
    for k, side in ((i, direction), (j, -direction)):
        on_side = side * s > 0
        if np.count_nonzero(owner[on_side] == k) <= np.count_nonzero(on_side) / 2:
            return None
    return _Line(point=node, normal=normal, direction=direction, shared_xy=shared_xy, start=0.0, end=end,
                 meets=True, at_node=[True, False])


def _line_between_nodes(node1: np.ndarray, node2: np.ndarray, pair: Tuple[int, int], owned_xy: List[np.ndarray],
                        shared_xy: np.ndarray, tile_m: float) -> Optional[_Line]:
    """
    For two planes that touch along an edge too short for a line of their own (see
    `_follows_shared_edge`), but meet at two nodes: the line between the nodes, if their
    shared pixels lie along it. Both nodes are where the planes are at the same height, so
    the line is their intersection.
    """
    if tuple(node2) < tuple(node1):
        node1, node2 = node2, node1
    length = np.linalg.norm(node2 - node1)
    if length < tile_m:
        return None
    along_dir = (node2 - node1) / length
    normal = np.array([along_dir[1], -along_dir[0]])
    along, dist = (shared_xy - node1) @ along_dir, (shared_xy - node1) @ normal
    if along.min() < -tile_m or along.max() > length + tile_m \
            or np.mean(np.abs(dist) <= MEETING_MAX_LINE_DIST_TILES * tile_m) < MEETING_MIN_ON_LINE:
        return None
    i, j = pair
    side_i, side_j = np.mean((owned_xy[i] - node1) @ normal), np.mean((owned_xy[j] - node1) @ normal)
    if side_i * side_j >= 0:
        return None
    return _Line(point=node1, normal=normal, direction=float(np.sign(side_i)), shared_xy=shared_xy,
                 start=0.0, end=float(length), meets=True, at_node=[True, True])


def _extend_to_walls(lines: Dict[Tuple[int, int], _Line], owned_xy: List[np.ndarray], regions: list,
                     building_shrunk, tile_m: float, zone_tiles: float) -> None:
    """
    Run each line whose shared pixels end within `WALL_MAX_TILES` (or, for lines where the
    planes meet, `MEETING_WALL_MAX_TILES`) of the building's edge, with no other plane's
    pixels in the way and neither plane's on the other's side of it (see
    `_wrong_side_past_end`), to the edge and `zone_tiles` past it (but
    not back into the building), e.g. a ridge to a gable wall, where the pixels by the
    wall often belong to no plane.
    """
    boundary = building_shrunk.boundary
    eroded = [r.buffer(-tile_m / 4) for r in regions]
    for (i, j), line in lines.items():
        lo, hi = line.pixel_range()
        max_tiles = MEETING_WALL_MAX_TILES if line.meets else WALL_MAX_TILES
        for end, pixel_end, sign in ((0, lo, -1.0), (1, hi, 1.0)):
            if line.at_node[end]:
                continue
            from_xy = line.at(pixel_end)
            if not building_shrunk.contains(Point(from_xy)):
                continue
            ray = LineString([from_xy, line.at(pixel_end + sign * (max_tiles + zone_tiles) * tile_m)])
            hits = sorted(abs(float((xy - from_xy) @ line.along))
                          for xy in shapely.get_coordinates(ray.intersection(boundary)))
            if not hits or hits[0] > max_tiles * tile_m:
                continue
            reach = min([hits[0] + zone_tiles * tile_m] + hits[1:2])
            in_the_way = LineString([from_xy, line.at(pixel_end + sign * hits[0])])
            if any(k not in (i, j) and in_the_way.intersects(r) for k, r in enumerate(eroded)):
                continue
            if _wrong_side_past_end(line, owned_xy[i], owned_xy[j], pixel_end, sign, hits[0], tile_m, zone_tiles):
                continue
            if end == 0:
                line.start = min(line.start, pixel_end - reach)
            else:
                line.end = max(line.end, pixel_end + reach)


def _wrong_side_past_end(line: _Line, pixels1_xy: np.ndarray, pixels2_xy: np.ndarray, pixel_end: float,
                         sign: float, length: float, tile_m: float, half_width_tiles: float) -> bool:
    """
    Whether, alongside the line past its shared pixels' end (`pixel_end`, `sign` being
    which way), for `length`, more than one, and more than `MEETING_MAX_MISCLASSIFIED`, of
    either plane's pixels are clearly on the other's side of it: e.g. one plane carries on
    across where the line would run.
    """
    for xy, side in ((pixels1_xy, line.direction), (pixels2_xy, -line.direction)):
        a = sign * ((xy - line.point) @ line.along - pixel_end)
        d = side * ((xy - line.point) @ line.normal)
        near = (a > 0) & (a <= length) & (np.abs(d) <= half_width_tiles * tile_m)
        # a single stray pixel, e.g. at an eaves corner, doesn't count
        if np.count_nonzero(near & (d < -tile_m / 2)) > max(1.0, MEETING_MAX_MISCLASSIFIED * np.count_nonzero(near)):
            return True
    return False


def _restrict(line: _Line, tile_m: float, zone_tiles: float) -> _MeetingLine:
    """The `_MeetingLine` for a line: where it applies, and the half-plane on plane i's side."""
    near = [_pixel_region(line.shared_xy, tile_m)]
    # past the shared pixels, the zone follows the line
    lo, hi = line.pixel_range()
    if line.start < lo - LINE_END_TILES * tile_m - 1e-6:
        near.append(LineString([line.at(line.start), line.at(lo)]))
    if line.end > hi + LINE_END_TILES * tile_m + 1e-6:
        near.append(LineString([line.at(hi), line.at(line.end)]))
    zone = shapely.union_all(near).buffer(
        zone_tiles * tile_m, cap_style=CAP_STYLE.square, join_style=JOIN_STYLE.mitre)
    # trimmed only along the line: across it, the zone reaches as far as the buffer does
    zone = zone.intersection(_strip(line, zone.length))
    core = _strip(line, CORE_HALF_WIDTH_TILES * tile_m)
    rays = []
    if line.at_node[0]:
        rays.append((line.at(line.start), line.along, line.end - line.start))
    if line.at_node[1]:
        rays.append((line.at(line.end), -line.along, line.end - line.start))
    return _MeetingLine(side_1=_half_plane(line.point, line.normal, 0.0, line.direction, zone),
                        zone=zone, core=core, rays=rays)


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


def _strip(line: _Line, half_width_m: float) -> Polygon:
    """The strip `half_width_m` either side of the line, from its start to its end."""
    w = half_width_m * line.normal
    p0, p1 = line.at(line.start), line.at(line.end)
    return Polygon([p0 - w, p1 - w, p1 + w, p0 + w])


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
    legal = _polygonal(building_shrunk.difference(_polygonal(shapely.union_all([g for _, g in exclusions]))))
    for (a, b), line in lines.items():
        if i not in (a, b):
            continue
        j = b if i == a else a
        own_side = _polygonal(line.core.intersection(line.side_1) if i == a else line.core.difference(line.side_1))
        others = _polygonal(shapely.union_all([g for k, g in exclusions if k != j]))
        legal = _polygonal(legal.union(_polygonal(own_side.intersection(building_shrunk).difference(others))))
    return legal


def _own_cores(lines: Dict[Tuple[int, int], _MeetingLine], i: int):
    """The parts of the cores of plane `i`'s lines on its side of them."""
    parts = [line.core.intersection(line.side_1) if i == a else line.core.difference(line.side_1)
             for (a, b), line in lines.items() if i in (a, b)]
    return _polygonal(shapely.union_all(parts)) if parts else Polygon()


def _raw_regions(lines: Dict[Tuple[int, int], _MeetingLine], regions: list) -> list:
    """
    Each plane's pixels plus, within the core of each of its lines, the neighbour's
    pixels on its side: those fit the plane about as well, being so close to where the
    planes meet.
    """
    raw = list(regions)
    for (i, j), line in lines.items():
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


def _rays_at_nodes(lines: Dict[Tuple[int, int], _MeetingLine]) -> list:
    """For each node, its rays (see `_MeetingLine.rays`) as (angle, length, pair), by angle."""
    rays_at = defaultdict(list)
    for pair, line in lines.items():
        for node, direction, length in line.rays:
            rays_at[tuple(node)].append((math.atan2(direction[1], direction[0]), length, pair))
    return [(np.array(node), sorted(rays)) for node, rays in rays_at.items()]


def _sector(node: np.ndarray, angle: float, sweep: float, radius: float) -> Polygon:
    """The sector of the disc about `node` from `angle` anticlockwise through `sweep`."""
    steps = max(int(math.ceil(sweep / (math.pi / 4))), 1)
    angles = angle + sweep * np.arange(steps + 1) / steps
    return Polygon([node] + [node + radius * np.array([math.cos(a), math.sin(a)]) for a in angles])


def _clip_at_nodes(lines: Dict[Tuple[int, int], _MeetingLine]) -> None:
    """
    Near a node, as far as the lines next to it there reach, a line applies only in the
    sectors either side of it, up to those lines, or up to its own extension past the node,
    whichever comes first. Its zone would otherwise end square across it at the node, and
    so reach into the sectors beyond the lines next to it; and in a sector wider than 180°,
    the part past its extension is on the other plane's side of it.
    """
    for node, rays in _rays_at_nodes(lines):
        if len(rays) < 2:
            continue
        for k, (angle, length, pair) in enumerate(rays):
            prev_angle, prev_length, _ = rays[k - 1]
            next_angle, next_length, _ = rays[(k + 1) % len(rays)]
            to_next = min((next_angle - angle) % (2 * math.pi) or 2 * math.pi, math.pi)
            to_prev = min((angle - prev_angle) % (2 * math.pi) or 2 * math.pi, math.pi)
            sweep = 2 * math.pi - to_next - to_prev
            if sweep < 1e-6:
                continue
            far = _sector(node, angle + to_next, sweep, min(prev_length, next_length))
            line = lines[pair]
            line.zone = _polygonal(line.zone.difference(far))
            line.core = _polygonal(line.core.difference(far))


def _fill_nodes(planes: List[RoofPolygon], lines: Dict[Tuple[int, int], _MeetingLine],
                building_shrunk, tile_m: float, radius_tiles: float = 1) -> None:
    """
    Around each node (see `_join_at_nodes`), give each plane the sector between its two
    lines there, out to `radius_tiles` (or the nearer of the lines' other ends), and take
    it from every other plane. Tiling drops the tip of a narrow sector, so the polygons
    would otherwise stop short of the node.
    """
    for node, rays in _rays_at_nodes(lines):
        if len(rays) < 2:
            continue
        for (a0, len0, pair0), (a1, len1, pair1) in zip(rays, rays[1:] + rays[:1]):
            common = set(pair0) & set(pair1)
            if len(common) != 1:
                continue
            i = common.pop()
            sweep = (a1 - a0) % (2 * math.pi)
            if sweep < 1e-6:
                continue
            r = min(radius_tiles * tile_m, len0, len1)
            mid = node + r / 2 * np.array([math.cos(a0 + sweep / 2), math.sin(a0 + sweep / 2)])
            if not all(_on_side(lines[pair], pair, i, mid) for pair in (pair0, pair1)):
                continue
            sector = _sector(node, a0, sweep, r).intersection(building_shrunk)
            # a sector apart from the plane's polygon would be dropped with it (see `largest_polygon`)
            own = planes[i].get('roof_geom_27700')
            if own is None or own.intersection(sector).area < 1e-6:
                continue
            for k, plane in enumerate(planes):
                geom = plane.get('roof_geom_27700')
                if geom is None:
                    continue
                if k == i:
                    plane['roof_geom_27700'] = _polygonal(geom.union(sector))
                elif geom.intersects(sector):
                    plane['roof_geom_27700'] = _polygonal(geom.difference(sector))


def _on_side(line: _MeetingLine, pair: Tuple[int, int], k: int, xy: np.ndarray) -> bool:
    """Whether `xy` is on plane `k`'s side of the line between the planes in `pair`."""
    return line.side_1.contains(Point(xy)) == (k == pair[0])


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
    between planes, which are already straight where they should be.

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
    if n < 2:
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
