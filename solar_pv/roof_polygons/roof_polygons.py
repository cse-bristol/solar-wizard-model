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
    de_zigzag
from solar_pv.datatypes import RoofPlane, RoofPolygon
from solar_pv.roof_polygons.occupancy import tile_polygon
from solar_pv.roof_polygons.split_evenly import split_evenly


def create_roof_polygons(building_id: str,
                         building_geom: Polygon,
                         planes: List[RoofPlane],
                         max_roof_slope_degrees: int,
                         min_roof_area_m: int,
                         min_roof_degrees_from_north: int,
                         flat_roof_degrees: int,
                         min_dist_to_edge_m: float,
                         resolution_metres: float,
                         method: str = "legacy") -> List[RoofPolygon]:
    """Add roof polygons and other related fields to the dicts in `planes`"""
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
        method=method)
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
                          debug: bool = False) -> List[RoofPolygon]:

    if len(planes) == 0:
        return []

    if method == "mincut":
        make_polygons = _create_roof_polygons_mincut
    elif method == "legacy":
        make_polygons = _create_roof_polygons_legacy
    else:
        raise ValueError(f"Unknown roof polygon method {method}")

    try:
        return make_polygons(building_geom,
                             planes,
                             max_roof_slope_degrees=max_roof_slope_degrees,
                             min_roof_area_m=min_roof_area_m,
                             min_roof_degrees_from_north=min_roof_degrees_from_north,
                             flat_roof_degrees=flat_roof_degrees,
                             min_dist_to_edge_m=min_dist_to_edge_m,
                             resolution_metres=resolution_metres,
                             debug=debug)
    except Exception as e:
        building_id = planes[0]['building_id']
        print(f"Exception during roof polygon creation for building_id {building_id}:")
        traceback.print_exception(e)
        _write_test_data(_to_test_data(planes[0]['building_id'], planes, building_geom))
        raise e


def _create_roof_polygons_legacy(building_geom: Polygon,
                                 planes: List[RoofPlane],
                                 max_roof_slope_degrees: int,
                                 min_roof_area_m: int,
                                 min_roof_degrees_from_north: int,
                                 flat_roof_degrees: int,
                                 min_dist_to_edge_m: float,
                                 resolution_metres: float,
                                 debug: bool = False) -> List[RoofPolygon]:
    plane_polys: List[RoofPolygon] = []
    for plane in planes:
        plane = cast(RoofPolygon, plane)
        plane['panel_slope'] = flat_roof_degrees if plane['is_flat'] else plane['slope']

        raw_roof_poly, roof_poly = _make_polygon(plane, building_geom, min_dist_to_edge_m, resolution_metres)

        if roof_poly and not roof_poly.is_empty and raw_roof_poly and not raw_roof_poly.is_empty:
            plane['roof_geom_27700'] = roof_poly
            plane['roof_geom_raw_27700'] = raw_roof_poly
            plane_polys.append(plane)

    if debug:
        print(f"Made {len(plane_polys)} initial roof polygons")

    plane_polys = _merge_touching(plane_polys, building_geom, min_dist_to_edge_m, resolution_metres)

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

        _set_usability(plane, max_roof_slope_degrees, min_roof_area_m, min_roof_degrees_from_north)

    return [p for p in plane_polys if _is_valid(p)]


def _create_roof_polygons_mincut(building_geom: Polygon,
                                 planes: List[RoofPlane],
                                 max_roof_slope_degrees: int,
                                 min_roof_area_m: int,
                                 min_roof_degrees_from_north: int,
                                 flat_roof_degrees: int,
                                 min_dist_to_edge_m: float,
                                 resolution_metres: float,
                                 perimeter_lambda: float = 0.3,
                                 debug: bool = False) -> List[RoofPolygon]:
    """
    Each plane's inlier pixels are tile-quantized by min-cut on a grid aligned to its own
    aspect, within a legal area: the shrunk building minus areas claimed by
    neighbouring planes (see `_exclusions`). Tiles straddling the legal edge are promoted
    and then re-clipped to it, so where two planes share an edge along the line where
    they meet (see `_meeting_lines`), their boundary comes out as that straight line.
    Any remaining overlaps are split the same way, or else by nearest pixel.

    `roof_geom_raw_27700` is the plane's pixels, as squares, within its polygon: its area
    is what the plane's area (and so its kWp and kWh) is based on.
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
    gradients = [_gradient(p) for p in plane_polys]

    lines = _meeting_lines(gradients, owned_xy, regions, tile_m)
    exclusions, additions = _exclusions(lines, regions, tile_m)
    for i, plane in enumerate(plane_polys):
        region = shapely.union_all([regions[i]] + additions[i])
        legal = building_shrunk.difference(shapely.union_all(exclusions[i]))

        tiled = tile_polygon(region, legal, plane['aspect'], region.centroid, tile_m, perimeter_lambda)
        roof_poly = tiled.intersection(legal) if tiled is not None else regions[i].intersection(legal)

        plane['roof_geom_raw_27700'] = regions[i]
        plane['roof_geom_27700'] = roof_poly

    valid = [i for i, p in enumerate(plane_polys) if _is_valid(p)]
    new_index = {old: new for new, old in enumerate(valid)}
    plane_polys = [plane_polys[i] for i in valid]
    valid_lines = {(new_index[i], new_index[j]): line for (i, j), line in lines.items()
                   if i in new_index and j in new_index}
    _split_overlaps(plane_polys, valid_lines, [owned_xy[i] for i in valid], tile_m)

    for plane in plane_polys:
        roof_poly = largest_polygon(plane['roof_geom_27700'])
        plane['roof_geom_27700'] = roof_poly
        if roof_poly is None or roof_poly.is_empty:
            continue
        plane['roof_geom_raw_27700'] = _polygonal(plane['roof_geom_raw_27700'].intersection(roof_poly))
        _set_usability(plane, max_roof_slope_degrees, min_roof_area_m, min_roof_degrees_from_north)

    return [p for p in plane_polys if _is_valid(p)]


def _pixel_region(pixels_xy: np.ndarray, resolution_metres: float):
    halfr = resolution_metres / 2
    x, y = pixels_xy[:, 0], pixels_xy[:, 1]
    return shapely.union_all(shapely.box(x - halfr, y - halfr, x + halfr, y + halfr)).simplify(0)


@dataclass
class _MeetingLine:
    """
    The line where two planes meet, restricted to the strip (`zone`) along the edge
    they share. `side_1` is the half-plane on plane 1's side of the line.
    """
    side_1: Polygon
    zone: Polygon


def _meeting_lines(gradients: List[np.ndarray], owned_xy: List[np.ndarray], regions: list, tile_m: float,
                   min_shared_tiles: float = 3,
                   max_line_dist_tiles: float = 1.5,
                   min_on_line: float = 0.9,
                   zone_tiles: float = 2) -> Dict[Tuple[int, int], _MeetingLine]:
    """
    For pairs of planes (i, j), i < j, that share an edge running along the line where
    the two planes meet: that line.

    Only the pixels of each plane edge-adjacent to the other count as shared, so planes
    separated by unassigned pixels, or touching only at a corner, share nothing. The line
    must follow the shared edge: the shared pixels must extend at least `min_shared_tiles`
    along it, and `min_on_line` of them must lie within `max_line_dist_tiles` of it.
    Planes that are adjacent without meeting along their intersection - e.g. side by side,
    at a step - fail this.
    """
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

            line = _fit_meeting_line(gradients[i], gradients[j], owned_xy[i], owned_xy[j], shared, tile_m)
            if line is None:
                continue
            centre, normal, offset, direction = line
            along = (shared - centre) @ np.array([-normal[1], normal[0]])
            dist = np.abs((shared - centre) @ normal - offset)
            if along.max() - along.min() < (min_shared_tiles - 1) * tile_m \
                    or np.mean(dist <= max_line_dist_tiles * tile_m) < min_on_line:
                continue

            zone = _pixel_region(shared, tile_m).buffer(
                zone_tiles * tile_m, cap_style=CAP_STYLE.square, join_style=JOIN_STYLE.mitre)
            lines[(i, j)] = _MeetingLine(side_1=_half_plane(centre, normal, offset, direction, zone), zone=zone)
    return lines


def _exclusions(lines: Dict[Tuple[int, int], _MeetingLine], regions: list, tile_m: float) -> Tuple[List[list], List[list]]:
    """
    For each plane, the areas it may not occupy, and areas to add to its region.

    Along an edge shared with a neighbour along the line where they meet, the line takes
    precedence over which plane owns the pixels: each plane is excluded from the
    neighbour's side of it and gets the neighbour's pixels on its own side. Elsewhere, a
    plane is excluded from other planes' regions eroded by half a tile. A plane's own
    region eroded by half a tile is never excluded.
    """
    square_mitre = dict(cap_style=CAP_STYLE.square, join_style=JOIN_STYLE.mitre)
    eroded = [r.buffer(-tile_m / 2, **square_mitre) for r in regions]

    exclusions = [[] for _ in regions]
    additions = [[] for _ in regions]
    for i in range(len(regions)):
        for j in range(i + 1, len(regions)):
            exclusions[i].append(eroded[j])
            exclusions[j].append(eroded[i])
            line = lines.get((i, j))
            if line is not None:
                exclusions[i].append(line.zone.difference(line.side_1).difference(eroded[i]))
                exclusions[j].append(line.zone.intersection(line.side_1).difference(eroded[j]))
                additions[i].append(regions[j].intersection(line.zone).intersection(line.side_1))
                additions[j].append(regions[i].intersection(line.zone).difference(line.side_1))
    return exclusions, additions


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

    faces = shapely.get_parts(shapely.polygonize(shapely.get_parts(shapely.union_all(edges))))
    points = shapely.point_on_surface(faces)
    covered_by = shapely.STRtree(points).query(polys, predicate='contains')

    face_owners = defaultdict(list)
    for plane_idx, face_idx in zip(*covered_by):
        face_owners[face_idx].append(plane_idx)

    owned_faces = [[] for _ in planes]
    for face_idx, owners in face_owners.items():
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
    return i if _nearest_dist(owned_xy[i], point) <= _nearest_dist(owned_xy[j], point) else j


def _polygonal(geom) -> MultiPolygon:
    """Just the polygons: overlays can also produce collapsed lines and points."""
    return MultiPolygon([g for g in shapely.get_parts(geom) if g.geom_type == 'Polygon'])


def _fit_meeting_line(gradient1: np.ndarray,
                      gradient2: np.ndarray,
                      pixels1_xy: np.ndarray,
                      pixels2_xy: np.ndarray,
                      shared_xy: np.ndarray,
                      tile_m: float,
                      min_gradient_diff: float = 0.05,
                      search_tiles: float = 3,
                      max_misclassified: float = 0.1) -> Optional[Tuple[np.ndarray, np.ndarray, float, float]]:
    """
    The line where two roof planes meet, near the pixels they share, as
    (centre, unit normal, offset of the line from centre along the normal, +/-1 for which
    side of it plane 1 is on). None if the planes are near-parallel, or the line separates
    their pixels near the shared ones poorly.

    The line's direction comes from the planes' gradients (it's perpendicular to their
    difference); its offset is the 1D threshold along that normal that best separates the
    two planes' nearby pixels.
    """
    normal = gradient1 - gradient2
    norm = np.linalg.norm(normal)
    if norm < min_gradient_diff:
        return None
    normal = normal / norm

    near = _pixel_region(shared_xy, tile_m).buffer(search_tiles * tile_m)
    xy1 = pixels1_xy[shapely.contains_xy(near, pixels1_xy[:, 0], pixels1_xy[:, 1])]
    xy2 = pixels2_xy[shapely.contains_xy(near, pixels2_xy[:, 0], pixels2_xy[:, 1])]

    centre = shared_xy.mean(axis=0)
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
    if np.count_nonzero(clearly_wrong & is1) > max_misclassified * len(xy1) \
            or np.count_nonzero(clearly_wrong & ~is1) > max_misclassified * len(xy2):
        return None
    return centre, normal, offset, direction


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
                   max_roof_slope_degrees: int,
                   min_roof_area_m: int,
                   min_roof_degrees_from_north: int) -> None:
    # Arbitrarily use the area of roof_geom_27700 rather than roof_geom_raw_27700...
    area = plane['roof_geom_27700'].area / math.cos(math.radians(plane['slope']))

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
                  max_area_diff_pct: float = 0.35) -> Tuple[Optional[Polygon], Optional[Polygon]]:
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

    return largest_polygon(raw_roof_poly), largest_polygon(roof_poly)


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
                    max_slope_diff: int = 4) -> List[RoofPolygon]:
    """
    Merge any planes that
    a) have the same aspect
    b) have a slope that differs less than `max_slope_diff`
    c) intersect each other
    d) are not flat
    """
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
                        and p['roof_geom_27700'].intersects(poly):
                    mergeable.append(p)

            if len(mergeable) > 0:
                for to_merge in mergeable:
                    checked.add(id(to_merge))

                plane['inliers_xy'] = np.concatenate([p['inliers_xy'] for p in mergeable] + [plane['inliers_xy']])
                raw_roof_poly, roof_poly = _make_polygon(plane, building_geom, min_dist_to_edge_m, resolution_metres)
                plane['roof_geom_27700'] = roof_poly
                plane['roof_geom_raw_27700'] = raw_roof_poly

            if _is_valid(plane):
                merged.append(plane)

    return merged


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
        fname = join(debug_data_dir, f"roof_poly_{test_data['building_id']}.json", 'w')
        with open(fname) as f:
            json.dump(test_data, f, sort_keys=True, default=str)
        print(f"Wrote debug data to {fname}")
    else:
        print(json.dumps(test_data, sort_keys=True, default=str))
