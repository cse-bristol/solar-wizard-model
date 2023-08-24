# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import json
import time
import traceback
from collections import defaultdict
from typing import List, Dict, Optional, cast

import math
import numpy as np
from psycopg2.sql import Identifier
import psycopg2.extras
from shapely import wkt, affinity, ops, set_precision, MultiLineString
from shapely.geometry import LineString, Polygon, CAP_STYLE, JOIN_STYLE, MultiPoint, \
    MultiPolygon, Point
from shapely.prepared import prep
from shapely.strtree import STRtree
from shapely.validation import make_valid

from solar_pv import tables
from solar_pv.db_funcs import connection, sql_command
from solar_pv.geos import azimuth_deg, square, largest_polygon, get_grid_cells, \
    de_zigzag, multi, split_evenly
from solar_pv.constants import FLAT_ROOF_DEGREES_THRESHOLD
from solar_pv.roof_polygons.roof_polygon_archetypes import construct_archetypes, \
    get_archetype
from solar_pv.types import RoofPlane, RoofPolygon


def create_roof_polygons(pg_uri: str,
                         job_id: int,
                         planes: List[RoofPlane],
                         max_roof_slope_degrees: int,
                         min_roof_area_m: int,
                         min_roof_degrees_from_north: int,
                         flat_roof_degrees: int,
                         large_building_threshold: float,
                         min_dist_to_edge_m: float,
                         min_dist_to_edge_large_m: float,
                         resolution_metres: float) -> List[RoofPolygon]:
    """Add roof polygons and other related fields to the dicts in `planes`"""
    toids = list({plane['toid'] for plane in planes})
    building_geoms = _building_geoms(pg_uri, job_id, toids)
    return _create_roof_polygons(
        building_geoms,
        planes,
        max_roof_slope_degrees=max_roof_slope_degrees,
        min_roof_area_m=min_roof_area_m,
        min_roof_degrees_from_north=min_roof_degrees_from_north,
        flat_roof_degrees=flat_roof_degrees,
        large_building_threshold=large_building_threshold,
        min_dist_to_edge_m=min_dist_to_edge_m,
        min_dist_to_edge_large_m=min_dist_to_edge_large_m,
        resolution_metres=resolution_metres)


def _create_roof_polygons(building_geoms: Dict[str, Polygon],
                          planes: List[RoofPlane],
                          max_roof_slope_degrees: int,
                          min_roof_area_m: int,
                          min_roof_degrees_from_north: int,
                          flat_roof_degrees: int,
                          large_building_threshold: float,
                          min_dist_to_edge_m: float,
                          min_dist_to_edge_large_m: float,
                          resolution_metres: float) -> List[RoofPolygon]:
    polygons_by_toid = defaultdict(list)
    roof_polygons: List[RoofPolygon] = []
    # TODO these will have to be based on a different size system, not panel w/h
    # archetypes = construct_archetypes(0.99, 1.64)
    # max_archetype_area = max(archetypes, key=lambda a: a.polygon.area).polygon.area

    # Sort planes so that southerly aspects are considered first
    # (as already-created polygons take priority when ensuring two roof planes don't overlap)
    planes.sort(key=lambda p: (p['toid'], abs(180 - p['aspect_adjusted'])))

    plane_polys = []
    for plane in planes:
        plane = cast(dict, plane)
        toid = plane['toid']
        plane['aspect_raw'] = plane['aspect']
        plane['aspect'] = plane['aspect_adjusted']
        del plane['aspect_adjusted']
        building_geom = building_geoms[toid]
        try:
            # set is_flat, update slope of flat roofs
            is_flat = plane['slope'] <= FLAT_ROOF_DEGREES_THRESHOLD
            plane['is_flat'] = is_flat
            if is_flat:
                plane['slope'] = flat_roof_degrees

            roof_poly = _initial_polygon(plane, resolution_metres)
            if not roof_poly or roof_poly.is_empty:
                continue

            roof_poly = _grid_polygon(roof_poly, plane['aspect'], grid_size=1.0)
            roof_poly = de_zigzag(roof_poly)

            roof_poly = _constrain_to_building(building_geom,
                                               roof_poly,
                                               large_building_threshold,
                                               min_dist_to_edge_large_m,
                                               min_dist_to_edge_m)

            if not roof_poly or roof_poly.is_empty:
                continue

            plane['roof_poly'] = roof_poly
            plane_polys.append(plane)

        except Exception as e:
            traceback.print_exception(e)
            print(json.dumps(_to_test_data(toid, [plane], building_geom),
                             sort_keys=True, default=str))
            raise e

    plane_polys = _merge_touching(plane_polys, resolution_metres)
    for p in plane_polys:
        p['roof_poly'] = set_precision(p['roof_poly'], 0.01)
    _remove_overlaps(plane_polys)

    for plane in plane_polys:
        toid = plane['toid']
        building_geom = building_geoms[toid]
        try:
            is_flat = plane['is_flat']
            roof_poly = plane['roof_poly']
            del plane['roof_poly']

            # roof_poly = _grid_polygon(roof_poly, plane['aspect'], resolution_metres)
            # roof_poly = de_zigzag(roof_poly)
            #
            # roof_poly = _constrain_to_building(building_geom,
            #                                    roof_poly,
            #                                    large_building_threshold,
            #                                    min_dist_to_edge_large_m,
            #                                    min_dist_to_edge_m)
            # if not roof_poly or roof_poly.is_empty:
            #     continue

            # any other planes in the same toid will now not be allowed to overlap this one:
            # polygons_by_toid[toid].append(roof_poly)

            # Set usability:
            if plane['slope'] > max_roof_slope_degrees:
                plane['usable'] = False
                plane['not_usable_reason'] = "SLOPE"
            elif plane['aspect'] < min_roof_degrees_from_north:
                plane['usable'] = False
                plane['not_usable_reason'] = "ASPECT"
            elif plane['aspect'] > 360 - min_roof_degrees_from_north:
                plane['usable'] = False
                plane['not_usable_reason'] = "ASPECT"
            elif roof_poly.area < min_roof_area_m:
                plane['usable'] = False
                plane['not_usable_reason'] = "AREA"
            else:
                plane['usable'] = True

            # Add other info:
            plane['roof_geom_27700'] = roof_poly.wkt
            easting, northing = roof_poly.centroid.xy
            plane['easting'] = easting[0]
            plane['northing'] = northing[0]
            plane['raw_footprint'] = roof_poly.area
            plane['raw_area'] = roof_poly.area / math.cos(math.radians(plane['slope']))
            roof_polygons.append(plane)
        except Exception as e:
            traceback.print_exception(e)
            print(json.dumps(_to_test_data(toid, [plane], building_geom),
                             sort_keys=True, default=str))
            raise e

    return roof_polygons


def _initial_polygon(plane: RoofPlane, resolution_metres: float) -> Polygon:
    """
    Make the initial roof polygon (basically by just drawing round
    all the pixels and then de-zigzagging)
    """
    halfr = resolution_metres / 2
    r = resolution_metres
    pixels = [square(xy[0] - halfr, xy[1] - halfr, r) for xy in plane['inliers_xy']]
    geom = ops.unary_union(pixels)
    geom = de_zigzag(geom)
    geom = geom.buffer(math.sqrt(resolution_metres / 2) / 4,
                       cap_style=CAP_STYLE.square,
                       join_style=JOIN_STYLE.mitre,
                       resolution=1)
    return largest_polygon(geom)


def _grid_polygon(roof_poly: Polygon, aspect: float, grid_size: float):
    centroid = roof_poly.centroid

    # Rotate the roof area CCW by aspect, to be gridded easily:
    plane_points = affinity.rotate(roof_poly, aspect, origin=centroid)

    # grid_size = math.sqrt(resolution_metres ** 2 * 2.0)
    grid = get_grid_cells(plane_points, grid_size, grid_size, 0, 0, grid_start='bounds')
    grid = affinity.rotate(MultiPolygon(grid), -aspect, origin=centroid).geoms
    roof_poly = ops.unary_union(grid)
    roof_poly = make_valid(roof_poly)
    return roof_poly


# def _closest_corner_to(bbox: Polygon, point: Point) -> Point:
#     """Return the corner of `bbox` closest to `point`"""
#     pass
#
#
# def _furthest_corner_from(bbox: Polygon, point: Point) -> Point:
#     """Return the corner of `bbox` furthest from `point`"""
#     pass
#
#
# def _split_using_point(poly: Polygon, point: Point) -> List[Polygon]:
#     """
#     Split a polygon using lines drawn horizontally and vertically that
#     pass through a point
#     """
#     pass
#
#
# def _bbox_regularisation(poly: Polygon, aspect: float, min_area: float) -> Polygon:
#     centroid = poly.centroid
#     rotated: Polygon = affinity.rotate(poly, aspect, origin=centroid)
#
#     bbox = rotated.envelope
#     diffs = multi(bbox.difference(rotated))
#     for diff in diffs.geoms:
#         if diff.area >= min_area:
#             diff_bbox = diff.envelope
#             p1 = _closest_corner_to(diff_bbox, centroid)
#             # Use diff_bbox.length as the line length as that's guaranteed to be long
#             # enough to extend out of the roof poly
#             # TODO this will only work in 1 direction... need to move away from centroid
#             p2 = affinity.translate(diff_bbox.length, diff_bbox.length)
#             l = LineString([p1.coords[0], p2.coords[0]])
#             # TODO there could be multiple intersections... which do I want? closest? furthest?
#             new_bboxes = _split_using_point(diff_bbox, l.intersection(poly.boundary))
#             # 1 bbox will be (mostly) interior to poly, don't need
#             # 1 should be (mostly) exterior, and 2 should (mostly) intersect
#
#
#     return affinity.rotate(bbox, -aspect, origin=centroid)


# def remove_tendrils(roof_poly: Polygon, buf: float = 0.6) -> Optional[Polygon]:
#     splitter = roof_poly.buffer(-buf, cap_style=CAP_STYLE.square, join_style=JOIN_STYLE.mitre, resolution=1)
#     splitter = splitter.buffer(buf, cap_style=CAP_STYLE.square, join_style=JOIN_STYLE.mitre, resolution=1)
#     splitter = largest_polygon(splitter).exterior
#     roof_poly = largest_polygon(split_with(roof_poly, splitter))
#     return roof_poly
#
#
# def split_with(poly, splitter):
#     """Split a Polygon with a LineString or LinearRing"""
#
#     union = poly.boundary.union(splitter)
#     poly = prep(poly)
#     return MultiPolygon([pg for pg in ops.polygonize(union) if poly.contains(pg.representative_point())])


def _merge_touching(planes, resolution_metres: float, max_slope_diff: int = 4) -> List[dict]:
    """
    Merge any planes that
    a) have the same aspect
    b) have a slope that differs less than `max_slope_diff`
    c) intersect each other
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

            slope = plane['slope']
            poly = plane['roof_poly']
            mergeable = [p for p in plane_group if id(p) not in checked and abs(p['slope'] - slope) <= max_slope_diff and p['roof_poly'].intersects(poly)]

            for to_merge in mergeable:
                checked.add(id(to_merge))

            plane['roof_poly'] = ops.unary_union([p['roof_poly'] for p in mergeable] + [poly])
            plane['inliers_xy'] = np.concatenate([p['inliers_xy'] for p in mergeable] + [plane['inliers_xy']])
            plane['roof_poly'] = _initial_polygon(plane, resolution_metres)

            merged.append(plane)

    return merged


def _remove_overlaps(roof_polygons: List[dict]) -> None:
    # TODO maybe have an rtree here?
    for rp1 in roof_polygons:
        for rp2 in roof_polygons:
            if rp1['toid'] == rp2['toid'] and rp1['plane_id'] != rp2['plane_id']:
                p1, p2 = split_evenly(rp1['roof_poly'], rp2['roof_poly'])
                rp1['roof_poly'] = p1
                rp2['roof_poly'] = p2
                # overlap = p1.intersection(p2)
                # if overlap is None or overlap.is_empty:
                #     continue
                #
                # overlap = multi(overlap)
                # # split_overlap_parts = []
                # # splitter = []
                # for overlap_part in overlap.geoms:
                #     if overlap_part.geom_type != 'Polygon' or overlap_part.area < 0.0001:
                #         continue
                #     points = multi(overlap_part.exterior.intersection(p1.boundary))
                #     points = [p.centroid if p.geom_type != 'Point' else p for p in points.geoms]
                #     if len(points) < 2:
                #         continue
                #     # points.insert(len(points) // 2, overlap_part.centroid)
                #     splitter = LineString(points)
                #     p1 = largest_polygon(ops.split(p1, splitter))
                #     p2 = largest_polygon(ops.split(p2, splitter))
                #     # for part in multi(ops.split(overlap_part, splitter)).geoms:
                #     #     if part.geom_type == 'Polygon':
                #     #         split_overlap_parts.append(part)
                #
                # rp1['roof_poly'] = p1
                # rp2['roof_poly'] = p2
                # overlap_parts = []
                # for poly in intersection.geoms:
                #     if poly.geom_type != 'Polygon' or poly.area < 0.0001:
                #         continue
                #     centroid = poly.centroid
                #     plane_points = affinity.rotate(poly, rp1['aspect'], origin=centroid)
                #     grid = get_grid_cells(plane_points, 0.25, 0.25, 0, 0, grid_start='bounds')
                #     grid = affinity.rotate(MultiPolygon(grid), -rp1['aspect'], origin=centroid).geoms
                #     for cell in grid:
                #         overlap_parts.append(cell.intersection(poly).buffer(0.05))
                #

                # p1_new = p1.difference(p2)
                # p2_new = p2.difference(p1)
                # p1_parts = [p1_new]
                # p2_parts = [p2_new]
                # for poly in split_overlap_parts:
                #     # TODO constant
                #     poly = set_precision(poly, 0.01)
                #     p1_dist = poly.centroid.distance(p1_new)
                #     p2_dist = poly.centroid.distance(p2_new)
                #     if p1_dist <= p2_dist:
                #         p1_parts.append(poly)
                #     else:
                #         p2_parts.append(poly)
                #
                # rp1['roof_poly'] = largest_polygon(ops.unary_union(p1_parts))
                # rp2['roof_poly'] = largest_polygon(ops.unary_union(p2_parts))


# def _remove_overlaps(toid: str, roof_poly: Polygon, polygons_by_toid: Dict[str, List[Polygon]]):
#     intersecting_polys = [p for p in polygons_by_toid[toid] if p.intersects(roof_poly)]
#     if len(intersecting_polys) > 0:
#         other_polys = ops.unary_union(intersecting_polys).buffer(0.1,
#                                                                  cap_style=CAP_STYLE.square,
#                                                                  join_style=JOIN_STYLE.mitre,
#                                                                  resolution=1)
#         roof_poly = roof_poly.difference(other_polys)
#         roof_poly = largest_polygon(roof_poly)
#     roof_poly = make_valid(roof_poly)
#     return roof_poly


def _constrain_to_building(building_geom: Polygon,
                           roof_poly: Polygon,
                           large_building_threshold: float,
                           min_dist_to_edge_large_m: float,
                           min_dist_to_edge_m: float):
    if building_geom.area < large_building_threshold:
        neg_buffer = -min_dist_to_edge_m
    else:
        neg_buffer = -min_dist_to_edge_large_m
    building_geom_shrunk = building_geom.buffer(
        neg_buffer, cap_style=CAP_STYLE.square, join_style=JOIN_STYLE.mitre)
    roof_poly = roof_poly.intersection(building_geom_shrunk)
    roof_poly = largest_polygon(roof_poly)
    return roof_poly


# TODO already loading these in roofdet now - probably just make it a field on RoofPlane
def _building_geoms(pg_uri: str, job_id: int, toids: List[str]) -> Dict[str, Polygon]:
    with connection(pg_uri, cursor_factory=psycopg2.extras.DictCursor) as pg_conn:
        buildings = sql_command(
            pg_conn,
            """
            SELECT toid, ST_AsText(geom_27700) AS geom_27700 
            FROM {buildings}
            WHERE toid = ANY( %(toids)s )""",
            {"toids": toids},
            buildings=Identifier(tables.schema(job_id), tables.BUILDINGS_TABLE),
            result_extractor=lambda rows: rows)
        by_toid = {}
        for building in buildings:
            geom = wkt.loads(building['geom_27700'])
            by_toid[building['toid']] = geom
        return by_toid


def _to_test_data(toid: str, planes: List[dict], building_geom: Polygon) -> dict:
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
        "toid": toid,
    }
