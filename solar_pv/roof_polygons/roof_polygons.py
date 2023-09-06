# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import json
import time
import traceback
from collections import defaultdict
from typing import List, Dict, Optional, cast, Tuple

import math
import numpy as np
from networkx import Graph, cycle_basis
from psycopg2.sql import Identifier
import psycopg2.extras
from shapely import wkt, affinity, ops, set_precision, MultiLineString, Polygon, \
    LineString, Point
from shapely.geometry import LineString, Polygon, CAP_STYLE, JOIN_STYLE, MultiPoint, \
    MultiPolygon, Point
from shapely.prepared import prep
from shapely.strtree import STRtree
from shapely.validation import make_valid

from solar_pv import tables
from solar_pv.db_funcs import connection, sql_command
from solar_pv.geos import azimuth_deg, square, largest_polygon, get_grid_cells, \
    de_zigzag, multi, densify_polygon, geoms
from solar_pv.constants import FLAT_ROOF_DEGREES_THRESHOLD
from solar_pv.types import RoofPlane, RoofPolygon


def create_roof_polygons(pg_uri: str,
                         job_id: int,
                         toid: str,
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
    building_geoms = _building_geoms(pg_uri, job_id, [toid])
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

    roof_polygons: List[RoofPolygon] = []

    if len(planes) == 0:
        return []

    # building_aspect = max(planes, key=lambda p: len(p['inliers_xy']))['aspect_adjusted']
    plane_polys = []
    for plane in planes:
        # TODO move all this non-polygon related stuff up into roofdet
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

            roof_poly = _make_polygon(plane, resolution_metres)

            roof_poly = _constrain_to_building(building_geom,
                                               roof_poly,
                                               large_building_threshold,
                                               min_dist_to_edge_large_m,
                                               min_dist_to_edge_m)

            if roof_poly and not roof_poly.is_empty:
                plane['roof_poly'] = roof_poly
                plane_polys.append(plane)

        except Exception as e:
            traceback.print_exception(e)
            print(json.dumps(_to_test_data(toid, [plane], building_geom),
                             sort_keys=True, default=str))
            raise e

    plane_polys = _merge_touching(plane_polys, resolution_metres)

    # TODO is this needed?
    for p in plane_polys:
        p['roof_poly'] = set_precision(p['roof_poly'], 0.01)
    _remove_overlaps(plane_polys)

    for plane in plane_polys:
        toid = plane['toid']
        building_geom = building_geoms[toid]
        try:
            roof_poly = plane['roof_poly']

            # Constrain a second time in case _merge_touching caused
            # it to re-overflow
            roof_poly = _constrain_to_building(building_geom,
                                               roof_poly,
                                               large_building_threshold,
                                               min_dist_to_edge_large_m,
                                               min_dist_to_edge_m)

            if not roof_poly or roof_poly.is_empty:
                continue

            del plane['roof_poly']

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


def _make_polygon(plane: RoofPlane, resolution_metres: float,
                  max_area_diff: float = 5,
                  max_area_diff_pct: float = 0.35) -> Optional[Polygon]:
    # Make the initial roof polygon (basically by just drawing round
    # all the pixels and then de-zigzagging)
    halfr = resolution_metres / 2
    r = resolution_metres
    pixels = [square(xy[0] - halfr, xy[1] - halfr, r) for xy in plane['inliers_xy']]
    geom = ops.unary_union(pixels)
    geom = de_zigzag(geom)
    roof_poly = largest_polygon(geom)

    if not roof_poly or roof_poly.is_empty:
        return None

    # If a bbox rotated to match the aspect is close enough area-wise to the initial
    # polygon, just use that:
    rotated: Polygon = affinity.rotate(roof_poly, plane['aspect'], origin=roof_poly.centroid)
    bbox = rotated.envelope
    area_diff = bbox.area - rotated.area
    if area_diff < max_area_diff and area_diff / rotated.area < max_area_diff_pct:
        return affinity.rotate(bbox, -plane['aspect'], origin=roof_poly.centroid)

    # Otherwise grid it based on a grid oriented to the aspect and then de-zigzag
    roof_poly = _grid_polygon(roof_poly, plane['aspect'], grid_size=1.0)
    roof_poly = de_zigzag(roof_poly)
    return roof_poly


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


# def remove_tendrils(roof_poly: Polygon, buf: float = 0.6) -> Optional[Polygon]:
#     splitter = roof_poly.buffer(-buf, cap_style=CAP_STYLE.square, join_style=JOIN_STYLE.mitre, resolution=1)
#     splitter = splitter.buffer(buf, cap_style=CAP_STYLE.square, join_style=JOIN_STYLE.mitre, resolution=1)
#     splitter = largest_polygon(splitter).exterior
#     roof_poly = largest_polygon(split_with(roof_poly, splitter))
#     return roof_poly


def _merge_touching(planes, resolution_metres: float, max_slope_diff: int = 4) -> List[dict]:
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

            slope = plane['slope']
            poly = plane['roof_poly']
            mergeable = []
            for p in plane_group:
                if id(p) not in checked \
                        and p['is_flat'] is False \
                        and abs(p['slope'] - slope) <= max_slope_diff \
                        and p['roof_poly'].intersects(poly):
                    mergeable.append(p)

            if len(mergeable) > 0:
                for to_merge in mergeable:
                    checked.add(id(to_merge))

                plane['roof_poly'] = ops.unary_union([p['roof_poly'] for p in mergeable] + [poly])
                plane['inliers_xy'] = np.concatenate([p['inliers_xy'] for p in mergeable] + [plane['inliers_xy']])
                plane['roof_poly'] = _make_polygon(plane, resolution_metres)

            if plane['roof_poly']:
                merged.append(plane)

    return merged


def _remove_overlaps(roof_polygons: List[dict]) -> None:
    # TODO maybe have an rtree here?
    for i, rp1 in enumerate(roof_polygons):
        for j, rp2 in enumerate(roof_polygons):
            if i > j:
                p1, p2 = _split_evenly(rp1['roof_poly'], rp2['roof_poly'])
                rp1['roof_poly'] = p1
                rp2['roof_poly'] = p2


def _constrain_to_building(building_geom: Polygon,
                           roof_poly: Polygon,
                           large_building_threshold: float,
                           min_dist_to_edge_large_m: float,
                           min_dist_to_edge_m: float) -> Optional[Polygon]:
    if not roof_poly or roof_poly.is_empty:
        return None

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


def _split_evenly(p1: Polygon, p2: Polygon,
                  min_area: float = 0.01,
                  min_dist_between_planes: float = 0.1,
                  voronoi_point_density: float = 0.1) -> Tuple[Polygon, Polygon]:
    """
    Split 2 overlapping polygons evenly
    TODO more docs
    """
    overlap = p1.intersection(p2)
    if overlap is None or overlap.is_empty:
        return p1, p2

    overlap = multi(overlap)
    splitter = []
    for overlap_part in overlap.geoms:
        if overlap_part.geom_type != 'Polygon' or overlap_part.area < min_area:
            continue

        # Check if this is a simple overlap where the line between the 2 points
        # where the boundaries of p1 and p2 overlap is (nearly completely) contained
        # within the overlap
        straight_central_line = []
        for g in p1.boundary.intersection(p2.boundary).geoms:
            if not g.intersects(overlap_part):
                continue
            if g.geom_type == 'Point':
                straight_central_line.append(g)
            elif g.geom_type == 'LineString':
                straight_central_line.append(g.centroid)
            else:
                raise ValueError(f"Intersection of boundary of 2 roof planes was not point or linestring: was {g.geom_type}")

        straight_central_line = LineString(straight_central_line)
        if overlap_part.buffer(0.1).contains(straight_central_line):
            splitter.append(straight_central_line.buffer(min_dist_between_planes / 2,
                                                         cap_style=CAP_STYLE.square,
                                                         join_style=JOIN_STYLE.mitre,
                                                         resolution=1))
            continue

        # Not a simple overlap - has turns and so on.
        # Use the road centreline finding algorithm outlined in
        # https://proceedings.esri.com/library/userconf/proc96/TO400/PAP370/P370.HTM
        overlap_part = densify_polygon(overlap_part, voronoi_point_density)
        edges = geoms(ops.voronoi_diagram(overlap_part, edges=True))

        graph = Graph()
        for edge in edges:
            if overlap_part.contains(edge) or (edge.length <= voronoi_point_density and overlap_part.intersects(edge)):
                node1 = edge.coords[0]
                node2 = edge.coords[-1]
                graph.add_node(node1)
                graph.add_node(node2)
                graph.add_edge(node1, node2, geom=edge)

        if len(graph) == 0:
            continue

        # Will never finish if there are cycles in the graph - so break them randomly
        for cycle in cycle_basis(graph):
            n1 = cycle[0]
            n2 = cycle[1]
            graph.remove_edge(n1, n2)

        # Prune the voronoi edges back until we have a single string of line
        # segments which only has 2 ends (2 nodes with degree=1)
        candidate_edges = []
        degrees = dict(graph.degree)
        while max(degrees.values()) > 2:
            for node, degree in list(degrees.items()):
                if degree == 1:
                    for n1, n2, data in list(graph.edges(node, data=True)):
                        candidate_edges.append(data['geom'])
                        graph.remove_edge(n1, n2)
                        degrees[n1] -= 1
                        degrees[n2] -= 1
                    graph.remove_node(node)

        # Now there should be just 2 nodes with degree=1 (either end of the centre-line)
        deg1_nodes = 0
        for node, degree in graph.degree:
            if degree == 1:
                deg1_nodes += 1
        assert deg1_nodes == 2

        # Add back in any edges that do not create a fork (so we create a
        # string of line segments which only has 2 ends).
        # candidate_edges is treated as a stack as the edges have to be evaluated in
        # LIFO order so that we build out from the centre-line
        while len(candidate_edges) > 0:
            edge = candidate_edges.pop()
            node1 = edge.coords[0]
            node2 = edge.coords[-1]
            if (node1 in graph or node2 in graph) \
                    and (node1 not in graph or graph.degree(node1) < 2)\
                    and (node2 not in graph or graph.degree(node2) < 2):
                graph.add_edge(node1, node2, geom=edge)

        usable_edges = [e[2].get('geom') for e in graph.edges(data=True)]

        # There should still be just 2 nodes with degree=1
        # Find the end points of the line and extend them to touch the closest point
        # of the intersection between the boundaries of p1 and p2
        end_points = []
        for node, degree in list(graph.degree):
            if degree == 1:
                end_points.append(Point(node))
        assert len(end_points) == 2

        tp = MultiPoint(straight_central_line.coords)
        usable_edges.append(LineString([end_points[0], ops.nearest_points(end_points[0], tp)[1]]))
        usable_edges.append(LineString([end_points[1], ops.nearest_points(end_points[1], tp)[1]]))

        part_splitter = []
        for ls in geoms(ops.linemerge(usable_edges)):
            part_splitter.append(ls.simplify(1.0))
        part_splitter = MultiLineString(part_splitter)
        print(part_splitter.wkt)
        splitter.append(part_splitter.buffer(min_dist_between_planes / 2,
                                             cap_style=CAP_STYLE.square,
                                             join_style=JOIN_STYLE.mitre,
                                             resolution=1))

    if len(splitter) == 0:
        return p1, p2

    splitter = ops.unary_union(splitter)
    p1_new = largest_polygon(p1.difference(splitter))
    p2_new = largest_polygon(p2.difference(splitter))
    return p1_new, p2_new
