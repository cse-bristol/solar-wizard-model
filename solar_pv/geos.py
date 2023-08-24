# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import itertools
import json
from typing import List, Tuple, Union, cast

import math
import numpy as np
from shapely.geometry.base import BaseGeometry, BaseMultipartGeometry
from shapely import ops, affinity, set_precision, LinearRing
from shapely.prepared import prep
from shapely.strtree import STRtree
from shapely.geometry import Polygon, shape, MultiPolygon, mapping, LineString, \
    MultiPoint, MultiLineString, Point
from shapely import wkt, ops

from solar_pv.db_funcs import sql_command
from solar_pv.lidar.en_to_grid_ref import en_to_grid_ref, is_in_range
from solar_pv.util import round_down_to, round_up_to, frange


def rect(x: float, y: float, w: float, h: float) -> Polygon:
    return Polygon([(x, y),
                    (x, y + h),
                    (x + w, y + h),
                    (x + w, y),
                    (x, y)])


def square(x: float, y: float, edge: float) -> Polygon:
    return rect(x, y, edge, edge)


def from_geojson(geojson):
    if isinstance(geojson, str):
        geojson = json.loads(geojson)
    return shape(geojson)


def to_geojson_str(geom):
    if isinstance(geom, BaseGeometry):
        geom_dict = mapping(geom)
        geojson = json.dumps(geom_dict)
        return geojson
    return None


def to_geojson_dict(geom):
    if isinstance(geom, BaseGeometry):
        geom_dict = mapping(geom)
        return geom_dict
    return None


def from_geojson_file(geojson_file: str):
    with open(geojson_file) as f:
        return from_geojson(f.read())


def bounds_polygon(pg_conn, job_id: int) -> Polygon:
    """
    Returns a shapely polygon of the job bounds, which will be buffered
    by the horizon_search_distance if it's a PV job.
    """
    text = sql_command(
        pg_conn,
        """
        SELECT 
           ST_AsText(ST_Buffer(bounds, coalesce((params->>'horizon_search_radius')::int, 0))) AS bounds
        FROM models.job_queue
        WHERE job_id = %(job_id)s 
        """,
        bindings={"job_id": job_id},
        result_extractor=lambda res: res[0][0]
    )
    return wkt.loads(text)


def get_grid_cells(poly, cell_w, cell_h, spacing_w=0, spacing_h=0, grid_start: str = 'origin') -> List[Polygon]:
    """
    Get the cells of a grid that intersect with `poly` as polygons

    param `grid_start` == 'origin' means that the grid would intersect
    (0,0) if extended (useful for making OS grid refs). Otherwise
    if `grid_start` == 'bounds' grid starts at (xmin, ymin) of poly.
    """
    xmin, ymin, xmax, ymax = poly.bounds
    if grid_start == 'origin':
        xmin = round_down_to(xmin, cell_w + spacing_w)
        ymin = round_down_to(ymin, cell_h + spacing_h)
        xmax = round_up_to(xmax, cell_w + spacing_w)
        ymax = round_up_to(ymax, cell_h + spacing_h)
    elif grid_start != 'bounds':
        raise ValueError(f"Unrecognised grid_start: {grid_start}")

    cells = []
    for x in frange(xmin, xmax, cell_w + spacing_w):
        for y in frange(ymin, ymax, cell_h + spacing_h):
            cells.append(rect(x, y, cell_w, cell_h))
    rtree = STRtree(cells)
    return [cast(Polygon, cells[idx]) for idx in rtree.query(poly, predicate='intersects')]


def get_grid_refs(poly, cell_size: int) -> List[str]:
    """
    Get grid regs (in the same format that the LiDAR filenames use:
    e.g. SV54ne, or SM66) of the bottom left (SW) corner of each grid ref tile
    that intersects the polygon (which should be in srid 27700)
    """
    grid_refs = []
    for cell in get_grid_cells(poly, cell_size, cell_size):
        x, y, _, _ = cell.bounds
        if is_in_range(x, y):
            grid_refs.append(en_to_grid_ref(x, y, cell_size))
        else:
            print(f"Cannot get grid ref for EN ({x},{y}) - out of bounds")
    return grid_refs


def largest_polygon(g: Union[MultiPolygon, Polygon]):
    if g is None:
        return None
    if g.geom_type == 'Polygon':
        return g
    polygons = [p for p in multi(g).geoms if p.geom_type == 'Polygon']
    if len(polygons) == 0:
        return None
    elif len(polygons) == 1:
        return polygons[0]

    return max(polygons, key=lambda poly: poly.area)


def multi(g: BaseGeometry) -> BaseMultipartGeometry:
    if g.geom_type == 'Polygon':
        return MultiPolygon([g])
    elif g.geom_type == 'Point':
        return MultiPoint([g])
    elif g.geom_type == 'LineString':
        return MultiLineString([g])
    elif g.geom_type == 'LinearRing':
        return MultiLineString([g])
    else:
        return g


def geoms(g: BaseGeometry) -> List[BaseGeometry]:
    """Recursively unnest grouped geometries"""
    if g.geom_type in ("MultiPoint", "MultiLineString", "MultiPolygon"):
        return g.geoms
    elif g.geom_type == "GeometryCollection":
        gs = [geoms(g_) for g_ in g.geoms]
        return list(itertools.chain.from_iterable(gs))
    else:
        return [g]


def azimuth_rad(p1: Tuple[float, float], p2: Tuple[float, float]) -> float:
    angle = math.atan2(p2[0] - p1[0], p2[1] - p1[1])
    return angle if angle > 0 else angle + math.pi


def azimuth_deg(p1: Tuple[float, float], p2: Tuple[float, float]) -> float:
    angle = math.atan2(p2[0] - p1[0], p2[1] - p1[1])
    return math.degrees(angle) if angle > 0 else math.degrees(angle) + 180


def project(x: float, y: float, src_srs: int, dst_srs: int) -> Tuple[float, float]:
    from osgeo import ogr
    from osgeo import osr

    in_sr = osr.SpatialReference()
    in_sr.ImportFromEPSG(src_srs)
    # Force gdal to use x,y ordering for coords:
    # https://gdal.org/tutorials/osr_api_tut.html#crs-and-axis-order
    in_sr.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    out_sr = osr.SpatialReference()
    out_sr.ImportFromEPSG(dst_srs)
    out_sr.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)

    Point = ogr.Geometry(ogr.wkbPoint)
    Point.AddPoint(x=float(x), y=float(y))
    Point.AssignSpatialReference(in_sr)
    Point.TransformTo(out_sr)
    return Point.GetX(), Point.GetY()


def project_geom(geom: BaseGeometry, src_srs: int, dst_srs: int):
    return ops.transform(lambda x, y: project(x, y, src_srs, dst_srs), geom)


def polygon_line_segments(polygon: Polygon, min_length: float = 0) -> List[LineString]:
    """
    Decompose a polygon to straight-line segments.

    Optionally specify a minimum length for the line segment.
    """
    segments = []
    for ring in itertools.chain([polygon.exterior], polygon.interiors):
        segments.extend(line_segments(ring, min_length))

    return segments


def line_segments(ls: LineString, min_length: float = 0) -> List[LineString]:
    segments = []

    a, b = itertools.tee(ls.coords)
    next(b, None)
    for p1, p2 in zip(a, b):
        line = LineString([p1, p2])
        if line.length > min_length:
            segments.append(line)

    return segments


def simplify_by_angle(poly: Polygon, tolerance_degrees: float = 1.0) -> Polygon:
    """
    tolerance_degrees: degree tolerance for comparison between vectors.

    Adapted from code in https://github.com/shapely/shapely/issues/1046
    """
    holes = [ip.coords[:] for ip in poly.interiors]
    simple_shell = _simplify_ring_by_angle(poly.exterior.coords[:], tolerance_degrees)
    simple_holes = [_simplify_ring_by_angle(hole, tolerance_degrees) for hole in holes]
    simple_poly = simple_shell.difference(ops.unary_union(simple_holes))
    return simple_poly


def _simplify_ring_by_angle(coords, tolerance_degrees: float) -> Polygon:
    """
    tolerance_degrees: degree tolerance for comparison between vectors.

    Adapted from code in https://github.com/shapely/shapely/issues/1046
    """
    vector_rep = np.diff(coords, axis=0)
    num_vectors = len(vector_rep)
    angles_list = []
    for i in range(0, num_vectors):
        angles_list.append(np.abs(_get_angle(vector_rep[i], vector_rep[(i + 1) % num_vectors])))

    thresh_vals_by_deg = np.where(np.array(angles_list) > tolerance_degrees)

    new_idx = list(thresh_vals_by_deg[0] + 1)
    new_vertices = [coords[idx] for idx in new_idx]

    return Polygon(new_vertices)


def _get_angle(vec_1, vec_2):
    dot = np.dot(vec_1, vec_2)
    det = np.cross(vec_1, vec_2)
    angle_in_rad = np.arctan2(det, dot)
    return np.degrees(angle_in_rad)


def interpolate_between_points(p1: Tuple[float, float], p2: Tuple[float, float], fraction: float):
    if fraction == 0.0:
        x = p1[0]
        y = p1[1]
    elif fraction == 1.0:
        x = p2[0]
        y = p2[1]
    else:
        x = p1[0] + (p2[0] - p1[0]) * fraction
        y = p1[1] + (p2[1] - p1[1]) * fraction
    return x, y


def de_zigzag(poly: Polygon, limit: float = 2.8) -> Polygon:
    """
    De-zigzag a polygon by moving each point halfway towards the next point (unless
    the distance to the next point is over `limit`)
    TODO holes
    """
    p = largest_polygon(poly).exterior.coords
    num_p = len(p)
    new_points = []
    for i in range(0, num_p):
        p1 = p[i]
        p2 = p[(i + 1) % num_p]
        dist = math.dist(p1, p2)
        if dist > limit:
            new_points.append(p1)
        new_points.append(interpolate_between_points(p1, p2, 0.5))
    return Polygon(new_points)


def perpendicular_bisector(line_segment: LineString, length: float):
    l1 = line_segment.parallel_offset(length / 2, side='left')
    l2 = line_segment.parallel_offset(length / 2, side='right')
    return LineString([l1.centroid.coords[0], l2.centroid.coords[0]])


def densify_line(ls: LineString, step: float) -> LineString:
    if ls.length < step:
        return ls
    points = []

    for seg in line_segments(ls):
        length = seg.length
        for dist in frange(0, length + step, step):
            points.append(seg.interpolate(dist))

    return LineString(points)


def densify_polygon(p: Polygon, step: float) -> Polygon:
    holes = p.interiors
    dense_shell = densify_line(p.exterior, step)
    dense_holes = [densify_line(hole, step) for hole in holes]
    dense_poly = Polygon(dense_shell).difference(ops.unary_union(dense_holes))
    return dense_poly


def split_poly(poly: Polygon, splitter: LineString | MultiLineString | LinearRing):
    """
    Split a Polygon with a LineString, MultiLineString or LinearRing. Essentially the
    same as what shapely.ops.split does, except that has a guard that is too strict and
    doesn't allow using MultiLineStrings or LinearRings as splitters.
    """
    if splitter.geom_type not in ("LineString", "MultiLineString", "LinearRing"):
        raise ValueError(f"Cannot split polygon with {splitter}")

    union = poly.boundary.union(splitter)
    poly = prep(poly)
    return MultiPolygon([pg for pg in ops.polygonize(union) if poly.contains(pg.representative_point())])


def split_evenly(p1: Polygon, p2: Polygon) -> Tuple[Polygon, Polygon]:
    """
    Split 2 overlapping polygons evenly
    TODO more docs
    """
    overlap = p1.intersection(p2)
    if overlap is None or overlap.is_empty:
        return p1, p2

    overlap = multi(overlap)
    split_overlap_parts = []
    for overlap_part in overlap.geoms:
        if overlap_part.geom_type != 'Polygon' or overlap_part.area < 0.0001:
            continue
        overlap_part = densify_polygon(overlap_part, 0.3)
        edges = ops.voronoi_diagram(overlap_part, edges=True)
        usable_edges = []
        candidate_edges = []
        for edge in geoms(edges):
            if overlap_part.contains(edge):
                usable_edges.append(edge)
            else:
                candidate_edges.append(edge)

        rtree = STRtree(usable_edges)
        for edge in candidate_edges:
            touching = rtree.query(edge, predicate='intersects')
            if len(touching) < 2:
                usable_edges.append(edge)
                rtree = STRtree(usable_edges)

        splitter = []
        for ls in geoms(ops.linemerge(usable_edges)):
            splitter.append(ls.simplify(1.0))
        splitter = MultiLineString(splitter)
        split_overlap_parts.extend(split_poly(overlap_part, splitter).geoms)

    # if len(splitter) == 0:
    #     return p1, p2
    # splitter = MultiLineString(splitter)
    # split_overlap_parts = split_poly()
    # p1_new = largest_polygon(split_poly(p1, splitter))
    # p2_new = largest_polygon(split_poly(p2, splitter))
    p1_new = p1.difference(p2)
    p2_new = p2.difference(p1)
    p1_parts = [p1_new]
    p2_parts = [p2_new]
    for poly in split_overlap_parts:
        # TODO constant
        poly = set_precision(poly, 0.01)
        p1_dist = poly.centroid.distance(p1_new)
        p2_dist = poly.centroid.distance(p2_new)
        if p1_dist <= p2_dist:
            p1_parts.append(poly)
        else:
            p2_parts.append(poly)

    p1_new = largest_polygon(ops.unary_union(p1_parts))
    p2_new = largest_polygon(ops.unary_union(p2_parts))
    return p1_new, p2_new

    # intersection = p1.intersection(p2)
    # if intersection is None or intersection.is_empty:
    #     return p1, p2
    #
    # intersection = multi(intersection)
    # splitters = []
    #
    # for poly in intersection.geoms:
    #     if poly.geom_type != 'Polygon' or poly.area < 0.0001:
    #         continue
    #     poly = simplify_by_angle(poly)
    #     splitters.append(LineString(poly.exterior.coords))
    #     line_segments = polygon_line_segments(poly)
    #     for line_segment in line_segments:
    #         if line_segment.length < 0.1:
    #             continue
    #         pb = perpendicular_bisector(line_segment, 1000)
    #         pb_i = poly.exterior.intersection(pb)
    #         for point in multi(pb_i).geoms:
    #             dist = point.distance(line_segment)
    #             if dist > 0.1:
    #                 splitter = affinity.scale(line_segment.parallel_offset(dist / 2, side='left'), xfact=2.0, yfact=2.0)
    #                 splitter_2 = affinity.scale(line_segment.parallel_offset(dist / 2, side='right'), xfact=2.0, yfact=2.0)
    #                 if splitter.intersects(poly):
    #                     splitters.append(splitter)
    #                 if splitter_2.intersects(poly):
    #                     splitters.append(splitter_2)
    #
    # splitters = list(multi(ops.unary_union(splitters)).geoms)
    # split_parts = ops.polygonize(splitters)
    # p1_new = p1.difference(p2)
    # p2_new = p2.difference(p1)
    # p1_parts = [p1_new]
    # p2_parts = [p2_new]
    # for poly in split_parts:
    #     if poly.intersects(intersection):
    #         # TODO constant
    #         poly = set_precision(poly, 0.01)
    #         p1_dist = poly.centroid.distance(p1_new)
    #         p2_dist = poly.centroid.distance(p2_new)
    #         if p1_dist <= p2_dist:
    #             p1_parts.append(poly)
    #         else:
    #             p2_parts.append(poly)
    #
    # p1_new = largest_polygon(ops.unary_union(p1_parts))
    # p2_new = largest_polygon(ops.unary_union(p2_parts))
    # # if p1_new.geom_type != 'Polygon' or p2_new.geom_type != 'Polygon':
    # #     raise ValueError(f"made a non-polygon: {p1_new.geom_type} {p2_new.geom_type}")
    # return p1_new, p2_new


def slope_deg(a: float, b: float) -> float:
    """
    Return the slope of a plane defined by the X coefficient a and the Y coefficient b,
    in degrees from flat.
    """
    return abs(math.degrees(math.atan(math.sqrt(a**2 + b**2))))


def aspect_deg(a: float, b: float) -> float:
    """
    Return the aspect of a plane defined by the X coefficient a  and the Y coefficient b,
    in degrees from North.
    """
    return to_positive_angle(math.degrees(math.atan2(b, -a) + (math.pi / 2)))


def aspect_rad(a: float, b: float) -> float:
    """
    Return the aspect of a plane defined by the X coefficient a  and the Y coefficient b,
    in radians between 0 and 2pi
    """
    a = math.atan2(b, -a) + (math.pi / 2)
    return a if a >= 0 else a + (2 * math.pi)


def circular_mean_rad(pop):
    """
    Circular mean of a population of radians.
    Assumes radians between 0 and 2pi (might work with other ranges, not tested)
    Returns a value between 0 and 2pi.
    """
    cm = math.atan2(np.mean(np.sin(pop)), np.mean(np.cos(pop)))
    return cm if cm >= 0 else cm + (2 * math.pi)


def circular_sd_rad(pop):
    """
    Circular standard deviation of a population of radians.
    Assumes radians between 0 and 2pi (might work with other ranges, not tested).

    See https://en.wikipedia.org/wiki/Directional_statistics#Measures_of_location_and_spread
    """
    return math.sqrt(-2 * math.log(
        math.sqrt(sum(np.sin(pop)) ** 2 +
                  sum(np.cos(pop)) ** 2) /
        len(pop)))


def circular_variance_rad(pop):
    """
    Circular variance of a population of radians.
    Assumes radians between 0 and 2pi (might work with other ranges, not tested).

    See https://en.wikipedia.org/wiki/Directional_statistics#Measures_of_location_and_spread
    """
    return 1 - (math.sqrt(sum(np.sin(pop)) ** 2 +
                          sum(np.cos(pop)) ** 2) /
                len(pop))


def rad_diff(r1, r2):
    """
    Smallest difference between radians.
    Assumes radians between 0 and 2pi. Will return a positive number.
    """
    return min(abs(r1 - r2), (2 * math.pi) - abs(r1 - r2))


def deg_diff(r1, r2):
    """
    Smallest difference between degrees.
    Assumes degrees between 0 and 360. Will return a positive number.
    """
    return min(abs(r1 - r2), 360 - abs(r1 - r2))


def to_positive_angle(angle):
    angle = angle % 360
    return angle + 360 if angle < 0 else angle
