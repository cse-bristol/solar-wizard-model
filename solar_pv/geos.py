# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import itertools
import json
from typing import List, Tuple, Union, cast

import math
import numpy as np
from shapely.geometry.base import BaseGeometry
from shapely.strtree import STRtree
from shapely.geometry import Polygon, shape, MultiPolygon, mapping, LineString
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


def to_geojson(geom):
    if isinstance(geom, BaseGeometry):
        geom_dict = mapping(geom)
        geojson = json.dumps(geom_dict)
        return geojson
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
    return [cast(Polygon, p) for p in rtree.query(poly) if p.intersects(poly)]


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


def largest_polygon(multi: Union[MultiPolygon, Polygon]):
    if multi is None:
        return None
    if multi.type == 'Polygon':
        return multi
    if multi.type != 'MultiPolygon':
        return None
    polygons = [g for g in multi.geoms if g.type == 'Polygon']
    if len(polygons) == 0:
        return None
    elif len(polygons) == 1:
        return polygons[0]

    return max(polygons, key=lambda poly: poly.area)


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
    line_segments = []
    for ring in itertools.chain([polygon.exterior], polygon.interiors):
        a, b = itertools.tee(ring.coords)
        next(b, None)
        for p1, p2 in zip(a, b):
            line = LineString([p1, p2])
            if line.length > min_length:
                line_segments.append(line)

    return line_segments


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
