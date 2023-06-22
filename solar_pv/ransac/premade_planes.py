from dataclasses import dataclass

import math

import itertools
from typing import List, Tuple

import numpy as np
from shapely import ops
from shapely.geometry import Polygon, LineString, Point
from shapely.strtree import STRtree
from sklearn.linear_model import LinearRegression

_SLOPES = [25, 28, 30, 33, 35, 40, 45, 50,
           -25, -28, -30, -33, -35,
           3, 5, 7, 10, 15, 20]
# _SLOPES = [-25, -28, -30, -33, -35,
#            0, 3, 5, 10, 15, 20, 22, 25, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 42, 45, 47, 50]


@dataclass
class Plane:
    p1: Tuple[float, float]
    p2: Tuple[float, float]
    z: float
    slope: float

    def fit(self) -> LinearRegression:
        line = LineString([self.p1, self.p2])
        points = []
        for offset in [0, 2, 4, 6, 8]:
            l_off = line.parallel_offset(offset, 'left')
            lp1 = l_off.coords[0]
            lp2 = l_off.coords[1]
            z_offset = math.tan(math.radians(self.slope)) * offset
            for frac in [0.0, 0.2, 0.4, 0.6, 0.8, 1.0]:
                points.append(_interpolate_between_points(lp1, lp2, self.z + z_offset, frac))

        points = np.array(points)
        XY = points[:, :2]
        Z = points[:, 2]
        lr = LinearRegression()
        lr.fit(XY, Z)
        return lr


def create_planes(pixels_in_building: List[dict], polygon: Polygon) -> List[Plane]:
    planes = []
    points = [Point(p['x'], p['y'], p['elevation']) for p in pixels_in_building]
    rtree = STRtree(points)
    polygon = _simplify_by_angle(polygon)

    for ring in itertools.chain([polygon.exterior], polygon.interiors):
        for p1, p2 in pairwise(ring.coords):
            line = LineString([p1, p2])
            if line.length < 3:
                continue
            poly = line.buffer(2)
            # TODO need to dedupe these nearby pixels  by (height, dist) a bit
            nearby_pixels = [p for p in rtree.query(poly) if p.intersects(poly)]
            for pixel in nearby_pixels:
                dist = pixel.distance(line)
                offset_line = line.parallel_offset(dist, 'left')
                if offset_line.is_empty:
                    continue
                for slope in _SLOPES:
                    planes.append(Plane(p1=offset_line.coords[0], p2=offset_line.coords[1], z=pixel.coords[0][2], slope=slope))

    return planes


def _interpolate_between_points(p1, p2, z: float, fraction: float):
    if fraction == 0.0:
        x = p1[0]
        y = p1[1]
    elif fraction == 1.0:
        x = p2[0]
        y = p2[1]
    else:
        x = p1[0] + (p2[0] - p1[0]) * fraction
        y = p1[1] + (p2[1] - p1[1]) * fraction
    return x, y, z


def pairwise(iterable):
    a, b = itertools.tee(iterable)
    next(b, None)
    return zip(a, b)


def _simplify_by_angle(poly: Polygon, deg_tol: float = 1) -> Polygon:
    shell = Polygon(poly.exterior.coords)
    holes = [Polygon(ip.coords) for ip in poly.interiors]
    simple_shell = _simplify_ring_by_angle(shell, deg_tol)
    simple_holes = [_simplify_ring_by_angle(hole, deg_tol) for hole in holes]
    simple_poly = simple_shell.difference(ops.unary_union(simple_holes))
    return simple_poly


def _simplify_ring_by_angle(poly: Polygon, deg_tol: float) -> Polygon:
    """
    deg_tol: degree tolerance for comparison between successive vectors
    """
    ext_poly_coords = poly.exterior.coords[:]
    vector_rep = np.diff(ext_poly_coords, axis=0)
    num_vectors = len(vector_rep)
    angles_list = []
    for i in range(0, num_vectors):
        angles_list.append(np.abs(_get_angle(vector_rep[i], vector_rep[(i + 1) % num_vectors])))

    # get mask satisfying tolerance
    thresh_vals_by_deg = np.where(np.array(angles_list) > deg_tol)

    new_idx = list(thresh_vals_by_deg[0] + 1)
    new_vertices = [ext_poly_coords[idx] for idx in new_idx]

    return Polygon(new_vertices)


def _get_angle(vec_1, vec_2):
    dot = np.dot(vec_1, vec_2)
    det = np.cross(vec_1, vec_2)
    angle_in_rad = np.arctan2(det, dot)
    return np.degrees(angle_in_rad)