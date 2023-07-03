from dataclasses import dataclass

import math

import itertools
from typing import List, Tuple, cast

import numpy as np
from shapely import ops
from shapely.geometry import Polygon, LineString, Point, CAP_STYLE, JOIN_STYLE
from shapely.strtree import STRtree
from sklearn.linear_model import LinearRegression, HuberRegressor

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
                x, y = _interpolate_between_points(lp1, lp2, frac)
                z = self.z + z_offset,
                points.append((z, y, z))

        points = np.array(points)
        XY = points[:, :2]
        Z = points[:, 2]
        lr = LinearRegression()
        lr.fit(XY, Z)
        return lr


@dataclass
class PointPlane:
    points: List[Point]

    def fit(self) -> LinearRegression:
        points = np.array([p.coords[0] for p in self.points])
        XY = points[:, :2]
        Z = points[:, 2]
        lr = LinearRegression()
        lr.fit(XY, Z)
        return lr


@dataclass
class ArrayPlane:
    xy: np.ndarray
    z: np.ndarray
    idxs: np.ndarray

    def fit(self) -> LinearRegression:
        lr = LinearRegression()
        lr.fit(self.xy, self.z)
        return lr


def create_planes_2(xyz: np.ndarray, aspect: np.ndarray, res: float):
    from skimage import segmentation
    from skimage.future.graph import rag_mean_color, cut_threshold
    planes = []

    xy = xyz[:, :2]
    z = xyz[:, 2]
    min_xy = [np.amin(xy[:, 0]), np.amin(xy[:, 1])]

    normed = ((xy - min_xy) / res).astype(int)
    image = np.full((int(np.amax(normed[:, 1])) + 1,
                     int(np.amax(normed[:, 0])) + 1), -9999.0)
    idxs = np.zeros((int(np.amax(normed[:, 1])) + 1,
                     int(np.amax(normed[:, 0])) + 1), dtype=int)
    for i, pair in enumerate(normed):
        image[pair[1]][pair[0]] = aspect[i]
        idxs[pair[1]][pair[0]] = i

    image = np.flip(image, axis=0)
    idxs = np.flip(idxs, axis=0)
    mask = image != -9999.0

    initial_segments = segmentation.slic(image, compactness=30, start_label=1, mask=mask)

    g = rag_mean_color(image, initial_segments)

    segments = cut_threshold(initial_segments, g, 29)
    num_segments = np.amax(segments)
    # TODO make planes with a range of different thresholds?
    # TODO segment using height first
    for segment_id in range(1, num_segments + 1):
        idx_subset = idxs[segments == segment_id]
        if len(idx_subset) > 3:
            xy_subset = xy[idx_subset]
            z_subset = z[idx_subset]
            # TODO remove n worst outliers as variations?
            planes.append(ArrayPlane(xy=xy_subset, z=z_subset, idxs=idx_subset))

            # lr = HuberRegressor()
            # lr.fit(xy_subset, z_subset)
            # z_pred = lr.predict(xy_subset)
            # def loss_function(y_true, y_pred):
            #     return np.abs(y_true - y_pred)
            # residuals_subset = loss_function(z_subset, z_pred)
            # residual_threshold = 0.25
            # sd = np.std(residuals_subset[residuals_subset < residual_threshold])

            # lr2 = LinearRegression()
            # lr2.fit(xy_subset, z_subset)
            # z2_pred = lr2.predict(xy_subset)
            # residuals_subset2 = loss_function(z_subset, z2_pred)

            # sd2 = np.std(residuals_subset2[residuals_subset2 < residual_threshold])

            # print(residuals_subset)

    return planes


def create_planes(pixels_in_building: List[dict], polygon: Polygon) -> List[Plane]:
    planes = []
    points = [Point(p['x'], p['y'], p['elevation']) for p in pixels_in_building]
    rtree = STRtree(points)
    centroid = polygon.centroid
    polygon = _simplify_by_angle(polygon)

    def _rect_to_point_plane(_line: LineString, _point: Point, remove_lowest_n: int = None):
        _poly = _line.buffer(_point.distance(_line), single_sided=True, cap_style=CAP_STYLE.square, join_style=JOIN_STYLE.mitre)
        _pixels = [cast(Point, p) for p in rtree.query(_poly) if p.intersects(_poly)]
        if remove_lowest_n:
            _pixels.sort(key=lambda p: p.coords[0][2])
            _pixels = _pixels[remove_lowest_n:]

        if len(_pixels) > 0:
            planes.append(PointPlane(points=_pixels))

    def _triangle_to_point_plane(_line: LineString, _point: Point, remove_lowest_n: int = None):
        _poly = Polygon([_line.coords[0], _line.coords[1], _point.coords[0]])
        _pixels = [cast(Point, p) for p in rtree.query(_poly) if p.intersects(_poly)]
        if remove_lowest_n:
            _pixels.sort(key=lambda p: p.coords[0][2])
            _pixels = _pixels[remove_lowest_n:]
        if len(_pixels) > 0:
            planes.append(PointPlane(points=_pixels))

    for ring in itertools.chain([polygon.exterior], polygon.interiors):
        for p1, p2 in pairwise(ring.coords):
            line = LineString([p1, p2])
            if line.length < 1:
                continue

            _rect_to_point_plane(line, centroid)
            # _rect_to_point_plane(line, centroid, remove_lowest_n=5)
            # _rect_to_point_plane(line, centroid, remove_lowest_n=10)
            # _rect_to_point_plane(_shrink_line(line, 0.2), centroid)
            _rect_to_point_plane(line.parallel_offset(1, 'left'), centroid)
            # TODO how about centroid - 1/3 ? and so on

            _triangle_to_point_plane(line, centroid)
            # _triangle_to_point_plane(line, centroid, remove_lowest_n=5)
            # _triangle_to_point_plane(line, centroid, remove_lowest_n=10)
            # _triangle_to_point_plane(_shrink_line(line, 0.2), centroid)
            _triangle_to_point_plane(line.parallel_offset(1, 'left'), centroid)

            # poly = line.buffer(2)
            # # TODO need to dedupe these nearby pixels  by (height, dist) a bit
            # nearby_pixels = [p for p in rtree.query(poly) if p.intersects(poly)]
            # for pixel in nearby_pixels:
            #     dist = pixel.distance(line)
            #     offset_line = line.parallel_offset(dist, 'left')
            #     if offset_line.is_empty:
            #         continue
            #     for slope in _SLOPES:
            #         planes.append(Plane(p1=offset_line.coords[0], p2=offset_line.coords[1], z=pixel.coords[0][2], slope=slope))

    return planes


def _shrink_line(line: LineString, fraction: float):
    p1 = line.coords[0]
    p2 = line.coords[1]
    new_p1 = _interpolate_between_points(p1, p2, fraction)
    new_p2 = _interpolate_between_points(p1, p2, 1 - fraction)
    return LineString([new_p1, new_p2])


def _interpolate_between_points(p1, p2, fraction: float):
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