from dataclasses import dataclass

import math

import itertools
from typing import List, Tuple, cast

import numpy as np
from shapely import ops
from shapely.geometry import Polygon, LineString, Point, CAP_STYLE, JOIN_STYLE
from shapely.strtree import STRtree
from sklearn.linear_model import LinearRegression, HuberRegressor

from solar_pv.ransac.ransac import _aspect
from solar_pv.roof_polygons.roof_polygons_2 import _building_orientations

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
    plane_type: str

    def fit(self) -> LinearRegression:
        lr = LinearRegression()
        lr.fit(self.xy, self.z)
        return lr


def _image(xy: np.ndarray, vals: np.ndarray, res: float, nodata: float):
    min_xy = [np.amin(xy[:, 0]), np.amin(xy[:, 1])]

    normed = ((xy - min_xy) / res).astype(int)
    image = np.full((int(np.amax(normed[:, 1])) + 1,
                     int(np.amax(normed[:, 0])) + 1), nodata)
    idxs = np.zeros((int(np.amax(normed[:, 1])) + 1,
                     int(np.amax(normed[:, 0])) + 1), dtype=int)
    for i, pair in enumerate(normed):
        image[pair[1]][pair[0]] = vals[i]
        idxs[pair[1]][pair[0]] = i

    image = np.flip(image, axis=0)
    idxs = np.flip(idxs, axis=0)
    return image, idxs


def _segment(image: np.ndarray, mask: np.ndarray, threshold: float):
    from skimage import segmentation
    from skimage.future.graph import rag_mean_color, cut_threshold
    initial_segments = segmentation.slic(image, compactness=30, start_label=1, mask=mask)

    g = rag_mean_color(image, initial_segments)

    segments = cut_threshold(initial_segments, g, threshold)
    # sort out 0 being used as a segment ID
    segments += 1
    segments *= mask
    return segments


def _segment_sizes(segment_image: np.ndarray):
    ids, sizes = np.unique(segment_image, return_counts=True)
    return list(zip(ids, sizes))


def _merge_small_segments(segment_image: np.ndarray, max_size: int):
    from skimage.segmentation import expand_labels

    segment_sizes = _segment_sizes(segment_image)
    small_segments = [zss[0] for zss in segment_sizes if zss[1] <= max_size and zss[0] != 0]
    mask = np.isin(segment_image, small_segments)
    nodata_mask = segment_image == 0
    segment_image[mask] = 0
    enlarged_segment_image = expand_labels(segment_image)
    enlarged_segment_image[nodata_mask] = 0
    return enlarged_segment_image


def _contours(image, mask):
    import numpy as np
    import matplotlib.pyplot as plt

    from skimage import measure
    contours = measure.find_contours(image, mask=mask)

    # Display the image and plot all contours found
    fig, ax = plt.subplots()
    ax.imshow(image, cmap=plt.cm.gray)

    for contour in contours:
        ax.plot(contour[:, 1], contour[:, 0], linewidth=2)

    ax.axis('image')
    ax.set_xticks([])
    ax.set_yticks([])
    plt.show()


def _dbscan(z):
    from sklearn.cluster import DBSCAN, OPTICS
    from sklearn import metrics
    z = z.reshape(-1, 1)
    db = DBSCAN(eps=0.6, min_samples=5).fit(z)
    # db = OPTICS(min_cluster_size=10, xi=0.4).fit(z)
    labels = db.labels_

    # Number of clusters in labels, ignoring noise if present.
    n_clusters_ = len(set(labels)) - (1 if -1 in labels else 0)
    n_noise_ = list(labels).count(-1)

    print("Estimated number of clusters: %d" % n_clusters_)
    print("Estimated number of noise points: %d" % n_noise_)
    if n_clusters_ > 1:
        print("Silhouette Coefficient: %0.3f" % metrics.silhouette_score(z, labels))

    labels[labels == 0] = np.amax(labels) + 1
    # labels[labels == -1] = np.amax(labels) + 1
    return labels


def hillshade(array: np.ndarray, azimuth: float, angle_altitude: float):
    """
    Adapted from GDAL c++ algorithm
    see https://github.com/OSGeo/gdal/blob/09320728b45d7d6b0bf50dad350bdbb97db0bcd6/apps/gdaldem_lib.cpp#L754-L803
    """
    azimuth = 360.0 - azimuth + 90.0

    x, y = np.gradient(array)
    azimuth_rad = math.radians(azimuth)
    altitude_rad = math.radians(angle_altitude)

    shaded = ((altitude_rad * 254 -
                 (y * np.cos(azimuth_rad) * np.cos(altitude_rad) * 254 -
                  x * np.sin(azimuth_rad) * np.cos(altitude_rad) * 254)) /
                np.sqrt(1 + x * x + y * y))
    return (shaded + 1).clip(min=1.0)


def create_planes_2(xyz: np.ndarray, aspect: np.ndarray, polygon: Polygon, res: float):
    planes = []
    from skimage import measure

    xy = xyz[:, :2]
    z = xyz[:, 2]
    nodata = 0.0  # careful - some segmentations use 0 as a label
    aspect_image, idxs = _image(xy, aspect, res, nodata)
    z_image, _ = _image(xy, z, res, nodata)

    # TODO use building orientation - then do a segmenting per azimuth
    #      can also check plane aspect matches hillshade azimuth
    orientations = _building_orientations(polygon)
    z_mask = (z_image != nodata)
    hillshades = [{"hillshade": hillshade(z_image, o, 0.0), "orientation": o} for o in orientations]

    # h1 = hillshade(z_image, 76, 0.0) * z_mask
    # h2 = hillshade(z_image, 76 + 90, 0.0) * z_mask
    # h3 = hillshade(z_image, 76 + 180, 0.0) * z_mask
    # h4 = hillshade(z_image, 76 + 270, 0.0) * z_mask
    # h2_seg = measure.label(h2 > 20, background=0)

    # z_segments = _segment(z_image, z_image != nodata, threshold=1.5)  # 3 works much better for 1650 but much worse for 1657 / 1649
    # z_segments = _merge_small_segments(z_segments, max_size=3)

    # attempt at better height segments... I think it's better. worse results in some places more due to luck on part of old approach than anything
    noise_val = -1
    z_labels = _dbscan(z)
    z_segments, _ = _image(xy, z_labels, res, nodata)
    noise_mask = z_segments == noise_val
    z_segments = measure.label(z_segments, background=nodata)
    z_segments = _merge_small_segments(z_segments, max_size=3)
    z_segments[noise_mask] = noise_val

    num_z_segments = int(np.amax(z_segments))

    from skimage import feature
    from skimage import measure
    from skimage import exposure
    # c = feature.canny(exposure.rescale_intensity(z_image), mask=z_image != nodata, )
    # _contours(z_image, mask=z_image != nodata)

    for z_segment_id in range(1, num_z_segments + 1):
        z_idx_subset = idxs[z_segments == z_segment_id]
        if len(z_idx_subset) > 3:
            # don't mask out small z_segments - only nodata and other large z_segment_ids
            mask = z_segments == z_segment_id
            # TODO make planes with a range of different thresholds?
            segmented_aspect = _segment(aspect_image, mask, threshold=29)
            segmentings = [{"segments": segmented_aspect, "plane_type": "segmented_aspect"}]
            # segmentings = []
            for hs in hillshades:
                segmented_hs = measure.label(hs["hillshade"] > 20, background=0) * mask
                segmentings.append({"segments": segmented_hs, "plane_type": f"hillshade_{hs['orientation']}"})

            for segments in segmentings:
                num_segments = np.amax(segments["segments"])
                for segment_id in range(1, num_segments + 1):
                    idx_subset = idxs[segments["segments"] == segment_id]
                    if len(idx_subset) > 3:
                        xy_subset = xy[idx_subset]
                        z_subset = z[idx_subset]
                        # TODO remove n worst outliers as variations?
                        # TODO maybe something like running RANSAC on the points within each segment?
                        planes.append(ArrayPlane(xy=xy_subset, z=z_subset, idxs=idx_subset, plane_type=segments["plane_type"]))

                    # avg_aspect = np.average(aspect_image[segments == segment_id])
                    # lr = LinearRegression()
                    # lr.fit(xy_subset, z_subset)
                    # plane_aspect = _aspect(lr.coef_[0], lr.coef_[1])
                    # print(f"segment {segment_id}: points {len(z_subset)} avg aspect {avg_aspect} plane aspect {plane_aspect}")
                    #
                    # z_pred = lr.predict(xy_subset)
                    # residuals_subset = z_subset - z_pred
                    # res_image, res_idxs = _image(xy_subset, residuals_subset, res, nodata)
                    # res_segments = _segment(res_image, res_image != nodata, threshold=0.5)
                    # print("")

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