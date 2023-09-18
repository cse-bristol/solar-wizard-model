from dataclasses import dataclass

from typing import List, Optional

import numpy as np
from shapely.geometry import Polygon
from skimage import measure
from skimage import segmentation
from skimage.future.graph import rag_mean_color, cut_threshold
from sklearn.linear_model import LinearRegression


@dataclass
class Plane:
    xy: np.ndarray
    z: np.ndarray
    idxs: np.ndarray
    sample_residual_threshold: float
    plane_type: str
    plane_id: str

    def fit(self) -> LinearRegression:
        lr = LinearRegression()
        lr.fit(self.xy, self.z)
        return lr


# TODO move somewhere else, gets used outside this file
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
    initial_segments = segmentation.slic(image, compactness=30, start_label=1, mask=mask)

    g = rag_mean_color(image, initial_segments, connectivity=2)

    segments = cut_threshold(initial_segments, g, threshold)
    # sort out 0 being used as a segment ID
    segments += 1
    segments *= mask
    segments = measure.label(segments, connectivity=1)
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


def _dbscan(z):
    from sklearn.cluster import DBSCAN
    z = z.reshape(-1, 1)
    db = DBSCAN(eps=0.6, min_samples=5).fit(z)

    labels = db.labels_
    labels[labels == 0] = np.amax(labels) + 1
    return labels


def roughness(image):
    """
    Roughness detection based on the Terrain Ruggedness Index: (the mean difference
    between a central pixel and its surrounding cells).

    With some tweaks:
    * corner cells of the 3x3 window are weighted slightly less
    * uses angular difference rather than standard difference so that it works for
      aspect rasters (assumes degree-based values)
    """

    win = np.array([[0.7, 1.0, 0.7],
                    [1.0, 0.0, 1.0],
                    [0.7, 1.0, 0.7]])

    rows, columns = image.shape
    diff_sum = np.zeros((rows, columns))

    pad = 1
    padded = np.pad(image, pad, mode='edge')

    for (y, x), val in np.ndenumerate(win):
        if val == 0:
            continue
        m = padded[y: rows + y, x: columns + x]
        # Use angular difference so that e.g. aspect rasters can be used:
        raw_diff = np.abs(m - image)
        diff = np.minimum(raw_diff, 360 - raw_diff) * val
        # diff = np.abs((m - image) * val)
        diff_sum += diff

    mean_diff = diff_sum / np.sum(win)
    return mean_diff


@dataclass
class PlaneDef:
    plane_type: str
    segmenting_threshold: float
    sample_residual_threshold: float
    max_slope: Optional[float] = None


def create_planes(xyz: np.ndarray, aspect: np.ndarray, slope: np.ndarray, res: float):
    # TODO try using min ground height as a noise val here?
    planes = []

    xy = xyz[:, :2]
    z = xyz[:, 2]
    nodata = 0.0
    aspect_image, idxs = _image(xy, aspect, res, nodata)
    z_image, _ = _image(xy, z, res, nodata)

    slope_image, _ = _image(xy, slope, res, nodata)

    # roughness_image = roughness(aspect_image)
    # roughness_mask = (roughness_image > 45) & (slope_image > 5)

    noise_val = -1
    z_labels = _dbscan(z)
    z_segments, _ = _image(xy, z_labels, res, nodata)
    noise_mask = z_segments == noise_val
    z_segments = measure.label(z_segments, background=nodata)
    z_segments[noise_mask] = noise_val

    num_z_segments = int(np.amax(z_segments))

    plane_defs: List[PlaneDef] = [
        PlaneDef(plane_type="segmented_aspect", segmenting_threshold=29, sample_residual_threshold=0.25),
        PlaneDef(plane_type="segmented_aspect", segmenting_threshold=29, sample_residual_threshold=2.0),
        PlaneDef(plane_type="segmented_aspect", segmenting_threshold=15, sample_residual_threshold=0.25),
        PlaneDef(plane_type="segmented_aspect", segmenting_threshold=15, sample_residual_threshold=2.0),
        #
        # PlaneDef(plane_type="segmented_slope", segmenting_threshold=5, sample_residual_threshold=0.25, max_slope=55),
        # PlaneDef(plane_type="segmented_slope", segmenting_threshold=5, sample_residual_threshold=2.0, max_slope=55),

        # PlaneDef(plane_type="segmented_z", segmenting_threshold=1.5, sample_residual_threshold=1.0),
    ]

    for z_segment_id in range(1, num_z_segments + 1):
        z_idx_subset = idxs[z_segments == z_segment_id]
        if len(z_idx_subset) <= 3:
            continue

        mask = z_segments == z_segment_id
        for plane_def in plane_defs:
            plane_type = plane_def.plane_type
            threshold = plane_def.segmenting_threshold
            sample_residual_threshold = plane_def.sample_residual_threshold
            if plane_type == "segmented_aspect":
                segments = _segment(aspect_image, mask, threshold=threshold)
            elif plane_type == "segmented_slope":
                segments = _segment(slope_image, mask & (slope_image < plane_def.max_slope), threshold=threshold)
            elif plane_type == "segmented_z":
                segments = _segment(z_image, mask, threshold=threshold)
            else:
                raise ValueError(f"Unrecognised plane_type: {plane_type}")

            num_segments = np.amax(segments)
            for segment_id in range(1, num_segments + 1):
                idx_subset = idxs[segments == segment_id]
                # TODO could use a higher threshold than 3?
                if len(idx_subset) > 3:
                    xy_subset = xy[idx_subset]
                    z_subset = z[idx_subset]
                    plane_id = f'{plane_type}_{z_segment_id}_{threshold}_{sample_residual_threshold}_{segment_id}'
                    planes.append(Plane(xy=xy_subset, z=z_subset, idxs=idx_subset,
                                        plane_type=plane_type, plane_id=plane_id,
                                        sample_residual_threshold=sample_residual_threshold))

    return planes
