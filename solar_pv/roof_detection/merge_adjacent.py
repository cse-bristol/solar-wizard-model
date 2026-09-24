import math
from typing import Dict, Tuple

import numpy as np
from skimage import morphology
from skimage.graph import RAG, merge_hierarchical
from skimage.measure import perimeter_crofton
from sklearn import metrics

from solar_pv.constants import ROOFDET_GOOD_SCORE, FLAT_ROOF_DEGREES_THRESHOLD, \
    AZIMUTH_ALIGNMENT_THRESHOLD, FLAT_ROOF_AZIMUTH_ALIGNMENT_THRESHOLD
from solar_pv.datatypes import RoofPlane
from solar_pv.roof_detection.premade_planes import _image
from solar_pv.geos import slope_deg, aspect_deg, deg_diff, circular_mean_rad, circular_sd_rad
from solar_pv.roof_detection.ransac import _group_areas
from solar_pv.roof_detection.plane_fit import PlaneFit, PlaneSums, mean_absolute_error, r2_score

DO_NOT_MERGE = 9999
DO_MERGE = -9999
R2_GOOD = 0.925


def _edge_weight(graph, src: int, dst: int) -> float:
    dst_node = graph.nodes[dst]
    src_node = graph.nodes[src]

    # 2 outliers:
    if dst_node['outlier'] is src_node['outlier'] is True:
        return DO_NOT_MERGE

    # 2 neighbouring planes:
    elif dst_node['outlier'] is src_node['outlier'] is False:
        # is score the kind of thing that can be legitimately averaged?
        # weighted average:
        dst_inliers = len(dst_node['xy_subset'])
        src_inliers = len(src_node['xy_subset'])
        curr_mae = ((dst_node['mae'] * dst_inliers) +
                    (src_node['mae'] * src_inliers)) / (dst_inliers + src_inliers)

        merged = _MergedFit(dst_node, src_node)

        new_slope = slope_deg(merged.x_coef, merged.y_coef)
        if new_slope > FLAT_ROOF_DEGREES_THRESHOLD and \
                dst_node['slope'] > FLAT_ROOF_DEGREES_THRESHOLD and \
                src_node['slope'] > FLAT_ROOF_DEGREES_THRESHOLD:
            curr_r2 = ((dst_node['r2'] * dst_inliers) +
                       (src_node['r2'] * src_inliers)) / (dst_inliers + src_inliers)
            new_r2 = merged.r2()
            # If the new score is still good enough, don't require it to be better than before
            weight = curr_r2 - new_r2 if new_r2 < R2_GOOD else DO_MERGE

            # if new aspect is outside the range of adjusted aspects, do not merge:
            new_aspect = aspect_deg(merged.x_coef, merged.y_coef)
            if deg_diff(new_aspect, src_node['aspect']) > AZIMUTH_ALIGNMENT_THRESHOLD \
                    and deg_diff(new_aspect, dst_node['aspect']) > AZIMUTH_ALIGNMENT_THRESHOLD:
                weight = DO_NOT_MERGE
        else:
            new_mae = merged.mae()
            # If the new score is still good enough, don't require it to be better than before
            weight = new_mae - curr_mae if new_mae > ROOFDET_GOOD_SCORE else DO_MERGE

    # A plane and an outlier
    else:
        curr_mae = dst_node.get('mae', src_node.get('mae'))
        curr_slope = dst_node.get('slope', src_node.get('slope'))
        merged = _MergedFit(dst_node, src_node)
        new_mae = merged.mae()
        weight = new_mae - curr_mae

        slope = slope_deg(merged.x_coef, merged.y_coef)
        # if roof has changed from flat to non-flat, do not merge:
        if slope > FLAT_ROOF_DEGREES_THRESHOLD >= curr_slope:
            weight = DO_NOT_MERGE
        if slope <= FLAT_ROOF_DEGREES_THRESHOLD < curr_slope:
            weight = DO_NOT_MERGE

        # if new aspect is outside the range of the adjusted aspect, do not merge:
        if slope > FLAT_ROOF_DEGREES_THRESHOLD and weight < 0:
            new_aspect = aspect_deg(merged.x_coef, merged.y_coef)
            aspect_adjusted = dst_node.get('aspect', src_node.get('aspect'))
            if deg_diff(new_aspect, aspect_adjusted) > AZIMUTH_ALIGNMENT_THRESHOLD:
                weight = DO_NOT_MERGE

    return weight


class _MergedFit:
    """
    The plane fit to the union of two nodes' points, for scoring the edge between them.

    Edge weights are recomputed for every neighbour of a node each time it merges, so
    for a large plane bordered by many outlier pixels, refitting it from its points for
    each edge would be expensive. Instead the fit comes from the nodes' `PlaneSums`, 
    in O(1). The metrics still need a single pass over the points' residuals.

    Fits are in each node's local coordinates (`xy_local`); the merge itself
    (`_update_node_data`) still refits the plane from its points with `PlaneFit`.
    """

    def __init__(self, dst_node: dict, src_node: dict):
        self._nodes = (dst_node, src_node)
        fit = (dst_node['plane_sums'] + src_node['plane_sums']).solve()
        if fit is None:
            lr = PlaneFit().fit(
                np.concatenate([dst_node['xy_local'], src_node['xy_local']]),
                np.concatenate([dst_node['z_subset'], src_node['z_subset']]))
            fit = (lr.coef_[0], lr.coef_[1], lr.intercept_)
        self.x_coef, self.y_coef, self._intercept = fit
        self._residuals = [node['z_subset'] - (node['xy_local'] @ (self.x_coef, self.y_coef) + self._intercept)
                           for node in self._nodes]
        self._n = sum(len(r) for r in self._residuals)

    def mae(self) -> float:
        return sum(np.abs(r).sum() for r in self._residuals) / self._n

    def r2(self) -> float:
        """As `r2_score`, over both nodes' points."""
        if self._n < 2:
            return float("nan")
        z_mean = sum(node['z_subset'].sum() for node in self._nodes) / self._n
        numerator = sum(r @ r for r in self._residuals)
        denominator = sum(((node['z_subset'] - z_mean) ** 2).sum() for node in self._nodes)
        if numerator == 0:
            return 1.0
        if denominator == 0:
            return 0.0
        return float(1 - numerator / denominator)


def _new_edge_weight(graph, src: int, dst: int, n: int):
    """
    Callback to recompute edge weights after merging node `src` into `dst`

    Parameters
    ----------
    graph : RAG
        The graph under consideration.
    src, dst : int
        The vertices in `graph` to be merged.
    n : int
        A neighbor of `src` or `dst` or both.
    """
    # By this point, `_update_node_data` has been called, so `src` has already
    # been merged into `dst` - so we ignore `src`.
    return {'weight': _edge_weight(graph, n, dst)}


def _update_node_data(graph, src: int, dst: int):
    """
    Callback called when merging two nodes of a graph.

    Parameters
    ----------
    graph : RAG
        The graph under consideration.
    src, dst : int
        The vertices in `graph` to be merged.
    """
    dst_node = graph.nodes[dst]
    src_node = graph.nodes[src]

    xy_subset = np.concatenate([dst_node['xy_subset'], src_node['xy_subset']])
    z_subset = np.concatenate([dst_node['z_subset'], src_node['z_subset']])
    aspect_subset = np.concatenate([dst_node['aspect_subset'], src_node['aspect_subset']])
    dst_node['xy_local'] = np.concatenate([dst_node['xy_local'], src_node['xy_local']])
    dst_node['plane_sums'] = dst_node['plane_sums'] + src_node['plane_sums']
    lr = PlaneFit()
    lr.fit(xy_subset, z_subset)
    z_pred = lr.predict(xy_subset)
    # merged_score = lr.score(xy_subset, z_subset)
    merged_score = mean_absolute_error(z_subset, z_pred)

    dst_node['building_id'] = dst_node.get('building_id', src_node.get('building_id'))
    dst_node['xy_subset'] = xy_subset
    dst_node['z_subset'] = z_subset
    dst_node['aspect_subset'] = aspect_subset
    dst_node['score'] = merged_score

    dst_node['x_coef'] = lr.coef_[0]
    dst_node['y_coef'] = lr.coef_[1]
    dst_node['intercept'] = lr.intercept_
    dst_node['slope'] = slope_deg(lr.coef_[0], lr.coef_[1])
    dst_node['is_flat'] = dst_node['slope'] <= FLAT_ROOF_DEGREES_THRESHOLD
    dst_node['aspect_raw'] = aspect_deg(lr.coef_[0], lr.coef_[1])
    dst_node['inliers_xy'] = xy_subset

    if dst_node['outlier'] is src_node['outlier'] is False:
        dst_node['plane_type'] = dst_node['plane_type'] + "_MERGED_" + src_node['plane_type']
        dst_node['plane_id'] = dst_node['plane_id'] + "_MERGED_" + src_node['plane_id']
    else:
        dst_node['plane_type'] = dst_node.get('plane_type', src_node.get('plane_type'))
        dst_node['plane_id'] = dst_node.get('plane_id', src_node.get('plane_id'))

    dst_node['outlier'] = False

    dst_node["r2"] = r2_score(z_subset, z_pred)
    dst_node["mae"] = merged_score
    dst_node["mse"] = metrics.mean_squared_error(z_subset, z_pred)
    dst_node["rmse"] = metrics.root_mean_squared_error(z_subset, z_pred)
    try:
        dst_node["msle"] = metrics.mean_squared_log_error(z_subset, z_pred)
    except ValueError:
        dst_node["msle"] = None
    dst_node["mape"] = metrics.mean_absolute_percentage_error(z_subset, z_pred)
    dst_node["sd"] = np.std(np.abs(z_subset - z_pred))

    z_image, idxs = _image(xy_subset, z_subset, nodata=-9999, res=dst_node['res'])
    plane_mask = z_image != -9999
    group_areas = _group_areas(plane_mask)
    roof_plane_area = group_areas[1]
    convex_hull = morphology.convex_hull_image(plane_mask)
    convex_hull_area = np.count_nonzero(convex_hull)
    dst_node["cv_hull_ratio"] = roof_plane_area / convex_hull_area

    perimeter = perimeter_crofton(plane_mask, directions=4)
    dst_node["thinness_ratio"] = (4 * np.pi * roof_plane_area) / (perimeter * perimeter)

    # units match RoofPlane as built by RANSAC: mean in degrees, sd in radians
    aspect_rads = np.radians(aspect_subset)
    dst_node["aspect_circ_mean"] = math.degrees(circular_mean_rad(aspect_rads))
    dst_node["aspect_circ_sd"] = circular_sd_rad(aspect_rads)

    if 'aspect' in src_node and 'aspect' in dst_node:
        a1 = dst_node["aspect"]
        a2 = src_node["aspect"]
        a1_diff = deg_diff(a1, dst_node['aspect_raw'])
        a2_diff = deg_diff(a2, dst_node['aspect_raw'])
        dst_node["aspect"] = a1 if a1_diff < a2_diff else a2
    elif 'aspect' in src_node:
        dst_node["aspect"] = src_node.get('aspect')


def _hierarchical_merge(graph, labels, thresh: float = 0):
    merge_hierarchical(labels, graph, thresh=thresh, rag_copy=False,
                       in_place_merge=True,
                       merge_func=_update_node_data,
                       weight_func=_new_edge_weight)

    merged_planes = {}
    for n in graph.nodes:
        plane = graph.nodes[n]
        if plane['outlier'] is False:
            # skimage and networkx seem to have different ideas about which the final label
            # of a merged plane is...:
            labels[np.isin(labels, plane['labels'])] = n
            del plane["xy_subset"]
            del plane["z_subset"]
            del plane["aspect_subset"]
            del plane["labels"]
            del plane["xy_local"]
            del plane["plane_sums"]
            merged_planes[n] = plane

    return merged_planes, labels


def _node_points(xy_subset: np.ndarray, z_subset: np.ndarray, aspect_subset: np.ndarray,
                 origin: np.ndarray) -> dict:
    """The attributes of a RAG node holding its pixels, including those `_MergedFit` needs."""
    xy_local = xy_subset - origin
    return {'xy_subset': xy_subset,
            'z_subset': z_subset,
            'aspect_subset': aspect_subset,
            'xy_local': xy_local,
            'plane_sums': PlaneSums.of(xy_local, z_subset)}


def _rag_score(xy, z, aspect, labels, planes: Dict[int, RoofPlane], res: float, nodata: int, connectivity: int = 1):
    label_image, idxs = _image(xy, labels, res, nodata=nodata)
    graph = RAG(label_image, connectivity=connectivity)
    if graph.has_node(nodata):
        graph.remove_node(nodata)

    # RAGs are constructed using edges, so if there are no edges it will make
    # an empty graph
    if graph.number_of_nodes() == 0 and len(planes) == 1:
        for plane_idx in planes.keys():
            graph.add_node(plane_idx)

    # `PlaneSums` need coordinates near 0:
    origin = xy.min(axis=0)
    for n in graph:
        mask = label_image == n
        xy_subset = xy[idxs[mask]]
        z_subset = z[idxs[mask]]
        graph.nodes[n].update({'labels': [n],
                               **_node_points(xy_subset, z_subset, aspect[idxs[mask]], origin),
                               'res': res,
                               'outlier': True})
        if n in planes:
            graph.nodes[n].update(planes[n])
            graph.nodes[n]['outlier'] = False

    for node_1_id, node_2_id, edge in graph.edges(data=True):
        edge['weight'] = _edge_weight(graph, node_2_id, node_1_id)

    return graph


def merge_adjacent(xy, z, aspect, labels, planes: Dict[int, RoofPlane],
                   res: float, nodata: int,
                   connectivity: int = 1, thresh: float = 0,
                   debug: bool = False) -> Tuple[Dict[int, RoofPlane], np.ndarray]:
    """
    Create a RAG (region adjacency graph) where the nodes are either a plane, or a single
    pixel that has not been fitted to any plane.
    Then hierarchically merge nodes in the RAG whenever the edge between the two nodes
    has a weight less than `thresh`.

    The weight of each edge is
     * for an edge between 2 planes: the weighted average score of the 2 planes minus the
     score of a plane that is fit to all the inliers of both planes.
     * for an edge between a plane and an outlier: the score of the plane minus the score
     of a plane fit to all inliers of the plane and the outlier.

    Any edge with weight under `thresh` indicates 2 regions that should be merged.
    """
    if thresh >= DO_NOT_MERGE:
        raise ValueError(f"threshold ({thresh}) was >= DO_NOT_MERGE ({DO_NOT_MERGE})")

    g = _rag_score(xy, z, aspect, labels, planes, res, nodata, connectivity=connectivity)

    if debug:
        print(f"Constructed graph with {len(planes)} planes, {g.number_of_nodes()} nodes, {g.number_of_edges()} edges")

    return _hierarchical_merge(g, labels, thresh=thresh)
