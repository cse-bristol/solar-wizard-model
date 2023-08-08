from typing import Dict

import numpy as np
from skimage.future.graph import RAG, merge_hierarchical
from sklearn import metrics
from sklearn.linear_model import LinearRegression

from solar_pv.constants import ROOFDET_GOOD_SCORE
from solar_pv.ransac.premade_planes import _image
from solar_pv.ransac.ransac import _slope, _aspect


DO_NOT_MERGE = 9999


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
        curr_score = ((dst_node['score'] * dst_inliers) +
                      (src_node['score'] * src_inliers)) / (dst_inliers + src_inliers)

        xy_subset = np.concatenate([dst_node['xy_subset'], src_node['xy_subset']])
        z_subset = np.concatenate([dst_node['z_subset'], src_node['z_subset']])
        lr = LinearRegression()
        lr.fit(xy_subset, z_subset)
        # new_score = lr.score(xy_subset, z_subset)
        new_score = metrics.mean_absolute_error(z_subset, lr.predict(xy_subset))
        # If the new score is still good enough, don't require it to be better than before
        weight = new_score - curr_score if new_score > ROOFDET_GOOD_SCORE else -1

    # A plane and an outlier
    else:
        curr_score = dst_node.get('score', src_node.get('score'))
        xy_subset = np.concatenate([dst_node['xy_subset'], src_node['xy_subset']])
        z_subset = np.concatenate([dst_node['z_subset'], src_node['z_subset']])
        lr = LinearRegression()
        lr.fit(xy_subset, z_subset)
        # new_score = lr.score(xy_subset, z_subset)
        new_score = metrics.mean_absolute_error(z_subset, lr.predict(xy_subset))
        weight = new_score - curr_score

    return weight


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
    lr = LinearRegression()
    lr.fit(xy_subset, z_subset)
    z_pred = lr.predict(xy_subset)
    # merged_score = lr.score(xy_subset, z_subset)
    merged_score = metrics.mean_absolute_error(z_subset, z_pred)

    dst_node['toid'] = dst_node.get('toid', src_node.get('toid'))
    dst_node['xy_subset'] = xy_subset
    dst_node['z_subset'] = z_subset
    dst_node['score'] = merged_score

    dst_node['x_coef'] = lr.coef_[0]
    dst_node['y_coef'] = lr.coef_[1]
    dst_node['intercept'] = lr.intercept_
    dst_node['slope'] = _slope(lr.coef_[0], lr.coef_[1])
    dst_node['aspect'] = _aspect(lr.coef_[0], lr.coef_[1])
    dst_node['inliers_xy'] = xy_subset

    if dst_node['outlier'] is src_node['outlier'] is False:
        dst_node['plane_type'] = dst_node['plane_type'] + "_MERGED_" + src_node['plane_type']
    else:
        dst_node['plane_type'] = dst_node.get('plane_type', src_node.get('plane_type'))

    dst_node['outlier'] = False

    dst_node["r2"] = metrics.r2_score(z_subset, z_pred)
    dst_node["mae"] = merged_score
    dst_node["mse"] = metrics.mean_squared_error(z_subset, z_pred)
    dst_node["rmse"] = metrics.mean_squared_error(z_subset, z_pred, squared=False)
    dst_node["msle"] = metrics.mean_squared_log_error(z_subset, z_pred)
    dst_node["mape"] = metrics.mean_absolute_percentage_error(z_subset, z_pred)

    # TODO:
    dst_node["sd"] = 0
    dst_node["aspect_circ_mean"] = 0
    dst_node["aspect_circ_sd"] = 0
    dst_node["thinness_ratio"] = 0
    dst_node["cv_hull_ratio"] = 0


def _hierarchical_merge(graph, labels, thresh: float = 0):
    labels2 = merge_hierarchical(labels, graph, thresh=thresh, rag_copy=False,
                                 in_place_merge=True,
                                 merge_func=_update_node_data,
                                 weight_func=_new_edge_weight)

    merged_planes = []
    for n in graph.nodes:
        plane = graph.nodes[n]
        if plane['outlier'] is False:
            del plane["xy_subset"]
            del plane["z_subset"]
            del plane["labels"]
            merged_planes.append(plane)

    return merged_planes


def _rag_score(xy, z, labels, planes: Dict[int, dict], res: float, nodata: int, connectivity: int = 1):
    label_image, idxs = _image(xy, labels, res, nodata=nodata)
    graph = RAG(label_image, connectivity=connectivity)
    if graph.has_node(nodata):
        graph.remove_node(nodata)

    for n in graph:
        mask = label_image == n
        xy_subset = xy[idxs[mask]]
        z_subset = z[idxs[mask]]
        graph.nodes[n].update({'labels': [n],
                               'xy_subset': xy_subset,
                               'z_subset': z_subset,
                               'outlier': True})
        if n in planes:
            graph.nodes[n].update(planes[n])
            graph.nodes[n]['outlier'] = False

    for node_1_id, node_2_id, edge in graph.edges(data=True):
        edge['weight'] = _edge_weight(graph, node_2_id, node_1_id)

    return graph


def merge_adjacent(xy, z, labels, planes: Dict[int, dict],
                   res: float, nodata: int, connectivity: int = 1, thresh: float = 0):
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
    g = _rag_score(xy, z, labels, planes, res, nodata, connectivity=connectivity)
    return _hierarchical_merge(g, labels, thresh=thresh)
