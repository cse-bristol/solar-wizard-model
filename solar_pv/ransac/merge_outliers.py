from typing import Dict

import numpy as np
from skimage.future.graph import RAG, merge_hierarchical
from sklearn.linear_model import LinearRegression

from solar_pv.ransac.premade_planes import _image
from solar_pv.ransac.ransac import _slope, _aspect


DO_NOT_MERGE = 9999


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
    dst_node = graph.nodes[dst]
    n_node = graph.nodes[n]

    if dst_node['outlier'] == n_node['outlier']:
        return {"weight": DO_NOT_MERGE}

    if dst_node['outlier'] is False and n_node['outlier'] is True:
        plane = dst_node
        outlier = n_node
    else:
        plane = n_node
        outlier = dst_node

    curr_score = plane['score']
    xy_subset = np.concatenate([plane['xy_subset'], outlier['xy_subset']])
    z_subset = np.concatenate([plane['z_subset'], outlier['z_subset']])
    lr = LinearRegression()
    lr.fit(xy_subset, z_subset)
    new_score = lr.score(xy_subset, z_subset)
    weight = curr_score - new_score
    return {'weight': weight}


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
    print(f"merging nodes {src} {dst}")
    dst_node = graph.nodes[dst]
    src_node = graph.nodes[src]

    dst_node['outlier'] = False

    xy_subset = np.concatenate([dst_node['xy_subset'], src_node['xy_subset']])
    z_subset = np.concatenate([dst_node['z_subset'], src_node['z_subset']])
    lr = LinearRegression()
    lr.fit(xy_subset, z_subset)
    merged_score = lr.score(xy_subset, z_subset)

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
    dst_node['plane_type'] = dst_node.get('plane_type', src_node.get('plane_type'))

    # TODO:
    dst_node["sd"] = None
    dst_node["aspect_circ_mean"] = None
    dst_node["aspect_circ_sd"] = None
    dst_node["thinness_ratio"] = None
    dst_node["cv_hull_ratio"] = None


def merge_adjacent_outliers(xy, z, labels, planes: Dict[int, dict], res: float, nodata: int, connectivity: int = 1, thresh: float = 0):
    if thresh >= DO_NOT_MERGE:
        raise ValueError(f"threshold ({thresh}) was >= DO_NOT_MERGE ({DO_NOT_MERGE})")
    g = rag_score(xy, z, labels, planes, res, nodata, connectivity=connectivity)
    return hierarchical_merge(g, labels, thresh=thresh)


def hierarchical_merge(graph, labels, thresh: float = 0):
    """
    Merge nodes in the plane graph (created by function `rag_score`) hierarchically
    whenever the edge between the two nodes has a weight greater than `thresh`.

    The default value of 0 means that nodes will only be merged when the score of a
    plane fit to all inliers of both planes is higher than the average score of both
    planes.
    """

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


def rag_score(xy, z, labels, planes: Dict[int, dict], res: float, nodata: int, connectivity: int = 1):
    """
    Create a RAG (region adjacency graph) where the regions are defined by the pixel inliers
    of each roof plane found on a building.

    The weight of each edge is the average score of the 2 planes minus the score of
    a plane that is fit to all the inliers of both planes. Any edge with weight
    under 0 therefore indicates 2 planes that should be merged.
    """
    label_image, idxs = _image(xy, labels, res, nodata=nodata)
    graph = RAG(label_image, connectivity=connectivity)
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
        n1 = graph.nodes[node_1_id]
        n2 = graph.nodes[node_2_id]
        if n1['outlier'] == n2['outlier']:
            edge['weight'] = DO_NOT_MERGE
            continue

        if n1['outlier'] is False and n2['outlier'] is True:
            plane = n1
            outlier = n2
        else:
            plane = n2
            outlier = n1

        curr_score = plane['score']
        xy_subset = np.concatenate([plane['xy_subset'], outlier['xy_subset']])
        z_subset = np.concatenate([plane['z_subset'], outlier['z_subset']])
        lr = LinearRegression()
        lr.fit(xy_subset, z_subset)
        new_score = lr.score(xy_subset, z_subset)
        weight = curr_score - new_score
        edge['weight'] = weight

    return graph
