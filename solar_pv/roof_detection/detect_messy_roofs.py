from typing import Dict, List

import numpy as np
from networkx import Graph
from skimage import measure
from skimage.future.graph import RAG

from solar_pv.datatypes import RoofPlane
from solar_pv.roof_detection.premade_planes import _image

_EDGE_OF_BUILDING = -1
_OUTLIER = 1
_NODATA = -9999

_MESS_THRESHOLD_PCT = 0.14
_TOTAL_MESS_THRESHOLD_PCT = 0.85


def _obstacle_groups_img(planes: Dict[int, RoofPlane], labels, xy, res: float, connectivity: int):
    """
    Make an image containg both the 'obstacle groups' (connected groups of outliers that
    do not touch the edge of the building) and the planes.
    """
    is_outlier = np.full(labels.shape, _EDGE_OF_BUILDING)
    is_outlier[~np.isin(labels, list(planes.keys()))] = _OUTLIER

    outlier_img, _ = _image(xy, is_outlier, res, _OUTLIER)

    # pad by 1 so that areas of outliers that connect to the edge of the building
    # don't count as an obstacle group:
    obstacle_groups_img = measure.label(np.pad(outlier_img, 1, constant_values=_OUTLIER),
                                        background=_EDGE_OF_BUILDING,
                                        connectivity=connectivity)
    obstacle_groups_img[obstacle_groups_img == obstacle_groups_img[0, 0]] = _EDGE_OF_BUILDING
    # un-pad by 1:
    obstacle_groups_img = obstacle_groups_img[1:-1, 1:-1]
    obstacle_groups_img[obstacle_groups_img != _EDGE_OF_BUILDING] += max(planes.keys())

    new_label_img, _ = _image(xy, labels, res, _NODATA)
    for plane_idx in planes.keys():
        obstacle_groups_img[new_label_img == plane_idx] = plane_idx

    return obstacle_groups_img


def _rag(planes: Dict[int, RoofPlane], obstacle_groups_img, connectivity: int) -> Graph:
    graph = RAG(obstacle_groups_img, connectivity=connectivity)
    if graph.has_node(_NODATA):
        graph.remove_node(_NODATA)
    if graph.has_node(_EDGE_OF_BUILDING):
        graph.remove_node(_EDGE_OF_BUILDING)

    # RAGs are constructed using edges, so if there are no edges it will make
    # an empty graph
    if graph.number_of_nodes() == 0 and len(planes) == 1:
        for plane_idx in planes.keys():
            graph.add_node(plane_idx)

    for n in graph:
        mask = obstacle_groups_img == n
        graph.nodes[n].update({'labels': [n],
                               'obstacle_group': True,
                               'inliers': np.count_nonzero(mask)})
        if n in planes:
            graph.nodes[n].update(planes[n])
            graph.nodes[n]['obstacle_group'] = False
    return graph


def _mess_score(graph: Graph, n: int) -> int:
    """
    Each flat plane is scored according to the sum of the size of each obstacle group
    it connects to:
    """
    mess_score = 0
    for neighbour_idx in graph.neighbors(n):
        neighbour = graph.nodes[neighbour_idx]
        if neighbour['obstacle_group'] is False:
            continue
        # obstacle_group_valid = all([graph.nodes[_n].get('is_flat') for _n in graph.neighbors(neighbour_idx)])
        # if obstacle_group_valid:
        #     mess_score += neighbour['inliers']
        mess_score += neighbour['inliers']
    return mess_score


def detect_messy_roofs(planes: Dict[int, RoofPlane], labels, xy, res: float, debug: bool = False) -> List[RoofPlane]:
    """
    Try and detect flat roofs covered in obstacles (pipes, air con etc).

    Each flat plane is scored based on the number of groups of outlier pixels it
    touches that do not touch the edge of the building.
    The intuition behind this is that the result of roof plane detection on these kind
    of roofs is often either:
    * one flat roof plane with many holes in
    * several touching flat roof planes with holes, and also holes formed by the gaps
      between the flat roof planes.

    Good flat roofs tend to lead directly on to the edge of the building, or there may
    be a layer of outliers between it and the edge of the building.

    If more than a threshold percentage of the pixels are either in an obstacle group
    or a flat roof that has been rejected for mess, reject the whole building.
    """
    has_flat_roofs = any(p['is_flat'] for p in planes.values())
    if not has_flat_roofs:
        return list(planes.values())

    obstacle_groups_img = _obstacle_groups_img(planes, labels, xy, res, connectivity=1)

    graph = _rag(planes, obstacle_groups_img, connectivity=1)

    if debug:
        print(f"Constructed graph with {len(planes)} planes, {graph.number_of_nodes()} nodes, {graph.number_of_edges()} edges")

    planes = []
    total_inliers = 0
    mess_inliers = 0
    for n in graph:
        node = graph.nodes[n]
        size = node['inliers']
        total_inliers += size
        if node['obstacle_group'] is True:
            mess_inliers += size
            continue
        if node['is_flat'] is True:
            mess_score = _mess_score(graph, n)
            mess_score_pct = mess_score / size
            if debug:
                print(f"plane {n} had raw score {mess_score}, pct {mess_score_pct}, threshold is {_MESS_THRESHOLD_PCT}")
            if mess_score_pct < _MESS_THRESHOLD_PCT:
                planes.append(node)
            else:
                mess_inliers += size
        else:
            planes.append(node)

    total_mess_score = mess_inliers / total_inliers if total_inliers > 0 else 0
    if debug:
        print(f"total mess score: {total_mess_score}")
    if total_mess_score >= _TOTAL_MESS_THRESHOLD_PCT:
        if debug:
            print(f"rejecting whole building due to total mess score: {total_mess_score}")
        return []

    return planes
