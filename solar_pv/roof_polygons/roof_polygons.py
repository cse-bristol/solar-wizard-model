# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import json
import traceback
from collections import defaultdict
from typing import List, Optional, cast, Tuple

import math
import numpy as np
from shapely import affinity, ops, set_precision
from shapely.geometry import Polygon, CAP_STYLE, JOIN_STYLE, MultiPolygon
from shapely.validation import make_valid

from solar_pv.geos import square, largest_polygon, get_grid_cells, \
    de_zigzag
from solar_pv.datatypes import RoofPlane, RoofPolygon
from solar_pv.roof_polygons.split_evenly import split_evenly


def create_roof_polygons(building_geom: Polygon,
                         planes: List[RoofPlane],
                         max_roof_slope_degrees: int,
                         min_roof_area_m: int,
                         min_roof_degrees_from_north: int,
                         flat_roof_degrees: int,
                         min_dist_to_edge_m: float,
                         resolution_metres: float) -> List[RoofPolygon]:
    """Add roof polygons and other related fields to the dicts in `planes`"""
    return _create_roof_polygons(
        building_geom,
        planes,
        max_roof_slope_degrees=max_roof_slope_degrees,
        min_roof_area_m=min_roof_area_m,
        min_roof_degrees_from_north=min_roof_degrees_from_north,
        flat_roof_degrees=flat_roof_degrees,
        min_dist_to_edge_m=min_dist_to_edge_m,
        resolution_metres=resolution_metres)


def _create_roof_polygons(building_geom: Polygon,
                          planes: List[RoofPlane],
                          max_roof_slope_degrees: int,
                          min_roof_area_m: int,
                          min_roof_degrees_from_north: int,
                          flat_roof_degrees: int,
                          min_dist_to_edge_m: float,
                          resolution_metres: float,
                          debug: bool = False) -> List[RoofPolygon]:

    if len(planes) == 0:
        return []

    try:
        plane_polys: List[RoofPolygon] = []
        for plane in planes:
            plane = cast(RoofPolygon, plane)
            is_flat = plane['is_flat']
            # TODO ideally store actual slope and panel_slope as separate things
            plane['slope'] = flat_roof_degrees if is_flat else plane['slope']

            raw_roof_poly, roof_poly = _make_polygon(plane, building_geom, min_dist_to_edge_m, resolution_metres)

            if roof_poly and not roof_poly.is_empty:
                plane['roof_geom_27700'] = roof_poly
                plane['roof_geom_raw_27700'] = raw_roof_poly
                plane_polys.append(plane)

        if debug:
            print(f"Made {len(plane_polys)} initial roof polygons")

        plane_polys = _merge_touching(plane_polys, building_geom, min_dist_to_edge_m, resolution_metres)

        if debug:
            print(f"Merged touching, now have {len(plane_polys)} roof polygons")

        for p in plane_polys:
            p['roof_geom_27700'] = largest_polygon(set_precision(p['roof_geom_27700'], 0.01))

        _remove_overlaps(plane_polys, debug=debug)

        if debug:
            print(f"Removed overlaps")

        for plane in plane_polys:
            roof_poly = plane['roof_geom_27700']
            plane['roof_geom_raw_27700'] = largest_polygon(plane['roof_geom_raw_27700'].intersection(roof_poly))

            # Arbitrarily use the area of roof_geom_27700 rather than roof_geom_raw_27700...
            area = roof_poly.area / math.cos(math.radians(plane['slope']))

            # Set usability:
            if plane['slope'] > max_roof_slope_degrees:
                plane['usable'] = False
                plane['not_usable_reason'] = "SLOPE"
            elif plane['aspect'] < min_roof_degrees_from_north:
                plane['usable'] = False
                plane['not_usable_reason'] = "ASPECT"
            elif plane['aspect'] > 360 - min_roof_degrees_from_north:
                plane['usable'] = False
                plane['not_usable_reason'] = "ASPECT"
            elif area < min_roof_area_m:
                plane['usable'] = False
                plane['not_usable_reason'] = "AREA"
            else:
                plane['usable'] = True

        return plane_polys

    except Exception as e:
        toid = planes[0]['toid']
        print(f"Exception during roof polygon creation for TOID {toid}:")
        traceback.print_exception(e)
        print(json.dumps(_to_test_data(planes[0]['toid'], planes, building_geom),
                         sort_keys=True, default=str))
        raise e


def _make_polygon(plane: RoofPlane,
                  building_geom: Polygon,
                  min_dist_to_edge_m: float,
                  resolution_metres: float,
                  max_area_diff: float = 5,
                  max_area_diff_pct: float = 0.35) -> Tuple[Optional[Polygon], Optional[Polygon]]:
    # Make the initial roof polygon (basically by just drawing round
    # all the pixels and then de-zigzagging)
    halfr = resolution_metres / 2
    r = resolution_metres
    pixels = [square(xy[0] - halfr, xy[1] - halfr, r) for xy in plane['inliers_xy']]
    geom = ops.unary_union(pixels)
    geom = de_zigzag(geom)
    raw_roof_poly = largest_polygon(geom)
    raw_roof_poly = _constrain_to_building(building_geom, raw_roof_poly, min_dist_to_edge_m)

    if not raw_roof_poly or raw_roof_poly.is_empty:
        return None, None

    # If a bbox rotated to match the aspect is close enough area-wise to the initial
    # polygon, just use that:
    rotated: Polygon = affinity.rotate(raw_roof_poly, plane['aspect'], origin=raw_roof_poly.centroid)
    bbox = rotated.envelope
    area_diff = bbox.area - rotated.area
    if area_diff < max_area_diff and area_diff / rotated.area < max_area_diff_pct:
        roof_poly = affinity.rotate(bbox, -plane['aspect'], origin=raw_roof_poly.centroid)
    else:
        # Otherwise grid it based on a grid oriented to the aspect and then de-zigzag
        roof_poly = _grid_polygon(raw_roof_poly, plane['aspect'], grid_size=1.0)
        roof_poly = de_zigzag(roof_poly)

    roof_poly = _constrain_to_building(building_geom, roof_poly, min_dist_to_edge_m)

    return largest_polygon(raw_roof_poly), largest_polygon(roof_poly)


def _grid_polygon(roof_poly: Polygon, aspect: float, grid_size: float):
    # TODO maybe this should require that the grid cell intersection with roof_poly
    #      is over a certain amount?
    centroid = roof_poly.centroid

    # Rotate the roof area CCW by aspect, to be gridded easily:
    plane_points = affinity.rotate(roof_poly, aspect, origin=centroid)

    # grid_size = math.sqrt(resolution_metres ** 2 * 2.0)
    grid = get_grid_cells(plane_points, grid_size, grid_size, 0, 0, grid_start='bounds')
    grid = affinity.rotate(MultiPolygon(grid), -aspect, origin=centroid).geoms
    roof_poly = ops.unary_union(grid)
    roof_poly = make_valid(roof_poly)
    return roof_poly


def _constrain_to_building(building_geom: Polygon,
                           roof_poly: Polygon,
                           min_dist_to_edge_m: float) -> Optional[Polygon]:
    if not roof_poly or roof_poly.is_empty:
        return None

    neg_buffer = -min_dist_to_edge_m
    building_geom_shrunk = building_geom.buffer(
        neg_buffer, cap_style=CAP_STYLE.square, join_style=JOIN_STYLE.mitre)
    roof_poly = roof_poly.intersection(building_geom_shrunk)
    roof_poly = largest_polygon(roof_poly)
    return roof_poly


# def remove_tendrils(roof_poly: Polygon, buf: float = 0.6) -> Optional[Polygon]:
#     splitter = roof_poly.buffer(-buf, cap_style=CAP_STYLE.square, join_style=JOIN_STYLE.mitre, resolution=1)
#     splitter = splitter.buffer(buf, cap_style=CAP_STYLE.square, join_style=JOIN_STYLE.mitre, resolution=1)
#     splitter = largest_polygon(splitter).exterior
#     roof_poly = largest_polygon(split_with(roof_poly, splitter))
#     return roof_poly


def _merge_touching(planes: List[RoofPolygon],
                    building_geom: Polygon,
                    min_dist_to_edge_m: float,
                    resolution_metres: float,
                    max_slope_diff: int = 4) -> List[RoofPolygon]:
    """
    Merge any planes that
    a) have the same aspect
    b) have a slope that differs less than `max_slope_diff`
    c) intersect each other
    d) are not flat
    """
    merged = []
    _by_aspect = defaultdict(list)
    for plane in planes:
        aspect = plane['aspect']
        _by_aspect[aspect].append(plane)

    for plane_group in _by_aspect.values():
        if len(plane_group) == 1:
            merged.append(plane_group[0])
            continue

        checked = set()
        for plane in plane_group:
            if id(plane) in checked:
                continue
            checked.add(id(plane))

            slope = plane['slope']
            poly = plane['roof_geom_27700']
            mergeable = []
            for p in plane_group:
                if id(p) not in checked \
                        and p['is_flat'] is False \
                        and abs(p['slope'] - slope) <= max_slope_diff \
                        and p['roof_geom_27700'].intersects(poly):
                    mergeable.append(p)

            if len(mergeable) > 0:
                for to_merge in mergeable:
                    checked.add(id(to_merge))

                plane['inliers_xy'] = np.concatenate([p['inliers_xy'] for p in mergeable] + [plane['inliers_xy']])
                raw_roof_poly, roof_poly = _make_polygon(plane, building_geom, min_dist_to_edge_m, resolution_metres)
                plane['roof_geom_27700'] = roof_poly
                plane['roof_geom_raw_27700'] = raw_roof_poly

            if plane['roof_geom_27700']:
                merged.append(plane)

    return merged


def _remove_overlaps(roof_polygons: List[RoofPolygon], debug: bool = False) -> None:
    # TODO maybe have an rtree here?
    for i, rp1 in enumerate(roof_polygons):
        for j, rp2 in enumerate(roof_polygons):
            if i > j:
                p1, p2 = split_evenly(rp1['roof_geom_27700'], rp2['roof_geom_27700'], debug=debug)
                rp1['roof_geom_27700'] = p1
                rp2['roof_geom_27700'] = p2


def _to_test_data(toid: str, planes: List[dict], building_geom: Polygon) -> dict:
    planes_ = []
    for plane in planes:
        plane = plane.copy()
        plane["inliers_xy"] = list(map(tuple, plane["inliers_xy"]))
        if "inliers" in plane:
            del plane["inliers"]
        planes_.append(plane)

    return {
        "planes": planes_,
        "building_geom": building_geom.wkt,
        "toid": toid,
    }
