# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import json
import traceback
from collections import defaultdict
from typing import List, Dict, Optional

import math
import numpy as np
from psycopg2.sql import Identifier
import psycopg2.extras
from shapely import wkt, affinity, ops
from shapely.geometry import LineString, Polygon, CAP_STYLE, JOIN_STYLE, MultiPoint, \
    MultiPolygon
from shapely.prepared import prep
from shapely.validation import make_valid

from solar_pv import tables
from solar_pv.db_funcs import connection, sql_command
from solar_pv.geos import azimuth, square, largest_polygon, get_grid_cells
from solar_pv.constants import FLAT_ROOF_DEGREES_THRESHOLD, \
    FLAT_ROOF_AZIMUTH_ALIGNMENT_THRESHOLD, AZIMUTH_ALIGNMENT_THRESHOLD
from solar_pv.roof_polygons.roof_polygon_archetypes import construct_archetypes, \
    get_archetype


def create_roof_polygons(pg_uri: str,
                         job_id: int,
                         planes: List[dict],
                         max_roof_slope_degrees: int,
                         min_roof_area_m: int,
                         min_roof_degrees_from_north: int,
                         flat_roof_degrees: int,
                         large_building_threshold: float,
                         min_dist_to_edge_m: float,
                         min_dist_to_edge_large_m: float,
                         panel_width_m: float,
                         panel_height_m: float,
                         resolution_metres: float) -> List[dict]:
    """Add roof polygons and other related fields to the dicts in `planes`"""
    toids = list({plane['toid'] for plane in planes})
    building_geoms = _building_geoms(pg_uri, job_id, toids)
    return _create_roof_polygons(
        building_geoms,
        planes,
        max_roof_slope_degrees=max_roof_slope_degrees,
        min_roof_area_m=min_roof_area_m,
        min_roof_degrees_from_north=min_roof_degrees_from_north,
        flat_roof_degrees=flat_roof_degrees,
        large_building_threshold=large_building_threshold,
        min_dist_to_edge_m=min_dist_to_edge_m,
        min_dist_to_edge_large_m=min_dist_to_edge_large_m,
        panel_width_m=panel_width_m,
        panel_height_m=panel_height_m,
        resolution_metres=resolution_metres)


def _create_roof_polygons(building_geoms: Dict[str, Polygon],
                          planes: List[dict],
                          max_roof_slope_degrees: int,
                          min_roof_area_m: int,
                          min_roof_degrees_from_north: int,
                          flat_roof_degrees: int,
                          large_building_threshold: float,
                          min_dist_to_edge_m: float,
                          min_dist_to_edge_large_m: float,
                          panel_width_m: float,
                          panel_height_m: float,
                          resolution_metres: float) -> List[dict]:
    polygons_by_toid = defaultdict(list)
    roof_polygons = []
    # TODO these will have to be based on a different size system, not panel w/h
    archetypes = construct_archetypes(panel_width_m, panel_height_m)
    max_archetype_area = max(archetypes, key=lambda a: a.polygon.area).polygon.area

    # Sort planes so that southerly aspects are considered first
    # (as already-created polygons take priority when ensuring two roof planes don't overlap)
    planes.sort(key=lambda p: (p['toid'], abs(180 - p['aspect'])))

    plane_polys = []
    for plane in planes:
        toid = plane['toid']
        building_geom = building_geoms[toid]
        try:
            # set is_flat, update slope and aspect of flat roofs
            is_flat = plane['slope'] <= FLAT_ROOF_DEGREES_THRESHOLD
            plane['is_flat'] = is_flat
            if is_flat:
                plane['slope'] = flat_roof_degrees

            # update orientations
            orientations = _building_orientations(building_geom)
            aspect_adjusted = False
            if not is_flat:
                for orientation in orientations:
                    if abs(orientation - plane['aspect']) < AZIMUTH_ALIGNMENT_THRESHOLD:
                        plane['aspect'] = orientation
                        aspect_adjusted = True
                        break
            else:
                for orientation in orientations:
                    if abs(orientation - 180) < FLAT_ROOF_AZIMUTH_ALIGNMENT_THRESHOLD:
                        plane['aspect'] = orientation
                        aspect_adjusted = True
                        break
            plane['aspect_adjusted'] = aspect_adjusted

            roof_poly = _initial_polygon(plane, resolution_metres)
            if not roof_poly or roof_poly.is_empty:
                continue

            plane['roof_poly'] = roof_poly
            plane_polys.append(plane)

        except Exception as e:
            traceback.print_exception(e)
            print(json.dumps(_to_test_data(toid, [plane], building_geom),
                             sort_keys=True, default=str))
            raise e

    plane_polys = _merge_touching(plane_polys, resolution_metres)

    for plane in plane_polys:
        toid = plane['toid']
        building_geom = building_geoms[toid]
        try:
            is_flat = plane['is_flat']
            roof_poly = plane['roof_poly']
            aspect_adjusted = plane['aspect_adjusted']
            del plane['roof_poly']

            # Potentially use a pre-made roof archetype instead:
            plane['archetype'] = False
            if not is_flat \
                    and aspect_adjusted \
                    and roof_poly.area < (max_archetype_area * 1.5)\
                    and roof_poly.area / building_geom.area > 0.14:

                roof_poly = _constrain_to_building(building_geom,
                                                   roof_poly,
                                                   large_building_threshold,
                                                   min_dist_to_edge_large_m,
                                                   min_dist_to_edge_m)
                if not roof_poly or roof_poly.is_empty:
                    continue

                roof_poly = _remove_overlaps(toid, roof_poly, polygons_by_toid)
                if not roof_poly or roof_poly.is_empty:
                    continue

                # archetype = get_archetype(roof_poly, archetypes, plane['aspect'])
                archetype = None
                if archetype is not None:
                    roof_poly = archetype.polygon
                    plane['archetype'] = True
                    plane['archetype_pattern'] = json.dumps(archetype.pattern)

                    roof_poly = _constrain_to_building(building_geom,
                                                       roof_poly,
                                                       large_building_threshold,
                                                       min_dist_to_edge_large_m,
                                                       min_dist_to_edge_m)
                    if not roof_poly or roof_poly.is_empty:
                        continue

                    roof_poly = _remove_overlaps(toid, roof_poly, polygons_by_toid)
                    if not roof_poly or roof_poly.is_empty:
                        continue

                    roof_poly = remove_tendrils(roof_poly)

            if plane['archetype'] is False:
                roof_poly = _grid_polygon(plane, resolution_metres)

                roof_poly = _constrain_to_building(building_geom,
                                                   roof_poly,
                                                   large_building_threshold,
                                                   min_dist_to_edge_large_m,
                                                   min_dist_to_edge_m)
                if not roof_poly or roof_poly.is_empty:
                    continue

                roof_poly = _remove_overlaps(toid, roof_poly, polygons_by_toid)
                if not roof_poly or roof_poly.is_empty:
                    continue

                # TODO is this working?
                roof_poly = remove_tendrils(roof_poly, resolution_metres)

            if not roof_poly or roof_poly.is_empty:
                continue

            # any other planes in the same toid will now not be allowed to overlap this one:
            polygons_by_toid[toid].append(roof_poly)

            # TODO if aspect not adjusted, set not usable?
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
            elif roof_poly.area < min_roof_area_m and plane['archetype'] is False:
                plane['usable'] = False
                plane['not_usable_reason'] = "AREA"
            else:
                plane['usable'] = True

            # Add other info:
            plane['roof_geom_27700'] = roof_poly.wkt
            easting, northing = roof_poly.centroid.xy
            plane['easting'] = easting[0]
            plane['northing'] = northing[0]
            plane['raw_footprint'] = roof_poly.area
            plane['raw_area'] = roof_poly.area / math.cos(math.radians(plane['slope']))
            roof_polygons.append(plane)
        except Exception as e:
            traceback.print_exception(e)
            print(json.dumps(_to_test_data(toid, [plane], building_geom),
                             sort_keys=True, default=str))
            raise e

    return roof_polygons


def _initial_polygon(plane, resolution_metres):
    """
    Make the initial roof polygon (basically by just drawing round
    all the pixels rotated to match the aspect)
    """
    pixels = []
    for p in plane['inliers_xy']:
        # Draw a square around the pixel centre:
        edge = math.sqrt(resolution_metres ** 2 * 2.0) / 2
        pixel = square(p[0] - edge, p[1] - edge, edge * 2)

        # Rotate the square to align with plane aspect:
        pixel = affinity.rotate(pixel, -plane['aspect'])
        pixels.append(pixel)
    neg_buffer = -((math.sqrt(resolution_metres ** 2 * 2.0) - resolution_metres) / 2)
    roof_poly = ops.unary_union(pixels).buffer(neg_buffer,
                                               cap_style=CAP_STYLE.square,
                                               join_style=JOIN_STYLE.mitre,
                                               resolution=1)

    roof_poly = largest_polygon(roof_poly)
    return roof_poly


def _grid_polygon(plane, resolution_metres):
    # Rotate the roof area CCW by aspect, to be gridded easily:
    aspect = plane['aspect']
    plane_points = MultiPoint(plane['inliers_xy'])
    centroid = plane_points.centroid

    plane_points = affinity.rotate(plane_points, aspect, origin=centroid)

    grid_size = math.sqrt(resolution_metres ** 2 * 2.0)
    grid = get_grid_cells(plane_points, grid_size, grid_size, 0, 0, grid_start='bounds')
    grid = affinity.rotate(MultiPolygon(grid), -aspect, origin=centroid).geoms
    roof_poly = ops.unary_union(grid)
    return roof_poly


def remove_tendrils(roof_poly: Polygon, buf: float = 0.6) -> Optional[Polygon]:
    splitter = roof_poly.buffer(-buf, cap_style=CAP_STYLE.square, join_style=JOIN_STYLE.mitre, resolution=1)
    splitter = splitter.buffer(buf, cap_style=CAP_STYLE.square, join_style=JOIN_STYLE.mitre, resolution=1)
    splitter = largest_polygon(splitter).exterior
    roof_poly = largest_polygon(split_with(roof_poly, splitter))
    return roof_poly


def split_with(poly, splitter):
    """Split a Polygon with a LineString or LinearRing"""

    union = poly.boundary.union(splitter)
    poly = prep(poly)
    return MultiPolygon([pg for pg in ops.polygonize(union) if poly.contains(pg.representative_point())])


def _merge_touching(planes, resolution_metres: float, max_slope_diff: int = 4) -> List[dict]:
    """
    Merge any planes that
    a) have the same aspect
    b) have a slope that differs less than `max_slope_diff`
    c) intersect each other
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
            poly = plane['roof_poly']
            mergeable = [p for p in plane_group if id(p) not in checked and abs(p['slope'] - slope) <= max_slope_diff and p['roof_poly'].intersects(poly)]

            for to_merge in mergeable:
                checked.add(id(to_merge))

            plane['roof_poly'] = ops.unary_union([p['roof_poly'] for p in mergeable] + [poly])
            plane['inliers_xy'] = np.concatenate([p['inliers_xy'] for p in mergeable] + [plane['inliers_xy']])
            plane['roof_poly'] = _initial_polygon(plane, resolution_metres)

            merged.append(plane)

    return merged


def _remove_overlaps(toid: str, roof_poly: Polygon, polygons_by_toid: Dict[str, List[Polygon]]):
    intersecting_polys = [p for p in polygons_by_toid[toid] if p.intersects(roof_poly)]
    if len(intersecting_polys) > 0:
        other_polys = ops.unary_union(intersecting_polys).buffer(0.1,
                                                                 cap_style=CAP_STYLE.square,
                                                                 join_style=JOIN_STYLE.mitre,
                                                                 resolution=1)
        roof_poly = roof_poly.difference(other_polys)
        roof_poly = largest_polygon(roof_poly)
    roof_poly = make_valid(roof_poly)
    return roof_poly


def _constrain_to_building(building_geom: Polygon,
                           roof_poly: Polygon,
                           large_building_threshold: float,
                           min_dist_to_edge_large_m: float,
                           min_dist_to_edge_m: float):
    if building_geom.area < large_building_threshold:
        neg_buffer = -min_dist_to_edge_m
    else:
        neg_buffer = -min_dist_to_edge_large_m
    building_geom_shrunk = building_geom.buffer(
        neg_buffer, cap_style=CAP_STYLE.square, join_style=JOIN_STYLE.mitre)
    roof_poly = roof_poly.intersection(building_geom_shrunk)
    roof_poly = largest_polygon(roof_poly)
    return roof_poly


def _building_geoms(pg_uri: str, job_id: int, toids: List[str]) -> Dict[str, Polygon]:
    with connection(pg_uri, cursor_factory=psycopg2.extras.DictCursor) as pg_conn:
        buildings = sql_command(
            pg_conn,
            """
            SELECT toid, ST_AsText(geom_27700) AS geom_27700 
            FROM {buildings}
            WHERE toid = ANY( %(toids)s )""",
            {"toids": toids},
            buildings=Identifier(tables.schema(job_id), tables.BUILDINGS_TABLE),
            result_extractor=lambda rows: rows)
        by_toid = {}
        for building in buildings:
            geom = wkt.loads(building['geom_27700'])
            by_toid[building['toid']] = geom
        return by_toid


def _building_orientations(building_geom):
    # TODO support non-square buildings - take the top n azimuths rather than
    #      the top 1. Ideally then take the 1 closest to the plane and allow
    #      adjusting to that, that +90, that +180 etc?
    # 1. decompose into straight-line segments and find the total length of all
    #    segments by azimuth:
    azimuths = defaultdict(int)
    for i in range(len(building_geom.exterior.coords) - 1):
        p1 = building_geom.exterior.coords[i]
        p2 = building_geom.exterior.coords[i + 1]
        segment = LineString([p1, p2])
        # TODO nearest n? 2?
        az = round(azimuth(p1, p2))
        azimuths[az] += segment.length

    # 2. take the top azimuth and define other orientations based on it:
    most_common_az = max(azimuths, key=azimuths.get)
    return (most_common_az,
            (most_common_az + 90) % 360,
            (most_common_az + 180) % 360,
            (most_common_az + 270) % 360)


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


if __name__ == '__main__':
    # osgb1000021445362
    building = "POLYGON((414361.1232490772 290262.23400396167,414409.5504070239 290278.1342139624,414415.4006675822 290280.0542393503,414471.3731655209 290298.6244831074,414470.770202179 290300.4714865399,414468.6033339495 290307.10949886445,414459.3769210793 290304.00545850757,414441.74597865756 290357.49755691225,414325.47880112164 290319.17405041336,414308.8197971075 290369.3841407508,414308.51978340244 290369.2741394106,414298.5203799294 290399.3441932516,414132.9129867344 290344.2934709728,414135.7228180804 290335.8534561896,414142.9723824033 290314.05341792613,414156.99300942366 290318.7134788263,414166.1624884436 290292.07343293744,414174.3719738624 290266.67338728433,414186.0024945787 290270.54343770375,414187.3475547756 290270.9904435359,414189.71666078945 290271.7774538043,414191.0345818997 290267.81544678786,414191.7985362576 290265.52144272684,414200.1619107024 290268.3054789929,414198.08203513006 290274.5574900773,414215.1637993486 290280.23356417514,414219.4039890516 290281.64358257974,414224.20420378755 290283.2396034165,414231.87754701043 290285.79063672764,414234.1454114872 290278.97162458533,414237.82257598895 290280.19464054896,414235.55471148936 290287.0136526985,414272.98638497293 290299.4538153364,414278.2766211545 290301.2038383153,414280.55672312283 290301.9638482371,414282.66659775644 290295.6338369153,414286.61636157666 290283.73381555616,414257.0650398705 290273.90368719946,414263.67464599875 290254.0436516954,414264.8146970443 290254.423656641,414266.5445933154 290249.2036472733,414271.5748183474 290250.8736690781,414269.8449220526 290256.0936784502,414292.05591556017 290263.47377481405,414293.2159675826 290263.86377986136,414296.1657916052 290254.98376389145,414308.9163619888 290259.2238192296,414325.7371141676 290264.81389226054,414328.4982376316 290265.731904253,414331.91439038474 290266.86791909434,414332.9794380326 290267.22292372346,414335.132534303 290267.9389330762,414341.76783091604 290270.1439619087,414342.0978110054 290269.1419600831,414344.0416942787 290263.2569493969,414346.0175756192 290257.27393852605,414361.1232490772 290262.23400396167))"
    building = wkt.loads(building)
    a, b, c, d = _building_orientations(building)
    print(a, b, c, d)