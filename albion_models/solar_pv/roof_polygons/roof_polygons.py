from collections import defaultdict
from typing import List, Dict

import math
from psycopg2.sql import Identifier
import psycopg2.extras
from shapely import wkt, affinity, ops
from shapely.geometry import LineString, Polygon, CAP_STYLE, JOIN_STYLE
from shapely.validation import make_valid

import albion_models.solar_pv.tables as tables
from albion_models.db_funcs import connection, sql_command
from albion_models.geos import azimuth, square, largest_polygon
from albion_models.solar_pv.constants import FLAT_ROOF_DEGREES_THRESHOLD, \
    FLAT_ROOF_AZIMUTH_ALIGNMENT_THRESHOLD, AZIMUTH_ALIGNMENT_THRESHOLD


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
                         resolution_metres: float):
    """Add roof polygons and other related fields to the dicts in `planes`"""
    toids = list({plane['toid'] for plane in planes})
    building_geoms = _building_geoms(pg_uri, job_id, toids)
    _create_roof_polygons(building_geoms,
                          planes,
                          max_roof_slope_degrees=max_roof_slope_degrees,
                          min_roof_area_m=min_roof_area_m,
                          min_roof_degrees_from_north=min_roof_degrees_from_north,
                          flat_roof_degrees=flat_roof_degrees,
                          large_building_threshold=large_building_threshold,
                          min_dist_to_edge_m=min_dist_to_edge_m,
                          min_dist_to_edge_large_m=min_dist_to_edge_large_m,
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
                          resolution_metres: float):
    polygons_by_toid = defaultdict(list)

    for plane in planes:
        toid = plane['toid']
        # set is_flat, update slope and aspect of flat roofs
        is_flat = plane['slope'] <= FLAT_ROOF_DEGREES_THRESHOLD
        plane['is_flat'] = is_flat
        if is_flat:
            plane['slope'] = flat_roof_degrees

        # update orientations
        building_geom = building_geoms[toid]
        orientations = _building_orientations(building_geom)
        for orientation in orientations:
            threshold = AZIMUTH_ALIGNMENT_THRESHOLD if not is_flat else FLAT_ROOF_AZIMUTH_ALIGNMENT_THRESHOLD
            if abs(orientation - plane['aspect']) < threshold:
                plane['aspect'] = orientation
                break

        # create roof polygon
        pixels = []
        for p in plane['inliers_xy']:
            # Draw a square around the pixel centre:
            edge = math.sqrt(resolution_metres**2 * 2.0) / 2
            pixel = square(p[0] - edge, p[1] - edge, edge * 2)

            # Rotate the square to align with plane aspect:
            pixel = affinity.rotate(pixel, -plane['aspect'])
            pixels.append(pixel)
        neg_buffer = -((math.sqrt(resolution_metres**2 * 2.0) - resolution_metres) / 2)
        roof_poly = ops.unary_union(pixels).buffer(neg_buffer,
                                                   cap_style=CAP_STYLE.square,
                                                   join_style=JOIN_STYLE.mitre,
                                                   resolution=1)
        roof_poly = largest_polygon(roof_poly)

        # constrain roof polygons to building geometry, enforcing min dist to edge:
        if building_geom.area < large_building_threshold:
            neg_buffer = -min_dist_to_edge_m
        else:
            neg_buffer = -min_dist_to_edge_large_m
        building_geom = building_geom.buffer(
            neg_buffer, cap_style=CAP_STYLE.square, join_style=JOIN_STYLE.mitre)
        roof_poly = roof_poly.intersection(building_geom)
        roof_poly = largest_polygon(roof_poly)

        # don't allow overlapping roof polygons:
        intersecting_polys = [p for p in polygons_by_toid[toid] if p.intersects(roof_poly)]
        if len(intersecting_polys) > 0:
            other_polys = ops.unary_union(intersecting_polys)
            roof_poly = roof_poly.difference(other_polys)
            roof_poly = largest_polygon(roof_poly)
        roof_poly = make_valid(roof_poly)
        # any other planes in the same toid will now not be allowed to overlap this one:
        polygons_by_toid[toid].append(roof_poly)

        # Set usability:
        if plane['slope'] > max_roof_slope_degrees:
            plane['usable'] = False
        elif plane['aspect'] < min_roof_degrees_from_north:
            plane['usable'] = False
        elif plane['aspect'] > 360 - min_roof_degrees_from_north:
            plane['usable'] = False
        elif roof_poly.area < min_roof_area_m:
            plane['usable'] = False
        else:
            plane['usable'] = True

        # Add other info:
        plane['roof_geom_27700'] = roof_poly.wkt
        easting, northing = roof_poly.centroid.xy
        plane['easting'] = easting[0]
        plane['northing'] = northing[0]
        plane['raw_footprint'] = roof_poly.area
        plane['raw_area'] = roof_poly.area / math.cos(math.radians(plane['slope']))


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
    # 1. decompose into straight-line segments and find the total length of all
    #    segments by azimuth:
    azimuths = defaultdict(int)
    for i in range(len(building_geom.exterior.coords) - 1):
        p1 = building_geom.exterior.coords[i]
        p2 = building_geom.exterior.coords[i + 1]
        segment = LineString([p1, p2])
        az = round(azimuth(p1, p2))
        azimuths[az] += segment.length

    # 2. take the top azimuth and define other orientations based on it:
    most_common_az = max(azimuths, key=azimuths.get)
    return (most_common_az,
            (most_common_az + 90) % 360,
            (most_common_az + 180) % 360,
            (most_common_az + 270) % 360)


if __name__ == '__main__':
    # osgb1000021445362
    building = "POLYGON((414361.1232490772 290262.23400396167,414409.5504070239 290278.1342139624,414415.4006675822 290280.0542393503,414471.3731655209 290298.6244831074,414470.770202179 290300.4714865399,414468.6033339495 290307.10949886445,414459.3769210793 290304.00545850757,414441.74597865756 290357.49755691225,414325.47880112164 290319.17405041336,414308.8197971075 290369.3841407508,414308.51978340244 290369.2741394106,414298.5203799294 290399.3441932516,414132.9129867344 290344.2934709728,414135.7228180804 290335.8534561896,414142.9723824033 290314.05341792613,414156.99300942366 290318.7134788263,414166.1624884436 290292.07343293744,414174.3719738624 290266.67338728433,414186.0024945787 290270.54343770375,414187.3475547756 290270.9904435359,414189.71666078945 290271.7774538043,414191.0345818997 290267.81544678786,414191.7985362576 290265.52144272684,414200.1619107024 290268.3054789929,414198.08203513006 290274.5574900773,414215.1637993486 290280.23356417514,414219.4039890516 290281.64358257974,414224.20420378755 290283.2396034165,414231.87754701043 290285.79063672764,414234.1454114872 290278.97162458533,414237.82257598895 290280.19464054896,414235.55471148936 290287.0136526985,414272.98638497293 290299.4538153364,414278.2766211545 290301.2038383153,414280.55672312283 290301.9638482371,414282.66659775644 290295.6338369153,414286.61636157666 290283.73381555616,414257.0650398705 290273.90368719946,414263.67464599875 290254.0436516954,414264.8146970443 290254.423656641,414266.5445933154 290249.2036472733,414271.5748183474 290250.8736690781,414269.8449220526 290256.0936784502,414292.05591556017 290263.47377481405,414293.2159675826 290263.86377986136,414296.1657916052 290254.98376389145,414308.9163619888 290259.2238192296,414325.7371141676 290264.81389226054,414328.4982376316 290265.731904253,414331.91439038474 290266.86791909434,414332.9794380326 290267.22292372346,414335.132534303 290267.9389330762,414341.76783091604 290270.1439619087,414342.0978110054 290269.1419600831,414344.0416942787 290263.2569493969,414346.0175756192 290257.27393852605,414361.1232490772 290262.23400396167))"
    building = wkt.loads(building)
    a, b, c, d = _building_orientations(building)
    print(a, b, c, d)
