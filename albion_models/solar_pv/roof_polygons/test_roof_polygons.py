import json
from os.path import join

import numpy as np
from shapely import wkt

from albion_models import paths
from albion_models.solar_pv.constants import FLAT_ROOF_AZIMUTH_ALIGNMENT_THRESHOLD
from albion_models.solar_pv.roof_polygons.roof_polygons import _building_orientations, \
    _create_roof_polygons
from albion_models.test.test_funcs import ParameterisedTestCase

# Large building in Castle Vale (Birmingham) where the previous approach
# to finding the orientation of the building (based on postGIS function
# ST_OrientedEnvelope) was wrong:
osgb1000021445362 = wkt.loads(
    "POLYGON((414361.1232490772 290262.23400396167,414409.5504070239 290278.1342139624,414415.4006675822 290280.0542393503,414471.3731655209 290298.6244831074,414470.770202179 290300.4714865399,414468.6033339495 290307.10949886445,414459.3769210793 290304.00545850757,414441.74597865756 290357.49755691225,414325.47880112164 290319.17405041336,414308.8197971075 290369.3841407508,414308.51978340244 290369.2741394106,414298.5203799294 290399.3441932516,414132.9129867344 290344.2934709728,414135.7228180804 290335.8534561896,414142.9723824033 290314.05341792613,414156.99300942366 290318.7134788263,414166.1624884436 290292.07343293744,414174.3719738624 290266.67338728433,414186.0024945787 290270.54343770375,414187.3475547756 290270.9904435359,414189.71666078945 290271.7774538043,414191.0345818997 290267.81544678786,414191.7985362576 290265.52144272684,414200.1619107024 290268.3054789929,414198.08203513006 290274.5574900773,414215.1637993486 290280.23356417514,414219.4039890516 290281.64358257974,414224.20420378755 290283.2396034165,414231.87754701043 290285.79063672764,414234.1454114872 290278.97162458533,414237.82257598895 290280.19464054896,414235.55471148936 290287.0136526985,414272.98638497293 290299.4538153364,414278.2766211545 290301.2038383153,414280.55672312283 290301.9638482371,414282.66659775644 290295.6338369153,414286.61636157666 290283.73381555616,414257.0650398705 290273.90368719946,414263.67464599875 290254.0436516954,414264.8146970443 290254.423656641,414266.5445933154 290249.2036472733,414271.5748183474 290250.8736690781,414269.8449220526 290256.0936784502,414292.05591556017 290263.47377481405,414293.2159675826 290263.86377986136,414296.1657916052 290254.98376389145,414308.9163619888 290259.2238192296,414325.7371141676 290264.81389226054,414328.4982376316 290265.731904253,414331.91439038474 290266.86791909434,414332.9794380326 290267.22292372346,414335.132534303 290267.9389330762,414341.76783091604 290270.1439619087,414342.0978110054 290269.1419600831,414344.0416942787 290263.2569493969,414346.0175756192 290257.27393852605,414361.1232490772 290262.23400396167))")

# Similar to above, nearby:
osgb1000021445346 = wkt.loads(
    "POLYGON((414120.70885106624 290225.92308903555,414118.60875686683 290225.2230799598,414110.95918968867 290247.3731178904,414088.7581972687 290240.07302214386,414089.3581613019 290238.2730189981,414088.2081091949 290237.8730139802,414093.30779254716 290222.2229862349,414094.5078463901 290222.6229914202,414095.00781643507 290221.1229887954,414096.15786698746 290221.4729936688,414096.6578401789 290220.0729913305,414097.2578089533 290218.42298861244,414094.50768607645 290217.5229767812,414093.0576204847 290217.02297046944,414095.1914053911 290207.8449513006,414096.207302925 290203.4729421641,414105.20770531526 290206.4229808706,414104.4577470713 290208.5729845264,414111.20805156365 290210.8730138199,414110.70807991776 290212.32301630627,414113.4582043646 290213.2730282805,414114.00817773415 290211.82302596036,414124.35864527215 290215.3730709584,414120.70885106624 290225.92308903555))")

# Simple nearby residential building:
osgb1000002043666111 = wkt.loads(
    "POLYGON((414544.06502718956 290923.98659185076,414551.70543551375 290928.86663333635,414549.4054695578 290932.46663596906,414548.6554291398 290931.97663186287,414546.99545346654 290934.56663373555,414547.3754739982 290934.8166358215,414547.87556173856 290937.10664441524,414547.6355654283 290937.4866447029,414550.2257036843 290939.1366587631,414550.71569664276 290938.3766582221,414554.10587791196 290940.54667665705,414548.8959546372 290948.686682562,414547.9459037869 290948.0766773892,414542.6656217729 290944.70664870244,414542.8956183624 290944.34664843924,414541.4655420609 290943.43664067966,414540.86555036646 290944.3566413076,414535.94528754713 290941.21661458595,414536.1252849306 290940.9366143873,414535.5452545702 290940.58661129914,414535.2552587515 290941.03661161475,414532.5151123287 290939.28659673536,414534.265086338 290936.5465947426,414534.7151104672 290936.8365971934,414535.0851049522 290936.2565967692,414534.8250909351 290936.08659534744,414537.96504421567 290931.1665917551,414538.89509379625 290931.75659679435,414539.805080135 290930.3265957389,414539.44506120216 290930.1065938159,414543.56500044465 290923.66658913466,414544.06502718956 290923.98659185076))")

osgb1000014994594 = wkt.loads(
    "POLYGON((359550.9 171704.15,359549.65 171706.15,359548.55 171705.45,359547.1 171707.8,359541.15 171704.05,359543.9 171699.7,359550.9 171704.15))"
)

def _load_test_data(toid: str):
    roof_polys_dir = join(paths.TEST_DATA, "roof_polygons")
    with open(join(roof_polys_dir, f"{toid}.json")) as f:
        data = json.load(f)
        planes = data['planes']
        for plane in planes:
            plane['inliers_xy'] = np.array(plane['inliers_xy'])
        building_geom = wkt.loads(data['building_geom'])
    return planes, building_geom


def _create_polygons_using_test_data(toid: str,
                                     max_roof_slope_degrees: int = 80,
                                     min_roof_area_m: int = 8,
                                     min_roof_degrees_from_north: int = 45,
                                     flat_roof_degrees: int = 10,
                                     large_building_threshold: float = 200,
                                     min_dist_to_edge_m: float = 0.3,
                                     min_dist_to_edge_large_m: float = 1):
    planes, building_geom = _load_test_data(toid)
    _create_roof_polygons(
        {toid: building_geom},
        planes,
        max_roof_slope_degrees=max_roof_slope_degrees,
        min_roof_area_m=min_roof_area_m,
        min_roof_degrees_from_north=min_roof_degrees_from_north,
        flat_roof_degrees=flat_roof_degrees,
        large_building_threshold=large_building_threshold,
        min_dist_to_edge_m=min_dist_to_edge_m,
        min_dist_to_edge_large_m=min_dist_to_edge_large_m,
        resolution_metres=1.0,
        panel_width_m=0.99,
        panel_height_m=1.64)
    for plane in planes:
        if "roof_geom_27700" in plane:
            plane['roof_geom_27700'] = wkt.loads(plane['roof_geom_27700'])
    return planes, building_geom


class RoofPolygonTest(ParameterisedTestCase):
    def test_building_orientation(self):
        self.parameterised_test([
            (osgb1000021445362, (72, 162, 252, 342)),
            (osgb1000021445346, (72, 162, 252, 342)),
            (osgb1000002043666111, (147, 237, 327, 57)),
            (osgb1000014994594, (58, 148, 238, 328)),
        ], _building_orientations)

    def test_roof_polygons_do_not_overlap(self):
        def _do_test(toid: str):
            planes, _ = _create_polygons_using_test_data(toid)
            for p1 in planes:
                for p2 in planes:
                    poly1 = p1['roof_geom_27700']
                    poly2 = p2['roof_geom_27700']
                    if p1['roof_plane_id'] != p2['roof_plane_id']:
                        crossover = poly1.intersection(poly2).area
                        assert crossover == 0, f"{p1['roof_plane_id']} overlaps {p2['roof_plane_id']} by {crossover} m2"

        self.parameterised_test([
            ("osgb1000021445086", None),
            ("osgb1000021445097", None),
        ], _do_test)

    def test_roof_polygons_stay_within_building(self):
        def _do_test(toid: str):
            min_dist_to_edge_m = 0.55
            planes, building_geom = _create_polygons_using_test_data(toid, min_dist_to_edge_m=min_dist_to_edge_m)
            building_geom = building_geom.buffer(-min_dist_to_edge_m)
            for p in planes:
                poly = p['roof_geom_27700']
                crossover = poly.difference(building_geom).area
                assert crossover < 0.000000001, f"{p['roof_plane_id']} overlaps  -ve buffered building by {crossover} m2"

        self.parameterised_test([
            ("osgb1000021445086", None),
            ("osgb1000021445097", None),
        ], _do_test)

    def test_failing_roof_polygons(self):
        def _do_test(toid: str):
            min_dist_to_edge_m = 0.55
            _create_polygons_using_test_data(toid, min_dist_to_edge_m=min_dist_to_edge_m)

        self.parameterised_test([
            ("osgb1000034161241", None),
            ("osgb1000034178593", None),
            ("osgb5000005113406742", None),
        ], _do_test)

    def test_flat_roofs_face_south(self):
        planes, _ = _create_polygons_using_test_data("osgb1000000137769485")
        assert len(planes) == 3
        min_aspect = 180 - FLAT_ROOF_AZIMUTH_ALIGNMENT_THRESHOLD
        max_aspect = 180 + FLAT_ROOF_AZIMUTH_ALIGNMENT_THRESHOLD
        checked = False
        for plane in planes:
            if not plane['is_flat']:
                continue
            assert min_aspect < plane['aspect'] <= max_aspect
            checked = True
        assert checked is True
