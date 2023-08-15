# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import csv
import json
import unittest
from os.path import join
from typing import List

from shapely import wkt

from solar_pv.paths import TEST_DATA
from solar_pv.roof_detection.detect_roofs import _detect_building_roof_planes, RoofDetBuilding
from solar_pv.test_utils.test_funcs import ParameterisedTestCase

_ROOFDET_DATA = join(TEST_DATA, "roof_detection")


def _load_data(filename: str) -> RoofDetBuilding:
    with open(filename) as f:
        building = json.load(f)
        building['polygon'] = wkt.loads(building['polygon'])
    return building


def _roofdet(toid: str, res: float):
    filename = f"{toid}.json" if not toid.endswith(".json") else toid
    planes = _detect_building_roof_planes(_load_data(join(_ROOFDET_DATA, filename)), filename, res, debug=False)
    return sorted([plane['aspect_adjusted'] for plane in planes])


class RoofDetTestCase(ParameterisedTestCase):

    def test_roof_detection(self):
        self.parameterised_test([
            # Tricky Totterdown terraces:
            ("osgb1000014994639", 1.0, [54, 234]),
            ("osgb1000014994636", 1.0, [53, 233]),
            ("osgb1000014994625", 1.0, [60, 241]),
            ("osgb1000014994628", 1.0, [57, 239]),
            ("osgb1000014994630", 1.0, [58, 238]),
            ("osgb1000014994631", 1.0, [55, 237]),
            ("osgb1000014994632", 1.0, [56, 236]),
            ("osgb1000014994633", 1.0, [55, 235]),
            ("osgb1000014994634", 1.0, [54, 236]),
            ("osgb1000014994636", 1.0, [53, 233]),
            ("osgb1000014994637", 1.0, [57, 235]),
            ("osgb1000014994638", 1.0, [54, 234]),
            ("osgb1000014994640", 1.0, [54, 234]),
            ("osgb1000014994648", 1.0, [54, 234]),
            ("osgb1000014994950", 1.0, [59, 236]),
            ("osgb1000014994951", 1.0, [58, 236]),
            ("osgb1000014994948", 1.0, [57, 237]),
            ("osgb1000014994947", 1.0, [61, 236]),

            # Irregular buildings:
            ("osgb1000014995098", 1.0, [82, 84, 177, 264, 264]),
            ("osgb1000014994877", 1.0, [209, 209, 209, 209]),  # Totterdown Mosque - currently fails due to dome

            # Messy roofs - should find nothing
            ("osgb1000014994794", 1.0, []),  # Totterdown pub - very messy roof - currently fails due to mess
            ("osgb1000002529080353", 1.0, []),  # Cotham school - currently fails due to mess
            ("osgb1000002529080355", 1.0, []),  # Cotham school - currently fails due to mess
            ("osgb1000002529080354", 1.0, []),  # Cotham school - currently fails due to mess

            # Flat roofs:
            ("osgb1000014998049", 1.0, [138]),
            ("osgb1000014998048", 1.0, [141, 226]),

            # warehouses etc:
            ("osgb1000014998052", 1.0, [10, 46, 155, 190, 226, 335]),  # Motorbike shop
            ("osgb1000014998047", 1.0, [143, 144, 324, 324]),

            # Various tricky buildings in Croyde
            ("osgb1000021672464", 1.0, [166, 166, 167, 257, 347, 347, 347]),
            ("osgb1000000337215292", 1.0, [155, 335]),
            ("osgb1000021681586", 1.0, [58, 238]),
            ("osgb1000021672474", 1.0, [74, 254]),
            ("osgb1000021672476", 1.0, [71, 251, 251]),
            ("osgb1000021672457", 1.0, [0, 90, 90, 180, 270]),
            ("osgb1000021672466", 1.0, [85, 265]),
            ("osgb1000000337226766", 1.0, [59, 59, 149, 239, 329]),
        ], _roofdet)
