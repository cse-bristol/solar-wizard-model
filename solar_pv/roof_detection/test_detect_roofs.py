# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import csv
import json
import unittest
from os.path import join
from typing import List

from shapely import wkt

from solar_pv.paths import TEST_DATA
from solar_pv.roof_detection.detect_roofs import _detect_building_roof_planes
from solar_pv.datatypes import RoofDetBuilding
from solar_pv.test_utils.test_funcs import ParameterisedTestCase

_ROOFDET_DATA = join(TEST_DATA, "roof_detection")


def _load_data(filename: str) -> RoofDetBuilding:
    with open(filename) as f:
        building = json.load(f)
        building['polygon'] = wkt.loads(building['polygon'])
    return building


def _roofdet(toid: str, res: float):
    filename = f"{toid}.json" if not toid.endswith(".json") else toid
    planes = _detect_building_roof_planes(_load_data(join(_ROOFDET_DATA, filename)), filename, res, debug=True)
    return sorted([plane['aspect'] for plane in planes])


class RoofDetTestCase(ParameterisedTestCase):

    def test_roof_detection(self):
        self.parameterised_test([
            # Tricky Totterdown terraces:
            ("0013", 1.0, [54, 234]),
            ("0008", 1.0, [54, 233]),
            ("0016", 1.0, [60, 240]),
            ("0003", 1.0, [57, 239]),
            ("0035", 1.0, [58, 238]),
            ("0021", 1.0, [56, 236]),
            ("0019", 1.0, [56, 236]),
            ("0031", 1.0, [55, 236]),
            ("0012", 1.0, [54, 236]),
            ("0008", 1.0, [54, 233]),
            ("0030", 1.0, [57, 234]),
            ("0034", 1.0, [54, 234]),
            ("0025", 1.0, [54, 234]),
            ("0027", 1.0, [54, 234]),
            ("0006", 1.0, [59, 236]),
            ("0018", 1.0, [58, 237]),
            ("0009", 1.0, [58, 237]),
            ("0011", 1.0, [61, 236]),

            # Irregular buildings:
            ("0032", 1.0, [82, 84, 168, 264, 264]),
            # ("0005", 1.0, [148, 148, 148, 148]),  # Totterdown Mosque - currently fails due to dome

            # Messy roofs - should find nothing
            # ("0015", 1.0, []),  # Totterdown pub - very messy non-flat roof - currently fails due to mess
            ("0029", 1.0, []),  # Cotham school - messy flat roof
            ("0028", 1.0, []),  # Cotham school - messy flat roof
            ("0033", 1.0, []),  # Cotham school - messy flat roof

            # Flat roofs:
            ("0023", 1.0, [138]),
            ("0017", 1.0, [141, 226]),

            # warehouses etc:
            ("0022", 1.0, [10, 46, 155, 190, 226, 335]),  # Motorbike shop
            ("0020", 1.0, [144, 144, 324, 324]),

            # Various tricky buildings in Croyde
            ("0004", 1.0, [166, 166, 167, 257, 346, 347, 347]),
            ("0001", 1.0, [154, 335]),
            ("0010", 1.0, [59, 238]),
            ("0024", 1.0, [73, 253]),
            ("0002", 1.0, [71, 251, 251]),
            ("0007", 1.0, [0, 90, 90, 180, 270]),
            ("0014", 1.0, [85, 265]),
            ("0026", 1.0, [59, 59, 149, 239, 329]),

            # Only has 1 pixel of lidar coverage...:
            ("0036", 1.0, []),

            # one plane that covers the entire roof and building is cardinally-aligned, so
            # no pixels outside building:
            ("0037", 1.0, [182]),

            # Below 0 height:
            ("0038", 1.0, [207]),
            # Totally round:
            ("0039", 1.0, []),

            # see premade_planes.py, L49-50
            ("0040", 1.0, []),
            ("0040", 1.0, []),

        ], _roofdet)
