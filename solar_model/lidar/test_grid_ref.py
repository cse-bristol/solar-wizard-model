# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
from solar_model.lidar.grid_ref import os_grid_ref_to_en, os_grid_ref_to_wkt
from solar_model.test_utils.test_funcs import ParameterisedTestCase


class GridRefTest(ParameterisedTestCase):

    def test_os_grid_ref_to_en(self):
        self.parameterised_test([
            # Test mapping of 1st letter:
            ('SV0000', (0, 0, 1000)),
            ('TV0000', (500000, 0, 1000)),
            ('NV0000', (0, 500000, 1000)),
            ('OV0000', (500000, 500000, 1000)),
            ('HV0000', (0, 1000000, 1000)),

            # Test mapping of 2nd letter:
            ('SA0000', (0, 400000, 1000)),
            ('SB0000', (100000, 400000, 1000)),
            ('SC0000', (200000, 400000, 1000)),
            ('SD0000', (300000, 400000, 1000)),
            ('SE0000', (400000, 400000, 1000)),
            ('SF0000', (0, 300000, 1000)),
            ('SG0000', (100000, 300000, 1000)),
            ('SH0000', (200000, 300000, 1000)),
            ('SJ0000', (300000, 300000, 1000)),
            ('SK0000', (400000, 300000, 1000)),
            ('SL0000', (0, 200000, 1000)),
            ('SM0000', (100000, 200000, 1000)),
            ('SN0000', (200000, 200000, 1000)),
            ('SO0000', (300000, 200000, 1000)),
            ('SP0000', (400000, 200000, 1000)),
            ('SQ0000', (0, 100000, 1000)),
            ('SR0000', (100000, 100000, 1000)),
            ('SS0000', (200000, 100000, 1000)),
            ('ST0000', (300000, 100000, 1000)),
            ('SU0000', (400000, 100000, 1000)),
            ('SV0000', (0, 0, 1000)),
            ('SW0000', (100000, 0, 1000)),
            ('SX0000', (200000, 0, 1000)),
            ('SY0000', (300000, 0, 1000)),
            ('SZ0000', (400000, 0, 1000)),

            # Test mapping of rest:
            ('SV0101', (1000, 1000, 1000)),
            ('SV0322', (3000, 22000, 1000)),
            ('SV2322', (23000, 22000, 1000)),
            ('SV12NE', (15000, 25000, 5000)),
            ('sv12ne', (15000, 25000, 5000)),

            ('SK54',   (450000, 340000, 10000)),
            ('SK54SW', (450000, 340000, 5000)),
            ('SK54SE', (455000, 340000, 5000)),
            ('SK54NE', (455000, 345000, 5000)),
            ('SK54NW', (450000, 345000, 5000)),

            ('SK5040', (450000, 340000, 1000)),
            ('SK5141', (451000, 341000, 1000)),
            ('SK5141SW', (451000, 341000, 500)),
            ('SK5141SE', (451500, 341000, 500)),
            ('SK5141NE', (451500, 341500, 500)),
            ('SK5141NW', (451000, 341500, 500)),
        ], os_grid_ref_to_en)

    def test_os_grid_ref_to_wkt(self):
        self.parameterised_test([
            ('SV12NE', 'POLYGON((15000 25000, 15000 30000, 20000 30000, 20000 25000, 15000 25000))'),
            ('SV0101', 'POLYGON((1000 1000, 1000 2000, 2000 2000, 2000 1000, 1000 1000))'),
        ], os_grid_ref_to_wkt)
