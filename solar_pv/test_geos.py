# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
from shapely import wkt

from solar_pv.geos import get_grid_refs, square, get_grid_cells, project, \
    project_geom, largest_polygon
from solar_pv.test_utils.test_funcs import ParameterisedTestCase

poly = wkt.loads("POLYGON((174470.31680707666 223518.17910779177,174276.26370506393 223546.12655343796,174091.39293130912 223611.39877670485,173922.81048688895 223711.4868656676,173776.9962875155 223842.54366778026,173659.55509069975 223999.53166544053,173575.0010619535 224176.41660640974,173526.58426080266 224366.39944638542,173516.16571608445 224562.17768839627,173592.06208690198 226246.89736822058,173620.03246149354 226440.9059367073,173685.3109052496 226625.73123606283,173785.38931157443 226794.27197027428,173916.4225013163 226940.05252346004,174073.37596095834 227057.47176382315,174250.21927701798 227142.01824846969,174440.1578345827 227190.44356056265,174635.893877738 227200.8871189106,176277.43827612288 227127.19793668273,176471.45017163677 227099.2587598961,176656.2848956962 227034.01048036112,176824.84087396128 226933.96001613676,176970.64198125486 226802.95142743978,177088.08636214878 226646.01822318762,177172.66166104548 226469.18996747906,177221.1183923546 226279.2606164265,177231.59478967154 226083.5274861198,177156.25176625728 224398.7982627386,177128.33552520312 224204.74183772347,177063.09347332583 224019.86164064688,176963.03333324415 223851.26395095012,176832.00113783564 223705.4291938767,176675.03339945318 223587.96285098934,176498.163520429 223503.3800010834,176308.1898859064 223454.9317731434,176112.4145528648 223444.480382007,174470.31680707666 223518.17910779177))")

poly2 = square(219999, 230001, 10000)
poly3 = square(460965, 366311, 1000)
poly4 = square(0, 0, 10)


class GeosTest(ParameterisedTestCase):
    def test_get_grid_refs(self):
        self.parameterised_test([
            (poly, 500000, ['S']),
            (poly, 100000, ['SM']),
            (poly, 10000, ['SM72']),
            (poly, 5000, ['SM72sw', 'SM72se', 'SM72nw', 'SM72ne']),
            (poly2, 10000, ['SN13', 'SN23', 'SN14', 'SN24']),
            (poly3, 10000, ['SK66']),
            # test that shapes partially outside the range of grid refs still work:
            (square(-1, -1, 10), 10000, ['SV00']),
            # test that shapes fully outside the range of grid refs still work:
            (square(-100, -100, 10), 10000, []),
        ], get_grid_refs)

    def test_get_grid_cells(self):
        def grid_cell_xy(poly, cell_w, cell_h, spacing_w=0, spacing_h=0):
            grid_cells = get_grid_cells(poly, cell_w, cell_h, spacing_w, spacing_h)
            xys = []
            for cell in grid_cells:
                x, y, _, _ = cell.bounds
                xys.append((x, y))
            return xys

        self.parameterised_test([
            (poly2, 10000, 10000, 0, 0,
             [(210000, 230000), (220000, 230000), (210000, 240000), (220000, 240000)]),
            (poly4, 3, 3, 1, 1,
             [(0, 0), (4, 0), (8, 0), (0, 4), (4, 4), (8, 4), (0, 8), (4, 8), (8, 8)]),
            (poly4, 3, 5, 1, 1,
             [(0, 0), (4, 0), (8, 0), (0, 6), (4, 6), (8, 6)]),
            (poly4, 5, 3, 1, 1,
             [(0, 0), (6, 0), (0, 4), (6, 4), (0, 8), (6, 8)]),
        ], grid_cell_xy)

    def test_project(self):
        self.parameterised_test([
            (-1.3183623236379631, 51.69980008039696, 4326, 27700, (447205.00083648594, 200336.99951118266)),
            (447205.00083648594, 200336.99951118266, 27700, 4326, (-1.3183623116012846, 51.69980007593203)),
        ], project)

    def test_project_geom(self):
        def to_test(geom, src_srs, dst_srs):
            return project_geom(geom, src_srs, dst_srs).wkt

        self.parameterised_test([
            (square(-1.3183623236379631, 51.69980008039696, 0.001), 4326, 27700,
             "POLYGON ((447205.00083648594 200336.99951118266, 447203.9628190113 200448.21811441745, 447273.07100138796 200448.8635877514, 447274.11054237804 200337.64498984604, 447205.00083648594 200336.99951118266))"),
            (square(447205.00083648594, 200336.99951118266, 1000), 27700, 4326,
             "POLYGON ((-1.3183623116012846 51.69980007593203, -1.3182272411473017 51.708790588071906, -1.3037559220079633 51.70870576100723, -1.3038938600255106 51.69971527605842, -1.3183623116012846 51.69980007593203))"),
        ], to_test)

    def test_largest_polygon(self):
        self.parameterised_test([
            (poly2, poly2),
            (wkt.loads("GEOMETRYCOLLECTION (POINT(1 1))"), None),
            (wkt.loads("GEOMETRYCOLLECTION (POINT(1 1), LINESTRING (10 10, 20 20, 10 40))"), None),
            (wkt.loads("GEOMETRYCOLLECTION (POINT(1 1), LINESTRING (10 10, 20 20, 10 40), POLYGON ((40 40, 20 45, 45 30, 40 40)))"),
             wkt.loads("POLYGON ((40 40, 20 45, 45 30, 40 40))")),
        ], largest_polygon)