# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import math
import unittest

from shapely import wkt

from solar_pv.geos import get_grid_refs, square, get_grid_cells, project, \
    project_geom, largest_polygon, simplify_by_angle, polygon_line_segments, \
    slope_deg, aspect_deg, aspect_rad, circular_mean_rad, circular_sd_rad, \
    circular_variance_rad, rad_diff, deg_diff, to_positive_angle, azimuth_deg
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
        cases = [
            (-1.3183623236379631, 51.69980008039696, 4326, 27700, (447205.00083648594, 200336.99951118266)),
            (447205.00083648594, 200336.99951118266, 27700, 4326, (-1.3183623116012846, 51.69980007593203)),
        ]
        for x, y, src_srs, dst_srs, expected in cases:
            actual = project(x, y, src_srs, dst_srs)
            for a, e in zip(actual, expected):
                self.assertAlmostEqual(a, e, places=6)

    def test_project_geom(self):
        cases = [
            (square(-1.3183623236379631, 51.69980008039696, 0.001), 4326, 27700,
             "POLYGON ((447205.00083648594 200336.99951118266, 447203.9628190113 200448.21811441745, 447273.07100138796 200448.8635877514, 447274.11054237804 200337.64498984604, 447205.00083648594 200336.99951118266))"),
            (square(447205.00083648594, 200336.99951118266, 1000), 27700, 4326,
             "POLYGON ((-1.3183623116012846 51.69980007593203, -1.3182272411473017 51.708790588071906, -1.3037559220079633 51.70870576100723, -1.3038938600255106 51.69971527605842, -1.3183623116012846 51.69980007593203))"),
        ]
        for geom, src_srs, dst_srs, expected_wkt in cases:
            actual = project_geom(geom, src_srs, dst_srs)
            expected = wkt.loads(expected_wkt)
            self.assertTrue(actual.equals_exact(expected, tolerance=1e-6),
                            f"\nExpected: {expected.wkt}\nActual  : {actual.wkt}")

    def test_largest_polygon(self):
        self.parameterised_test([
            (poly2, poly2),
            (wkt.loads("GEOMETRYCOLLECTION (POINT(1 1))"), None),
            (wkt.loads("GEOMETRYCOLLECTION (POINT(1 1), LINESTRING (10 10, 20 20, 10 40))"), None),
            (wkt.loads("GEOMETRYCOLLECTION (POINT(1 1), LINESTRING (10 10, 20 20, 10 40), POLYGON ((40 40, 20 45, 45 30, 40 40)))"),
             wkt.loads("POLYGON ((40 40, 20 45, 45 30, 40 40))")),
        ], largest_polygon)

    def test_simplify_by_angle(self):
        # Test example from https://github.com/shapely/shapely/issues/1046
        test_hex_wkt = 'POLYGON ((-10 10, -9.100000000000001 11.55884572681199, -8.5 12.59807621135332, -7.749999999999999 13.89711431702998, -5.549999999999999 17.7076260936815, -4.999999999999998 18.66025403784439, 0.0000000000000036 27.32050807568877, 6.800000000000001 27.32050807568877, 11.3 27.32050807568877, 14.5 27.32050807568877, 16 27.32050807568877, 18.1 27.32050807568877, 20 27.32050807568877, 23.4 21.43153532995459, 24.6 19.35307436087194, 25.05 18.57365149746594, 28.7 12.25166604983954, 29.95 10.08660254037844, 30 10, 27.15 5.063655198428702, 25.95 2.985194229346048, 24.55 0.5603230987496204, 23.65 -0.9985226280623696, 23.3 -1.604740410711477, 20 -7.320508075688771, 14.19999999999999 -7.320508075688771, 11.49999999999999 -7.320508075688771, 9.69999999999999 -7.320508075688771, 9.19999999999999 -7.320508075688771, 6.499999999999991 -7.320508075688771, -0.0000000000000089 -7.320508075688771, -0.6500000000000057 -6.194675050769005, -3.000000000000004 -2.124355652982141, -3.550000000000004 -1.171727708819258, -5.600000000000002 2.378976446696941, -7.650000000000001 5.929680602213139, -10 10))'
        test_hex_poly = wkt.loads(test_hex_wkt).normalize()
        print(test_hex_poly)

        simplified = simplify_by_angle(test_hex_poly).normalize()
        expected = wkt.loads("POLYGON ((-10 10, 0 27.32050807568877, 20 27.32050807568877, "
                             "30 10, 20 -7.320508075688771, 0 -7.320508075688771, -10 10))").normalize()
        assert simplified.equals_exact(expected, tolerance=1e-9), simplified.wkt

    def test_polygon_line_segments(self):
        def _polygon_line_segments(p):
            ls = polygon_line_segments(p)
            return [l.wkt for l in ls]
        self.parameterised_test([
            (wkt.loads("POLYGON ((-10 10, 0 27, 20 27, 30 10, 20 -7, 0 -7, -10 10))"),
             ['LINESTRING (-10 10, 0 27)',
              'LINESTRING (0 27, 20 27)',
              'LINESTRING (20 27, 30 10)',
              'LINESTRING (30 10, 20 -7)',
              'LINESTRING (20 -7, 0 -7)',
              'LINESTRING (0 -7, -10 10)']),
        ], _polygon_line_segments)


class GeosAnglesTest(unittest.TestCase):
    """
    Unit tests for the plane/angle maths that roof plane detection relies on.

    These use tolerance-based assertions rather than exact `==` so that
    floating-point drift (e.g. from a newer numpy/PROJ) doesn't cause spurious
    failures - the values themselves are what matter, not their last decimal.
    """

    def test_slope_deg(self):
        # flat plane -> 0 degrees:
        self.assertAlmostEqual(slope_deg(0, 0), 0, places=9)
        # unit gradient in either axis -> 45 degrees:
        self.assertAlmostEqual(slope_deg(1, 0), 45, places=9)
        self.assertAlmostEqual(slope_deg(0, 1), 45, places=9)
        # gradient magnitude is what counts, so sign is irrelevant:
        self.assertAlmostEqual(slope_deg(-1, 0), 45, places=9)
        self.assertAlmostEqual(slope_deg(0, -1), 45, places=9)
        # combined axes -> atan(sqrt(2)):
        self.assertAlmostEqual(slope_deg(1, 1), math.degrees(math.atan(math.sqrt(2))), places=9)
        # tan(30 deg) gradient -> 30 degrees:
        self.assertAlmostEqual(slope_deg(math.tan(math.radians(30)), 0), 30, places=9)

    def test_aspect_deg(self):
        # aspect is the compass direction (from North, clockwise) the plane faces,
        # i.e. the downhill direction. A plane rising towards +x (east) faces west:
        self.assertAlmostEqual(aspect_deg(1, 0), 270, places=9)   # rises east -> faces west
        self.assertAlmostEqual(aspect_deg(-1, 0), 90, places=9)   # rises west -> faces east
        self.assertAlmostEqual(aspect_deg(0, 1), 180, places=9)   # rises north -> faces south
        self.assertAlmostEqual(aspect_deg(0, -1), 0, places=9)    # rises south -> faces north
        self.assertAlmostEqual(aspect_deg(1, 1), 225, places=9)   # rises NE -> faces SW
        self.assertAlmostEqual(aspect_deg(-1, -1), 45, places=9)  # rises SW -> faces NE

    def test_aspect_deg_is_always_in_0_360(self):
        for a in range(-3, 4):
            for b in range(-3, 4):
                if a == 0 and b == 0:
                    continue
                self.assertTrue(0 <= aspect_deg(a, b) < 360)

    def test_aspect_rad_matches_aspect_deg(self):
        for a in range(-3, 4):
            for b in range(-3, 4):
                if a == 0 and b == 0:
                    continue
                self.assertAlmostEqual(aspect_rad(a, b), math.radians(aspect_deg(a, b)), places=9)

    def test_circular_mean_rad(self):
        # mean of identical angles is that angle:
        self.assertAlmostEqual(circular_mean_rad([1.0, 1.0, 1.0]), 1.0, places=9)
        # mean of 0 and 90 degrees is 45 degrees:
        self.assertAlmostEqual(circular_mean_rad([0, math.pi / 2]), math.pi / 4, places=9)
        # crucially it wraps: mean of 350 and 10 degrees is 0, not 180
        # (which a naive arithmetic mean would give). The result may come back as
        # either ~0 or ~2pi, so compare as a distance around the circle:
        self.assertAlmostEqual(
            rad_diff(circular_mean_rad([math.radians(350), math.radians(10)]), 0), 0, places=9)

    def test_circular_sd_rad(self):
        # no spread -> sd of 0:
        self.assertAlmostEqual(circular_sd_rad([1.0, 1.0, 1.0, 1.0]), 0, places=9)
        # a wider spread gives a larger sd than a narrower one:
        narrow = circular_sd_rad([math.radians(10), math.radians(20)])
        wide = circular_sd_rad([math.radians(10), math.radians(120)])
        self.assertGreater(wide, narrow)
        # known value for {0, 90 degrees}: R = sqrt(2)/2, sd = sqrt(-2 ln R):
        self.assertAlmostEqual(circular_sd_rad([0, math.pi / 2]), 0.8325546, places=6)

    def test_circular_variance_rad(self):
        self.assertAlmostEqual(circular_variance_rad([1.0, 1.0, 1.0]), 0, places=9)
        self.assertAlmostEqual(circular_variance_rad([0, math.pi / 2]), 0.2928932, places=6)

    def test_rad_diff(self):
        self.assertAlmostEqual(rad_diff(0.1, 0.2), 0.1, places=9)
        self.assertAlmostEqual(rad_diff(0, math.pi), math.pi, places=9)
        # takes the short way around the circle:
        self.assertAlmostEqual(rad_diff(0.1, 2 * math.pi - 0.1), 0.2, places=9)
        # symmetric:
        self.assertAlmostEqual(rad_diff(5.0, 1.0), rad_diff(1.0, 5.0), places=9)

    def test_deg_diff(self):
        self.assertAlmostEqual(deg_diff(90, 100), 10, places=9)
        self.assertAlmostEqual(deg_diff(0, 180), 180, places=9)
        # takes the short way around the circle:
        self.assertAlmostEqual(deg_diff(10, 350), 20, places=9)
        self.assertAlmostEqual(deg_diff(350, 10), 20, places=9)

    def test_to_positive_angle(self):
        self.assertAlmostEqual(to_positive_angle(0), 0, places=9)
        self.assertAlmostEqual(to_positive_angle(360), 0, places=9)
        self.assertAlmostEqual(to_positive_angle(370), 10, places=9)
        self.assertAlmostEqual(to_positive_angle(-10), 350, places=9)
        self.assertAlmostEqual(to_positive_angle(-370), 350, places=9)

    def test_azimuth_deg(self):
        # azimuth_deg gives the undirected orientation of a line segment, in (0, 180]:
        self.assertAlmostEqual(azimuth_deg((0, 0), (1, 0)), 90, places=9)    # E-W line
        self.assertAlmostEqual(azimuth_deg((0, 0), (0, 1)), 180, places=9)   # N-S line
        self.assertAlmostEqual(azimuth_deg((0, 0), (1, 1)), 45, places=9)    # NE-SW line
        self.assertAlmostEqual(azimuth_deg((0, 0), (-1, 1)), 135, places=9)  # NW-SE line
        # direction along the line doesn't matter (undirected):
        self.assertAlmostEqual(azimuth_deg((0, 0), (1, 1)), azimuth_deg((1, 1), (0, 0)), places=9)
