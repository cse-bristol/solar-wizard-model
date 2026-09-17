# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
"""
Tests for the r.pv irradiation port. Three layers:

- sub-function checks (solar constant, sun position, aspect handling);
- physical-property checks (orientation, clearness, shadowing) on synthetic pixels;
- a regression fixture of complete pixel input->output cases whose expected values are the
  GRASS r.pv outputs (the port reproduces them to <0.01%). These freeze the validated
  behaviour in-repo, now that the GRASS/PVMAPS golden-raster harness has been removed.
"""
import math
import unittest

import numpy as np

from solar_pv.pv import irradiation as ir

CSI = [1.000436, -0.017237, -0.040465, -0.004702, 0.000149, 0.000170, 0.000005, 0.035]


def solar_decl(day):
    """PVMAPS _calc_solar_declination; r.pv (and compute_daily_pv) uses declination = -this."""
    d1 = 2.0 * math.pi * day / 365.25
    return math.asin(0.3978 * math.sin(d1 - 1.4 + 0.0355 * math.sin(d1 - 0.0489)))


def _pv(slope, aspect, elev=40.0, lat=55.0, lon=-2.0, horizon=None, linke=3.0,
        cbh=0.9, cdh=0.5, temps=10.0, day=162, albedo=0.2):
    """Run compute_daily_pv for a single pixel; scalars broadcast to length-1 arrays."""
    horizon = np.zeros((1, 8)) if horizon is None else np.asarray(horizon)[None, :]
    temps8 = np.full((1, 8), temps) if np.isscalar(temps) else np.asarray(temps)[None, :]
    return ir.compute_daily_pv(
        np.array([slope]), np.array([aspect]), np.array([elev]),
        np.radians([lat]), np.radians([lon]), horizon, 45.0,
        np.array([linke]), np.array([cbh]), np.array([cdh]), temps8, albedo,
        day, -solar_decl(day), CSI)[0]


class SubFunctionTest(unittest.TestCase):

    def test_solar_constant_perihelion_vs_aphelion(self):
        # Earth is closest to the sun in early January -> higher extraterrestrial irradiance.
        self.assertGreater(ir.com_sol_const(3), ir.com_sol_const(185))
        self.assertTrue(1410 < ir.com_sol_const(3) < 1415)
        self.assertTrue(1320 < ir.com_sol_const(185) < 1325)

    def test_noon_solar_altitude(self):
        # At solar noon, altitude ~ 90 - lat + declination. lat 58.6, June decl +23.x:
        lat = np.radians([58.6])
        sinlat, coslat = np.sin(-lat), np.cos(-lat)
        decl = -solar_decl(162)
        sind, cosd = math.sin(decl), math.cos(decl)
        C31 = coslat * cosd
        C33 = sinlat * sind
        _, alt, _, sun_az = ir._sun_position(np.array([0.0]),
                                             sinlat * cosd, -coslat * sind, cosd, C31, C33)
        self.assertAlmostEqual(math.degrees(alt[0]), 90 - 58.6 + math.degrees(solar_decl(162)),
                               delta=0.5)
        # at noon the sun is due south -> CCW-from-East azimuth 270 -> horizon index 6:
        self.assertAlmostEqual(math.degrees(sun_az[0]) % 360, 270.0, delta=1.0)


class PhysicalPropertyTest(unittest.TestCase):

    def test_south_beats_north_in_summer(self):
        # aspect is compass (0 = N, clockwise): south = 180, north = 360.
        south = _pv(slope=35, aspect=180, day=162)
        north = _pv(slope=35, aspect=360, day=162)
        self.assertGreater(south, north * 1.15)

    def test_east_west_symmetry_in_summer(self):
        # SE (135) and SW (225) are mirror orientations -> near-equal daily energy under
        # symmetric (no-shadow, flat-temperature) conditions.
        se = _pv(slope=35, aspect=135, day=162)
        sw = _pv(slope=35, aspect=225, day=162)
        self.assertAlmostEqual(se, sw, delta=0.02 * se)

    def test_clearer_sky_gives_more_energy(self):
        dull = _pv(slope=30, aspect=180, cbh=0.5, day=162)
        clear = _pv(slope=30, aspect=180, cbh=1.0, day=162)
        self.assertGreater(clear, dull)

    def test_horizon_shadow_reduces_energy(self):
        clear = _pv(slope=30, aspect=180, day=162)
        # a 40-degree wall all around blocks the low sun:
        walled = _pv(slope=30, aspect=180, horizon=[math.radians(40)] * 8, day=162)
        self.assertLess(walled, clear)

    def test_steep_south_beats_flat_in_winter(self):
        # low winter sun favours a tilt toward it:
        flat = _pv(slope=5, aspect=180, day=17)
        tilted = _pv(slope=35, aspect=180, day=17)
        self.assertGreater(tilted, flat)


# Complete pixel cases captured from the thurso r.pv reference; expected = GRASS r.pv hpv.
# aspect is compass (0 = N, clockwise), converted from the GRASS CCW-from-East aspect r.pv
# consumed (S 270->180, N 90->360, E 360->90).
REGRESSION = [
    dict(tag="june_flat_south", day=162, slope=10.00000, aspect=180.00000, elev=38.1350,
         lat=1.02270667, lon=-0.06163945, linke=3.72466, cbh=0.239581, cdh=1.686120,
         horizon=[0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.060101, 0.0],
         temps=[9.1111, 8.772, 10.1393, 11.7185, 12.4906, 12.7094, 11.8796, 10.1409],
         expected=4156.91064),
    dict(tag="june_steep_south", day=162, slope=34.57690, aspect=180.00000, elev=38.1230,
         lat=1.02270670, lon=-0.06163919, linke=3.72466, cbh=0.239581, cdh=1.686120,
         horizon=[0.0, 0.0, 0.0, 0.0, 0.0, 0.041358, 0.0, 0.0],
         temps=[9.1111, 8.772, 10.1393, 11.7185, 12.4906, 12.7094, 11.8796, 10.1409],
         expected=4059.23120),
    dict(tag="june_steep_north", day=162, slope=36.44596, aspect=360.00000, elev=38.1500,
         lat=1.02270749, lon=-0.06163852, linke=3.72466, cbh=0.239581, cdh=1.686120,
         horizon=[0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
         temps=[9.1111, 8.772, 10.1393, 11.7185, 12.4906, 12.7094, 11.8796, 10.1409],
         expected=3212.17114),
    dict(tag="june_shadowed", day=162, slope=35.63406, aspect=90.00000, elev=31.0300,
         lat=1.02270515, lon=-0.06164170, linke=3.72466, cbh=0.239581, cdh=1.686120,
         horizon=[1.010623, 1.222365, 1.314702, 0.782229, 0.961739, 0.782229, 0.139402, 0.100799],
         temps=[9.1111, 8.772, 10.1393, 11.7185, 12.4906, 12.7094, 11.8796, 10.1409],
         expected=3186.57935),
    dict(tag="jan_flat_south", day=17, slope=10.00000, aspect=180.00000, elev=38.1350,
         lat=1.02270667, lon=-0.06163945, linke=3.18425, cbh=0.235446, cdh=0.836138,
         horizon=[0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.060101, 0.0],
         temps=[2.9296, 2.8031, 2.7901, 2.7777, 4.1343, 4.0183, 3.3056, 3.1786],
         expected=293.96561),
    dict(tag="jan_steep_south", day=17, slope=34.57690, aspect=180.00000, elev=38.1230,
         lat=1.02270670, lon=-0.06163919, linke=3.18425, cbh=0.235446, cdh=0.836138,
         horizon=[0.0, 0.0, 0.0, 0.0, 0.0, 0.041358, 0.0, 0.0],
         temps=[2.9296, 2.8031, 2.7901, 2.7777, 4.1343, 4.0183, 3.3056, 3.1786],
         expected=531.54266),
]


class RegressionTest(unittest.TestCase):
    """Locks the exact validated numbers (GRASS r.pv) without needing the golden rasters."""

    def test_matches_grass_reference(self):
        for case in REGRESSION:
            with self.subTest(tag=case["tag"]):
                port = ir.compute_daily_pv(
                    np.array([case["slope"]]), np.array([case["aspect"]]),
                    np.array([case["elev"]]), np.array([case["lat"]]), np.array([case["lon"]]),
                    np.array(case["horizon"])[None, :], 45.0,
                    np.array([case["linke"]]), np.array([case["cbh"]]), np.array([case["cdh"]]),
                    np.array(case["temps"])[None, :], 0.2,
                    case["day"], -solar_decl(case["day"]), CSI)[0]
                self.assertAlmostEqual(port, case["expected"],
                                       delta=0.005 * case["expected"],
                                       msg=f"{case['tag']}: port {port:.3f} vs {case['expected']:.3f}")


if __name__ == "__main__":
    unittest.main()
