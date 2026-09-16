# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import unittest

import numpy as np

from solar_pv.pv import pv_model

# csi.coeffs (coeffs[7] = 0.035 module-temp rise per W/m^2):
CSI = [1.000436, -0.017237, -0.040465, -0.004702, 0.000149, 0.000170, 0.000005, 0.035]


class EfficiencyTest(unittest.TestCase):

    def test_unity_at_stc(self):
        # STC = 1000 W/m^2, module temp 25C. Module temp = irr*0.035 + ambient, so ambient
        # = 25 - 35 = -10 gives module temp exactly 25 -> tprime 0, lnrelirr 0 -> pm/c0 = 1.
        eff = pv_model.efficiency(np.array([1000.0]), np.array([-10.0]), CSI)
        self.assertAlmostEqual(eff[0], 1.0, places=9)

    def test_zero_or_negative_irradiance(self):
        eff = pv_model.efficiency(np.array([0.0, -5.0]), np.array([20.0, 20.0]), CSI)
        np.testing.assert_array_equal(eff, [0.0, 0.0])

    def test_hotter_is_less_efficient(self):
        irr = np.array([800.0, 800.0])
        eff = pv_model.efficiency(irr, np.array([0.0, 30.0]), CSI)
        self.assertGreater(eff[0], eff[1])
        # relative efficiency stays in a sane band:
        self.assertTrue(np.all((eff > 0.8) & (eff < 1.15)))


class TemperatureInterpolateTest(unittest.TestCase):

    def setUp(self):
        # 8 three-hourly slots at 00,03,...,21:
        self.temps = np.array([[0.0, 3.0, 6.0, 9.0, 12.0, 15.0, 18.0, 21.0]])
        self.lon0 = np.array([0.0])  # zero longitude -> locTime == presTime

    def test_on_slot_boundary(self):
        # presTime 6.0 -> slot 2 exactly -> temps[2] = 6.0
        t = pv_model.interpolate_temperature(self.temps, 6.0, self.lon0)
        self.assertAlmostEqual(t[0], 6.0, places=6)

    def test_midway_between_slots(self):
        # presTime 7.5 -> slot 2, fraction 1.5/3 -> 6 + 0.5*(9-6) = 7.5
        t = pv_model.interpolate_temperature(self.temps, 7.5, self.lon0)
        self.assertAlmostEqual(t[0], 7.5, places=6)

    def test_longitude_shifts_local_time(self):
        # +15 deg longitude advances local time by 1 h vs lon 0:
        t0 = pv_model.interpolate_temperature(self.temps, 7.5, np.radians([0.0]))
        t_east = pv_model.interpolate_temperature(self.temps, 7.5, np.radians([15.0]))
        self.assertNotAlmostEqual(t0[0], t_east[0], places=3)

    def test_wraps_past_midnight(self):
        # presTime 23.5 -> slot 7, interpolates toward slot 0 (wrap):
        t = pv_model.interpolate_temperature(self.temps, 23.5, self.lon0)
        self.assertTrue(np.isfinite(t[0]))


if __name__ == "__main__":
    unittest.main()
