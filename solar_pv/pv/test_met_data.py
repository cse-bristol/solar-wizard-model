# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import os
import unittest
from os.path import join

import numpy as np

from solar_pv.paths import PROJECT_ROOT
from solar_pv.pv import met_data

MET_TAR = join(os.environ.get("PVGIS_DATA_TAR_FILE_DIR", PROJECT_ROOT), "pvgis_data_uk.tar")


@unittest.skipUnless(os.path.exists(MET_TAR), "pvgis_data_uk.tar not present")
class MetDataTest(unittest.TestCase):
    # a small EPSG:27700 grid over mainland Britain (near Bristol):
    GT = (360000.0, 20.0, 0.0, 172000.0, 0.0, -20.0)
    SHAPE = (10, 10)

    def _june(self):
        return met_data.MetData(MET_TAR, self.GT, self.SHAPE, resample="near").for_month(6)

    def test_layers_present_and_plausible(self):
        m = self._june()
        self.assertEqual(m.linke.shape, self.SHAPE)
        self.assertEqual(m.temps8.shape, self.SHAPE + (8,))
        self.assertTrue(np.all((m.linke > 1.5) & (m.linke < 8)))   # UK Linke turbidity
        self.assertTrue(np.all((m.cbh > 0) & (m.cbh < 3)))
        self.assertTrue(np.all((m.cdh > 0) & (m.cdh < 3)))
        self.assertTrue(np.all((m.temps8 > -20) & (m.temps8 < 40)))

    def test_summer_midday_warmer_than_predawn(self):
        m = self._june()
        self.assertGreater(np.nanmean(m.temps8[..., 4]),   # 12:00
                           np.nanmean(m.temps8[..., 1]))   # 03:00

    def test_corrections_near_one(self):
        m = self._june()
        # wind/spectral are multiplicative corrections around unity (or absent -> None):
        for corr in (m.wind, m.spectral):
            if corr is not None:
                finite = corr[np.isfinite(corr)]
                self.assertTrue(np.all((finite > 0.7) & (finite < 1.3)))

    def test_missing_layer_returns_none_not_error(self):
        # a bogus panel name has no spectral raster -> None (not a crash):
        m = met_data.MetData(MET_TAR, self.GT, self.SHAPE, panel="NoSuchPanel").for_month(6)
        self.assertIsNone(m.spectral)


if __name__ == "__main__":
    unittest.main()
