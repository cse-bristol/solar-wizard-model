# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
"""
End-to-end test of the standalone CLI path: buildings from a GeoPackage + a LiDAR
GeoTIFF in, results GeoPackage out, over a throwaway PostGIS cluster - no albion
harness involved.

The fixtures under testdata/e2e/ are a ~330-building patch of central Bristol (OSM
footprints) and the 1m DSM tile covering it, cropped to the building extent plus a
300m margin. The run uses horizon_search_radius=300 to match that margin, which also
keeps the fixture small and the run quick.

Met data comes from a cut-down pvgis_data_uk.tar committed under testdata/e2e/ (the
full UK-wide tar cropped to the fixture extent - see testdata/e2e/README.md), so the
test runs without the ~640MB original (e.g. in CI).

Requires the nix-shell (its proj carries the OSTN15 grids the 27700 transforms need);
skipped only when testing.postgresql is absent.
"""
import os
import unittest
from os.path import exists, dirname, join
from tempfile import TemporaryDirectory

from osgeo import ogr

from solar_pv import cli
from solar_pv.paths import TEST_DATA

try:
    import testing.postgresql
    _HAS_TESTING_POSTGRESQL = True
except ModuleNotFoundError:
    _HAS_TESTING_POSTGRESQL = False

BUILDINGS = join(TEST_DATA, "e2e", "buildings.gpkg")
LIDAR = join(TEST_DATA, "e2e", "lidar.tif")
MET_TAR = join(TEST_DATA, "e2e", "pvgis_data_uk.tar")


@unittest.skipUnless(_HAS_TESTING_POSTGRESQL, "testing.postgresql not installed")
class E2ETest(unittest.TestCase):
    def setUp(self):
        # model_solar_pv reads the tar directory straight from the environment:
        self._prev_pvgis_dir = os.environ.get("PVGIS_DATA_TAR_FILE_DIR")
        os.environ["PVGIS_DATA_TAR_FILE_DIR"] = dirname(MET_TAR)

    def tearDown(self):
        if self._prev_pvgis_dir is None:
            os.environ.pop("PVGIS_DATA_TAR_FILE_DIR", None)
        else:
            os.environ["PVGIS_DATA_TAR_FILE_DIR"] = self._prev_pvgis_dir

    def test_cli_runs_end_to_end(self):
        with TemporaryDirectory(prefix="solar_pv_e2e_") as work_dir:
            out = join(work_dir, "results.gpkg")
            cli.main([
                "--buildings", BUILDINGS,
                "--lidar", LIDAR,
                "--out", out,
                "--id-field", "osm_id",
                "--horizon-search-radius", "300",
                "--work-dir", work_dir,
            ])

            self.assertTrue(exists(out), "CLI produced no output GeoPackage")

            # keep the datasource refs alive - GDAL invalidates layers once their
            # owning datasource is garbage-collected:
            buildings_ds = ogr.Open(BUILDINGS)
            n_buildings_in = buildings_ds.GetLayer(0).GetFeatureCount()
            results = ogr.Open(out)

            pv_building = results.GetLayerByName("pv_building")
            self.assertIsNotNone(pv_building, "pv_building layer missing")
            # every input building gets a row (excluded ones carry an exclusion_reason):
            self.assertEqual(pv_building.GetFeatureCount(), n_buildings_in)

            pv_roof_plane = results.GetLayerByName("pv_roof_plane")
            self.assertIsNotNone(pv_roof_plane, "pv_roof_plane layer missing")
            self.assertGreater(pv_roof_plane.GetFeatureCount(), 0,
                               "no roof planes detected")

            # a detected roof plane should have positive area and annual generation:
            plane = next(iter(pv_roof_plane))
            self.assertGreater(plane.GetField("area_avg"), 0)
            self.assertGreater(plane.GetField("kwh_year_avg"), 0)
