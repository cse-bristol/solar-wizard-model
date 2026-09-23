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
import json
import os
import unittest
from os.path import exists, dirname, join
from tempfile import TemporaryDirectory

from osgeo import ogr

from solar_pv import cli
from solar_pv.constants import CONFIDENCE_WEIGHTS
from solar_pv.paths import TEST_DATA
from solar_pv.pv.confidence import combine

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

            # a detected roof plane should have positive area and annual generation,
            # with the P90 below the central estimate and a confidence in [0,1]:
            plane = next(iter(pv_roof_plane))
            self.assertGreater(plane.GetField("area"), 0)
            self.assertGreater(plane.GetField("kwh_year"), 0)
            self.assertLess(plane.GetField("kwh_year_p90"), plane.GetField("kwh_year"))
            self.assertGreaterEqual(plane.GetField("confidence"), 0)
            self.assertLessEqual(plane.GetField("confidence"), 1)

            pv_roof_plane.ResetReading()
            n_merged = 0
            for plane in pv_roof_plane:
                self._check_confidence_meta(plane)
                if "_MERGED_" in json.loads(plane.GetField("meta"))["plane_type"]:
                    n_merged += 1
            # make sure the merge path, which recomputes the aspect stats, was exercised:
            self.assertGreater(n_merged, 0, "no merged roof planes in the fixture")

    def _check_confidence_meta(self, plane):
        """The confidence sub-scores in `meta` survive to the GeoPackage, are
        consistent with the `confidence` column, and were derived from real
        per-plane stats."""
        rp_id = plane.GetField("roof_plane_id")
        meta = json.loads(plane.GetField("meta"))
        sub_scores = meta.get("confidence")
        self.assertIsInstance(sub_scores, dict, f"roof plane {rp_id}: no meta.confidence")
        self.assertEqual(set(sub_scores), set(CONFIDENCE_WEIGHTS), f"roof plane {rp_id}")
        for k, v in sub_scores.items():
            self.assertGreaterEqual(v, 0, f"roof plane {rp_id}: {k}")
            self.assertLessEqual(v, 1, f"roof plane {rp_id}: {k}")
        self.assertAlmostEqual(combine(sub_scores), plane.GetField("confidence"),
                               delta=1e-4, msg=f"roof plane {rp_id}")
        # real LiDAR pixel aspects never agree exactly, so an sd of 0 means the stat
        # was never computed:
        if not plane.GetField("is_flat"):
            self.assertGreater(meta["aspect_circ_sd"], 0, f"roof plane {rp_id}: {meta['plane_type']}")
