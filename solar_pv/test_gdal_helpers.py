# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import os
import shutil
import tempfile
import unittest
from os.path import join

import numpy as np
from osgeo import gdal

try:
    import testing.postgresql
    _HAS_TESTING_POSTGRESQL = True
except ModuleNotFoundError:
    _HAS_TESTING_POSTGRESQL = False

import psycopg2

from solar_pv import gdal_helpers

gdal.UseExceptions()

# Anchor test data to this file, not the cwd, so the test runs from any directory.
_LIDAR_TIF: str = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "testdata", "e2e", "lidar.tif")


def _read(path: str) -> np.ndarray:
    """Read band 1 as float64 with the raster's nodata turned into NaN."""
    ds = gdal.Open(path)
    band = ds.GetRasterBand(1)
    arr = band.ReadAsArray().astype(np.float64)
    nodata = band.GetNoDataValue()
    if nodata is not None:
        arr[arr == nodata] = np.nan
    return arr


class GdalHelpersRasterTests(unittest.TestCase):
    """Raster helpers that need no database - driven off the e2e LiDAR tile."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_create_vrt(self):
        vrt = join(self.tmp, "tiles.vrt")
        gdal_helpers.create_vrt([_LIDAR_TIF], vrt)

        self.assertTrue(os.path.exists(vrt))
        src = gdal.Open(_LIDAR_TIF)
        out = gdal.Open(vrt)
        self.assertEqual((src.RasterXSize, src.RasterYSize),
                         (out.RasterXSize, out.RasterYSize))

    def test_aspect(self):
        out = join(self.tmp, "aspect.tif")
        gdal_helpers.aspect(_LIDAR_TIF, out)

        arr = _read(out)
        vals = arr[~np.isnan(arr)]
        self.assertGreater(vals.size, 0)
        # Aspect is a compass bearing (0 = flat, via -zero_for_flat).
        self.assertGreaterEqual(float(vals.min()), 0.0)
        self.assertLessEqual(float(vals.max()), 360.0)

    def test_slope(self):
        out = join(self.tmp, "slope.tif")
        gdal_helpers.slope(_LIDAR_TIF, out)

        arr = _read(out)
        vals = arr[~np.isnan(arr)]
        self.assertGreater(vals.size, 0)
        # Slope in degrees.
        self.assertGreaterEqual(float(vals.min()), 0.0)
        self.assertLessEqual(float(vals.max()), 90.0)


@unittest.skipUnless(_HAS_TESTING_POSTGRESQL, "testing.postgresql not installed")
class GdalHelpersRasterizeTests(unittest.TestCase):
    """Rasterize helpers, which read geometry from a PG: source.

    The mask SQL is written with the double-quoted identifiers and single-quoted
    literals that psycopg2's `Identifier(...).as_string()` produces in real use, so
    the tests exercise exactly the quoting the argv form has to pass through
    untouched.
    """

    # A 10m square and its neighbour to the east, in EPSG:27700.
    _SQUARE_A = "POLYGON((400000 300000,400000 300010,400010 300010,400010 300000,400000 300000))"
    _SQUARE_B = "POLYGON((400010 300000,400010 300010,400020 300010,400020 300000,400010 300000))"

    def setUp(self):
        self.postgresql = testing.postgresql.Postgresql()
        self.pg_uri: str = self.postgresql.url()
        self.tmp: str = tempfile.mkdtemp()
        with psycopg2.connect(self.pg_uri) as conn, conn.cursor() as curs:
            curs.execute(f"""
                CREATE EXTENSION postgis;
                CREATE SCHEMA "t";
                CREATE TABLE "t"."poly" (label text, geom geometry(Polygon, 27700));
                INSERT INTO "t"."poly" (label, geom) VALUES
                    ('a', ST_GeomFromText('{self._SQUARE_A}', 27700)),
                    ('b', ST_GeomFromText('{self._SQUARE_B}', 27700));
                """)
            conn.commit()

    def tearDown(self):
        self.postgresql.stop()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_rasterize(self):
        out = join(self.tmp, "mask.tif")
        gdal_helpers.rasterize(
            self.pg_uri, 'SELECT "geom" FROM "t"."poly"', out, res=1, srid=27700)

        arr = _read(out)
        # Two 10x10m squares burned as 1, everything else initialised to 0.
        self.assertAlmostEqual((arr == 1).sum(), 200, delta=40)
        self.assertTrue(((arr == 0) | (arr == 1)).all())

    def test_rasterize_3d(self):
        out = join(self.tmp, "aspect.tif")
        gdal_helpers.rasterize_3d(
            self.pg_uri,
            "SELECT ST_Force3D(\"geom\", 42.0) FROM \"t\".\"poly\" WHERE label = 'a'",
            out, res=1.0, srid=27700,
            bounds=(400000, 300000, 400010, 300010))

        arr = _read(out)
        covered = arr[~np.isnan(arr)]
        self.assertGreater(covered.size, 50)
        # Z (42) is burned into every covered pixel; outside the polygon is NaN.
        np.testing.assert_allclose(covered, 42.0)


if __name__ == '__main__':
    unittest.main()
