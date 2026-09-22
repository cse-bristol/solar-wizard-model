# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import unittest

try:
    import testing.postgresql
    _HAS_TESTING_POSTGRESQL = True
except ModuleNotFoundError:
    _HAS_TESTING_POSTGRESQL = False

import psycopg2
import psycopg2.extras
from shapely.geometry import Polygon

from solar_pv.buildings import BuildingInput, load_buildings

_TEST_JOB_ID: int = 0

# Two 10m squares in EPSG:27700, one given as WKT and one as a shapely geometry:
_WKT_A = "POLYGON((100 100, 110 100, 110 110, 100 110, 100 100))"
_GEOM_B = Polygon([(200, 300), (215 ,300), (215, 315), (200, 315), (200, 300)])


@unittest.skipUnless(_HAS_TESTING_POSTGRESQL, "testing.postgresql not installed")
class LoadBuildingsTests(unittest.TestCase):
    def setUp(self):
        self.postgresql = testing.postgresql.Postgresql()
        self.pg_uri: str = self.postgresql.url()
        # The two empty per-job tables the building loader fills; mirrors the DDL in
        # database/create.schema.sql (kept minimal so this test needs no raster extension).
        with psycopg2.connect(self.pg_uri) as conn, conn.cursor() as curs:
            curs.execute("""
                CREATE EXTENSION postgis;
                CREATE SCHEMA models;
                CREATE TYPE models.pv_exclusion_reason AS ENUM (
                    'NO_LIDAR_COVERAGE', 'OUTDATED_LIDAR_COVERAGE',
                    'NO_ROOF_PLANES_DETECTED', 'ALL_ROOF_PLANES_UNUSABLE', 'TOO_SMALL');
                CREATE SCHEMA solar_pv_job_0;
                CREATE TABLE solar_pv_job_0.buildings (
                    building_id text,
                    geom_27700 geometry,
                    geom_27700_buffered_5 geometry,
                    exclusion_reason models.pv_exclusion_reason,
                    height real,
                    min_ground_height real,
                    max_ground_height real);
                CREATE TABLE solar_pv_job_0.bounds_27700 (
                    job_id int,
                    bounds_27700 geometry(multipolygon, 27700));
            """)
            conn.commit()

    def tearDown(self):
        self.postgresql.stop()

    def _buildings(self):
        return [
            BuildingInput(building_id="a", geom_27700=_WKT_A, height=12.5),
            BuildingInput(building_id="b", geom_27700=_GEOM_B),
        ]

    def test_load_buildings(self):
        load_buildings(self.pg_uri, _TEST_JOB_ID, self._buildings())

        with psycopg2.connect(self.pg_uri, cursor_factory=psycopg2.extras.DictCursor) as conn:
            with conn.cursor() as curs:
                curs.execute("""
                    SELECT building_id,
                           ST_SRID(geom_27700) AS srid,
                           height,
                           min_ground_height, max_ground_height,
                           exclusion_reason,
                           ST_Area(geom_27700_buffered_5) > ST_Area(geom_27700) AS moat_bigger
                    FROM solar_pv_job_0.buildings ORDER BY building_id;
                """)
                rows = {r["building_id"]: r for r in curs.fetchall()}

        self.assertEqual({"a", "b"}, set(rows))
        self.assertEqual(27700, rows["a"]["srid"])
        self.assertAlmostEqual(12.5, rows["a"]["height"], 3)
        self.assertIsNone(rows["a"]["exclusion_reason"])
        self.assertTrue(rows["a"]["moat_bigger"])
        self.assertIsNone(rows["b"]["height"])
        # Ground heights are not caller inputs; the loader leaves them NULL for
        # outdated_lidar_check to fill in later:
        self.assertIsNone(rows["a"]["min_ground_height"])
        self.assertIsNone(rows["a"]["max_ground_height"])

    def test_bounds_derived_from_building_extent(self):
        load_buildings(self.pg_uri, _TEST_JOB_ID, self._buildings())

        with psycopg2.connect(self.pg_uri, cursor_factory=psycopg2.extras.DictCursor) as conn:
            with conn.cursor() as curs:
                curs.execute("""
                    SELECT job_id, ST_SRID(bounds_27700) AS srid,
                           ST_XMin(bounds_27700), ST_YMin(bounds_27700),
                           ST_XMax(bounds_27700), ST_YMax(bounds_27700)
                    FROM solar_pv_job_0.bounds_27700;
                """)
                rows = curs.fetchall()

        self.assertEqual(1, len(rows))
        job_id, srid, xmin, ymin, xmax, ymax = rows[0]
        self.assertEqual(_TEST_JOB_ID, job_id)
        self.assertEqual(27700, srid)
        # The extent spans both buildings (100..215, 100..315):
        self.assertAlmostEqual(100, xmin, 3)
        self.assertAlmostEqual(100, ymin, 3)
        self.assertAlmostEqual(215, xmax, 3)
        self.assertAlmostEqual(315, ymax, 3)

    def test_load_is_idempotent(self):
        load_buildings(self.pg_uri, _TEST_JOB_ID, self._buildings())
        # A second load (e.g. a resumed run) must not duplicate rows:
        load_buildings(self.pg_uri, _TEST_JOB_ID, self._buildings())

        with psycopg2.connect(self.pg_uri) as conn, conn.cursor() as curs:
            curs.execute("SELECT COUNT(*) FROM solar_pv_job_0.buildings;")
            self.assertEqual(2, curs.fetchone()[0])
            curs.execute("SELECT COUNT(*) FROM solar_pv_job_0.bounds_27700;")
            self.assertEqual(1, curs.fetchone()[0])


if __name__ == '__main__':
    unittest.main()
