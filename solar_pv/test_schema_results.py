# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import unittest

try:
    import testing.postgresql
    _HAS_TESTING_POSTGRESQL = True
except ModuleNotFoundError:
    _HAS_TESTING_POSTGRESQL = False

import psycopg2

from solar_pv.model_solar_pv import _init_schema, _load_results

_TEST_JOB_ID: int = 0

# The models.pv_roof_plane columns that are NOT NULL other than the ones set explicitly
# below (building_id, roof_plane_id, job_id, roof_geom_4326, horizon, meta). Filled with a
# placeholder so the round-trip test can insert a row without hand-listing all 48 kWh columns.
_MONTHS = ["jan", "feb", "mar", "apr", "may", "jun",
           "jul", "aug", "sep", "oct", "nov", "dec"]
_NUMERIC_COLS = (
    [f"kwh_{m}_{s}" for m in _MONTHS for s in ("min", "avg", "max")]
    + [f"kwh_year_{s}" for s in ("min", "avg", "max")]
    + [f"kwp_{s}" for s in ("min", "avg", "max")]
    + ["kwh_per_kwp"]
    + [f"area_{s}" for s in ("min", "avg", "max")]
    + ["x_coef", "y_coef", "intercept", "slope", "aspect"])


@unittest.skipUnless(_HAS_TESTING_POSTGRESQL, "testing.postgresql not installed")
class SchemaResultsTests(unittest.TestCase):
    def setUp(self):
        self.postgresql = testing.postgresql.Postgresql()
        self.pg_uri: str = self.postgresql.url()
        with psycopg2.connect(self.pg_uri) as conn, conn.cursor() as curs:
            curs.execute("CREATE EXTENSION postgis; CREATE EXTENSION postgis_raster;")
            conn.commit()

    def tearDown(self):
        self.postgresql.stop()

    def test_init_schema_runs(self):
        _init_schema(self.pg_uri, _TEST_JOB_ID)

    def test_load_results_round_trips_buildings_and_roof_planes(self):
        _init_schema(self.pg_uri, _TEST_JOB_ID)
        with psycopg2.connect(self.pg_uri) as conn, conn.cursor() as curs:
            curs.execute(
                "INSERT INTO models.pv_building VALUES "
                "(%s, 'a', 'TOO_SMALL', 5.0), (%s, 'b', NULL, 9.0)",
                (_TEST_JOB_ID, _TEST_JOB_ID))
            numeric = ", ".join(_NUMERIC_COLS)
            placeholders = ", ".join(["1.0"] * len(_NUMERIC_COLS))
            curs.execute(f"""
                INSERT INTO models.pv_roof_plane
                    (building_id, roof_plane_id, job_id, roof_geom_4326,
                     horizon, is_flat, meta, {numeric})
                VALUES
                    ('a', 1, {_TEST_JOB_ID},
                     ST_SetSrid(ST_GeomFromText('POLYGON((0 0,0 1,1 1,1 0,0 0))'), 4326),
                     ARRAY[0.1, 0.2]::real[], false, '{{"k": 1}}'::jsonb, {placeholders})
            """)
            conn.commit()

        res = _load_results(self.pg_uri, _TEST_JOB_ID)

        self.assertEqual([b["building_id"] for b in res.buildings], ["a", "b"])
        self.assertEqual(res.buildings[0]["exclusion_reason"], "TOO_SMALL")
        self.assertIsNone(res.buildings[1]["exclusion_reason"])

        self.assertEqual(len(res.roof_planes), 1)
        rp = res.roof_planes[0]
        self.assertTrue(rp["roof_geom_4326"].startswith("POLYGON"))
        self.assertEqual(rp["horizon"], [0.1, 0.2])
        self.assertEqual(rp["meta"], {"k": 1})

    def test_load_results_empty_when_no_rows(self):
        _init_schema(self.pg_uri, _TEST_JOB_ID)
        res = _load_results(self.pg_uri, _TEST_JOB_ID)
        self.assertEqual(res.buildings, [])
        self.assertEqual(res.roof_planes, [])


if __name__ == "__main__":
    unittest.main()
