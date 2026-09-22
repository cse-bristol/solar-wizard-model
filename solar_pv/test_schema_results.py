# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import unittest

try:
    import testing.postgresql
    _HAS_TESTING_POSTGRESQL = True
except ModuleNotFoundError:
    _HAS_TESTING_POSTGRESQL = False

import psycopg2

from solar_pv.model_solar_pv import _init_schema

_TEST_JOB_ID: int = 0


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


if __name__ == "__main__":
    unittest.main()
