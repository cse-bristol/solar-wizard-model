import logging
import os
import unittest
from os.path import join

import psycopg2
import psycopg2.extras

from albion_models.heat_demand.model_heat_demand import model_heat_demand
from albion_models.paths import PROJECT_ROOT
from albion_models.test.test_funcs import gen_multipolygon, insert_job


class HeatDemandModelTestCase(unittest.TestCase):
    """
    This test requires a postGIS database loaded with:
    * OS mastermap buildings
    * OS mastermap building heights
    * Buildings aggregate table which requires addressbase and epc data (with addresses matched)

    The PG_URI environment variable should be set to the postgres connection URI.
    """

    @classmethod
    def setUpClass(self):
        pg_uri = os.environ.get("PG_URI")
        self.pg_conn = psycopg2.connect(pg_uri, cursor_factory=psycopg2.extras.DictCursor)
        logging.basicConfig(level=logging.DEBUG, format='[%(asctime)s] %(levelname)s: %(message)s')

    def test_model_integration(self):
        bounds = gen_multipolygon()
        insert_job(self.pg_conn, 999, bounds, 'test')
        model_heat_demand(self.pg_conn, 999, bounds, [], join(PROJECT_ROOT, 'heat_demand'), 2033.313)

    def tearDown(self):
        self.pg_conn.rollback()
        with self.pg_conn.cursor() as cursor:
            cursor.execute("DELETE FROM models.heat_demand WHERE job_id = 999")
            cursor.execute("DROP VIEW IF EXISTS models.heat_demand_job_999")
            cursor.execute("DELETE FROM models.job_queue WHERE job_id = 999")
            self.pg_conn.commit()
