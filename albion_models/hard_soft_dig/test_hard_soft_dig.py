import logging
import os
import unittest

import psycopg2
import psycopg2.extras

from albion_models.hard_soft_dig.model_hard_soft_dig import model_hard_soft_dig
from albion_models.test.test_funcs import gen_multipolygon, insert_job


class HardSoftDigTestCase(unittest.TestCase):
    """
    This test requires a postGIS database loaded with:
    * OS greenspace dataset
    * OSMM highways dataset
    * OS mastermap natural land dataset.

    The PG_URI environment variable should be set to the postgres connection URI.
    """

    @classmethod
    def setUpClass(self):
        pg_uri = os.environ.get("PG_URI")
        self.pg_conn = psycopg2.connect(pg_uri, cursor_factory=psycopg2.extras.DictCursor)
        logging.basicConfig(level=logging.DEBUG, format='[%(asctime)s] %(levelname)s: %(message)s')

    def test_overlapping_roads(self):
        bounds = gen_multipolygon()
        insert_job(self.pg_conn, 999, bounds, 'test')
        model_hard_soft_dig(self.pg_conn, 999, bounds, 10)
        with self.pg_conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*) FROM (
                    SELECT ST_Length(ST_Intersection(d1.geom_4326, d2.geom_4326)) len
                    FROM models.hard_soft_dig d1
                        INNER JOIN models.hard_soft_dig d2 
                        ON ST_Intersects(d1.geom_4326, d2.geom_4326)
                        AND d1.ctid != d2.ctid
                        WHERE d1.job_id = 999 AND d2.job_id = 999
                ) l WHERE len > 0
                """
            )
            count = cursor.fetchone()[0]
            self.pg_conn.commit()
            assert count == 0, f"bounds {bounds} had {count} overlapping roads"

    def tearDown(self):
        self.pg_conn.rollback()
        with self.pg_conn.cursor() as cursor:
            cursor.execute("DELETE FROM models.hard_soft_dig WHERE job_id = 999")
            cursor.execute("DROP VIEW models.hard_soft_dig_job_999")
            cursor.execute("DELETE FROM models.job_queue WHERE job_id = 999")
            self.pg_conn.commit()
