# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import os
import subprocess
import unittest
from os.path import join
from statistics import mean

try:
    import testing.postgresql
except ModuleNotFoundError:
    pass

import psycopg2
import psycopg2.extras

from solar_pv.rasters import create_elevation_override_raster

_TEST_ELEVATION_RASTER: str = os.path.realpath(
    "../testdata/solar_pv/rasters/inputs/elevation_4326.tif")
_TEST_OUT_DIR: str = os.path.realpath("../../testdata/solar_pv/rasters/outputs")
_TEST_JOB_ID: int = 0

_HEIGHTS = {
    't0': (78.8, 79.4),
    't1': (82.2, 84),
    't2': (82, 83.9),
    't3': (81.1, 83.5),
}


@unittest.skip("db test")
class RastersTests(unittest.TestCase):
    def setUp(self):
        self.postgresql = testing.postgresql.Postgresql()
        self.pg_uri: str = self.postgresql.url()

    def tearDown(self):
        self.postgresql.stop()

    def _create_test_db_tables(self):
        # create mastermap. building & height tables, and job buildings table

        sql_init: str = f"""
            CREATE EXTENSION postgis;
            CREATE SCHEMA mastermap;
            CREATE TABLE mastermap.building
            (
               toid text PRIMARY KEY NOT NULL,
               feature_code int,
               version int,
               version_date varchar(10),
               theme _varchar,
               calculated_area float(19),
               change_date _varchar,
               reason_for_change _varchar,
               descriptive_group _varchar,
               descriptive_term _varchar,
               make varchar(8),
               physical_level text,
               geom_4326 geometry
            );  
            CREATE TABLE mastermap.height
            (
               toid text PRIMARY KEY NOT NULL,
               version int,
               process_date date,
               tile_ref varchar(6),
               abs_hmin real,
               abs_h2 real,
               abs_hmax real,
               rel_h2 real,
               rel_hmax real,
               confidence text,
               height real
            );
            INSERT INTO mastermap.building (toid, geom_4326) VALUES ('t0', ST_GeomFromText(
                'POLYGON((-1.701755491120342 55.405670016335115,-1.701774159014207 55.40570131361896,-1.701847697220748 55.40568711576654,-1.70182904506775 55.4056558185327,-1.701755491120342 55.405670016335115))',
                4326));
            INSERT INTO mastermap.height (toid, abs_h2, abs_hmax) VALUES ('t0', {_HEIGHTS['t0'][0]}, {_HEIGHTS['t0'][1]});
            
            INSERT INTO mastermap.building (toid, geom_4326) VALUES ('t1', ST_GeomFromText(
                'POLYGON((-1.701628900599498 55.40562845520247,-1.70165652029864 55.405672641656324,-1.701814677619248 55.40564067880234,-1.701802819033078 55.40562160051307,-1.701776406869783 55.40562692753866,-1.70176064629374 55.4056017295532,-1.701628900599498 55.40562845520247))',
                4326));
            INSERT INTO mastermap.height (toid, abs_h2, abs_hmax) VALUES ('t1', {_HEIGHTS['t1'][0]}, {_HEIGHTS['t1'][1]});
            
            INSERT INTO mastermap.building (toid, geom_4326) VALUES ('t2', ST_GeomFromText(
                'POLYGON((-1.70176064629374 55.4056017295532,-1.701757369872127 55.4055964200859,-1.701795644468265 55.40558860599131,-1.701803757421909 55.405580718443254,-1.701797982008142 55.40557171880712,-1.701783161665352 55.40556853776669,-1.70177114130591 55.4055710244541,-1.701759280738021 55.40555221572231,-1.701601123721201 55.40558417850271,-1.701628900599498 55.40562845520247,-1.70176064629374 55.4056017295532))',
                4326));
            INSERT INTO mastermap.height (toid, abs_h2, abs_hmax) VALUES ('t2', {_HEIGHTS['t2'][0]}, {_HEIGHTS['t2'][1]}); 

            INSERT INTO mastermap.building (toid, geom_4326) VALUES ('t3', ST_GeomFromText(
                'POLYGON((-1.701839648285043 55.4057854073153,-1.70182857584937 55.405766780222216,-1.701882187217655 55.405756487480176,-1.7018640955462 55.405726251914594,-1.70181064212845 55.40573654503298,-1.701791615840049 55.4057045999214,-1.701687081092666 55.40572474257132,-1.70170563828322 55.40575605756712,-1.701683023397172 55.4057604054099,-1.701693628088101 55.405778222676005,-1.701716243657346 55.40577378497713,-1.70173511335185 55.4058055500075,-1.701839648285043 55.4057854073153))',
                4326));
            INSERT INTO mastermap.height (toid, abs_h2, abs_hmax) VALUES ('t3', {_HEIGHTS['t3'][0]}, {_HEIGHTS['t3'][1]}); 
            
            CREATE SCHEMA models;
            CREATE TYPE models.pv_exclusion_reason AS ENUM (
                'NO_LIDAR_COVERAGE',
                'OUTDATED_LIDAR_COVERAGE',
                'NO_ROOF_PLANES_DETECTED',
                'ALL_ROOF_PLANES_UNUSABLE'
            );                       
            CREATE SCHEMA solar_pv_job_0;
            CREATE TABLE solar_pv_job_0.buildings
            (
               toid text,
               geom_27700 geometry,
               exclusion_reason models.pv_exclusion_reason,
               height real
            );
            INSERT INTO solar_pv_job_0.buildings (toid) VALUES ('t0');
            INSERT INTO solar_pv_job_0.buildings (toid) VALUES ('t1');
            INSERT INTO solar_pv_job_0.buildings (toid) VALUES ('t2');
            """

        with psycopg2.connect(self.pg_uri, cursor_factory=psycopg2.extras.DictCursor) as conn:
            with conn.cursor() as curs:
                curs.execute(sql_init)

    def test_create_elevation_override_raster(self):
        """Test an elevation override raster gets the expected heights
        """
        self._create_test_db_tables()

        # 1.
        # Initially no outdated lidar buildings
        e_o_r = create_elevation_override_raster(self.pg_uri, _TEST_JOB_ID, _TEST_OUT_DIR, _TEST_ELEVATION_RASTER)
        self.assertIsNone(e_o_r)

        # 2.
        # With outdated lidar buildings
        with psycopg2.connect(self.pg_uri, cursor_factory=psycopg2.extras.DictCursor) as conn:
            with conn.cursor() as curs:
                # Change a building to have outdated lidar
                curs.execute("UPDATE solar_pv_job_0.buildings set exclusion_reason = 'OUTDATED_LIDAR_COVERAGE'::models.pv_exclusion_reason WHERE toid = 't0';"
                             "UPDATE solar_pv_job_0.buildings set exclusion_reason = 'OUTDATED_LIDAR_COVERAGE'::models.pv_exclusion_reason WHERE toid = 't1';")
                conn.commit()
                e_o_r = create_elevation_override_raster(self.pg_uri, _TEST_JOB_ID, _TEST_OUT_DIR, _TEST_ELEVATION_RASTER)
                self.assertIsNotNone(e_o_r)

                # Get centres => test points
                curs.execute("SELECT toid, ST_X(ST_Centroid(m.geom_4326)), ST_Y(ST_Centroid(m.geom_4326)), b.exclusion_reason = 'OUTDATED_LIDAR_COVERAGE'::models.pv_exclusion_reason "
                             "FROM mastermap.building m "
                             "JOIN solar_pv_job_0.buildings b USING (toid) ")
                test_points = curs.fetchall()

                # Get values at test points
                patch_raster_filename: str = join(_TEST_OUT_DIR, 'elevation_override.tif')
                for (toid, test_point_x, test_point_y, exp_height) in test_points:
                    res = subprocess.run(f"""
                        gdallocationinfo
                        -valonly
                        -geoloc
                        {patch_raster_filename} {test_point_x} {test_point_y}
                        """.replace("\n", " "), capture_output=True, text=True, shell=True)
                    self.assertIs(len(res.stderr), 0, f"Error running gdallocationinfo {res.stderr}")
                    if exp_height:
                        height = float(res.stdout)
                        exp_height = mean(_HEIGHTS[toid])
                        self.assertAlmostEqual(exp_height, height, 3, f"{toid}, exp {exp_height}, act {height}")
                    else:
                        self.assertIs(len(res.stdout.strip()), 0)


if __name__ == '__main__':
    unittest.main()
