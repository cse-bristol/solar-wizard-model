import shutil
from os.path import join

import psycopg2
from psycopg2.sql import SQL, Identifier

from albion_models.solar_pv import tables
from albion_models.solar_pv.model_solar_pv import _init_schema


def clone(pg_uri: str, job_id: int, name: str, lidar_dir: str):
    """
    Copy a PV job. This will skip to the 'aggregating horizons' stage
    as it copies the `pixel_horizons` and `roof_planes` tables
    (outputs of SAGA horizon finding and RANSAC for LiDAR stages
    respectively, which are the previous stages).

    This is important if testing changes to the later stages of the
    algorithm as the RANSAC roof-plane detection has a random element.
    It's also faster than running those bits each time.

    This script was created for a one-off purpose so is unlikely to
    be maintained; as it depends on implementation details of the PV
    model, if you need it or similar functionality you should check
    it still works.
    """
    pg_conn = psycopg2.connect(pg_uri)
    new_job_id = _copy_job_queue_entry(pg_conn, job_id, name)

    _copy_lidar_vrt(job_id, new_job_id, lidar_dir)
    _init_schema(pg_uri, new_job_id)
    _copy_pixel_horizons(pg_conn, job_id, new_job_id)
    _copy_roof_planes(pg_conn, job_id, new_job_id)
    print(f"Job {job_id} copied as job {new_job_id} with name '{name}'")


def _copy_job_queue_entry(pg_conn, job_id: int, name: str) -> int:
    with pg_conn.cursor() as cursor:
        cursor.execute("""
            INSERT INTO models.job_queue (
                project, 
                created_at, 
                bounds, 
                solar_pv, 
                heat_demand, 
                soft_dig, 
                lidar, 
                solar_pv_cost_benefit, 
                status, 
                email, 
                params 
            )
            SELECT
                %(name)s, 
                NOW(), 
                bounds, 
                solar_pv, 
                false, 
                false, 
                false, 
                false, 
                'NOT_STARTED', 
                email, 
                params
            FROM models.job_queue
            WHERE job_id = %(job_id)s
            RETURNING job_id
        """, {"job_id": job_id,
              "name": name})
        pg_conn.commit()
        return cursor.fetchone()[0]


def _copy_lidar_vrt(job_id: int, new_job_id: int, lidar_dir: str):
    shutil.copytree(
        join(lidar_dir, f"job_{job_id}"),
        join(lidar_dir, f"job_{new_job_id}"))


def _copy_pixel_horizons(pg_conn, job_id: int, new_job_id: int):
    with pg_conn.cursor() as cursor:
        cursor.execute(SQL("""
            CREATE TABLE {new_pixel_horizons} AS SELECT * FROM {old_pixel_horizons};
            
            ALTER TABLE {new_pixel_horizons} ADD PRIMARY KEY (pixel_id);
            CREATE INDEX ON {new_pixel_horizons} (roof_plane_id);
            
            ALTER TABLE {new_pixel_horizons} ALTER COLUMN en SET DATA TYPE geometry(Point,27700);
            CREATE INDEX ON {new_pixel_horizons} USING GIST (en);
        """).format(
            old_pixel_horizons=Identifier(tables.schema(job_id), tables.PIXEL_HORIZON_TABLE),
            new_pixel_horizons=Identifier(tables.schema(new_job_id), tables.PIXEL_HORIZON_TABLE),
        ))
        pg_conn.commit()


def _copy_roof_planes(pg_conn, job_id: int, new_job_id: int):
    with pg_conn.cursor() as cursor:
        cursor.execute(SQL("""
            INSERT INTO {new_roof_planes} SELECT * FROM {old_roof_planes}
        """).format(
            old_roof_planes=Identifier(tables.schema(job_id), tables.ROOF_PLANE_TABLE),
            new_roof_planes=Identifier(tables.schema(new_job_id), tables.ROOF_PLANE_TABLE),
        ))
        pg_conn.commit()


if __name__ == '__main__':
    clone('postgresql://albion_webapp:ydBbE3JCnJ4@localhost:5432/albion', 21, 'per-panel-archetype-7', '/home/neil/data/albion-models/lidar')
