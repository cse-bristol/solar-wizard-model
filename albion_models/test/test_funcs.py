import random
from datetime import datetime
from psycopg2.extras import Json


def gen_bbox():
    xmin = random.uniform(-2.7, -0.4)
    ymin = random.uniform(50.8, 53.6)
    xmax = xmin + 0.05
    ymax = ymin + 0.05
    return xmin, ymin, xmax, ymax


def gen_multipolygon():
    xmin1, ymin1, xmax1, ymax1 = gen_bbox()
    xmin2, ymin2, xmax2, ymax2 = gen_bbox()
    return f"MULTIPOLYGON((({xmin1} {ymax1}, {xmax1} {ymax1}, {xmax1} {ymin1}, {xmin1} {ymin1}, {xmin1} {ymax1}))," \
           f"(({xmin2} {ymax2}, {xmax2} {ymax2}, {xmax2} {ymin2}, {xmin2} {ymin2}, {xmin2} {ymax2})))"


def insert_job(pg_conn, job_id: int, bounds: str, project: str):
    with pg_conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO models.job_queue 
            (job_id, project, created_at, bounds, solar_pv, heat_demand, soft_dig, lidar, status, email, params) 
            VALUES 
            (%s, %s, %s, ST_Transform(ST_Multi(ST_GeomFromText(%s, 4326)), 27700), %s, %s, %s, %s, %s, %s, %s)
            """,
            (job_id, project, datetime.now(), bounds, False, False, True, False, 'NOT_STARTED', None, Json({}))
        )
        pg_conn.commit()
