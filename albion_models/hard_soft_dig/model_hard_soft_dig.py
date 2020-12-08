from os.path import join

from psycopg2.sql import SQL, Identifier

from albion_models.paths import SQL_DIR


def model_hard_soft_dig(pg_conn, job_id: int, bounds: str, soft_ground_buffer_metres: int):
    with pg_conn.cursor() as cursor:
        with open(join(SQL_DIR, "hard_soft_dig.sql")) as schema_file:
            cursor.execute(SQL(schema_file.read()).format(
                temp_schema=Identifier(f"temp_hard_soft_dig_{job_id}"),
                model_view=Identifier(f"hard_soft_dig_job_{job_id}"),
            ), {
                'soft_ground_buffer_metres': soft_ground_buffer_metres,
                'bounds': bounds,
                'job_id': job_id,
            })
        pg_conn.commit()
