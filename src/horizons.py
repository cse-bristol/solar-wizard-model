import subprocess

import psycopg2
from psycopg2.sql import SQL, Identifier


def get_horizons(lidar_tif: str, mask_tif: str, csv_out: str, search_radius: int, slices: int):
    res = subprocess.run(
        f'saga_cmd ta_lighting 3 '
        f'-DEM {lidar_tif} '
        f'-VISIBLE vis_out.tiff '
        f'-SVF svf_out.tiff '
        f'-CSV {csv_out} '
        f'-MASK {mask_tif} '
        f'-RADIUS {search_radius} '
        f'-NDIRS {slices} ',
        capture_output=True, text=True, shell=True
    )
    print(res.stdout)
    print(res.stderr)
    if res.returncode != 0:
        raise ValueError(res.stderr)


def load_horizons_to_db(pg_uri: str, job_id: int, horizon_csv: str):
    pg_conn = psycopg2.connect(pg_uri)
    table = f"horizons_job_{int(job_id)}"

    try:
        # todo won't work with horizon slices arg
        _sql(pg_conn, SQL("""
            CREATE TABLE models.{table} (
                x bigint,
                y bigint,
                easting double precision,
                northing double precision,
                slope double precision,
                aspect double precision,
                sky_view_factor double precision,
                percent_visible double precision,
                angle_rad_0 double precision,
                angle_rad_45 double precision,
                angle_rad_90 double precision,
                angle_rad_135 double precision,
                angle_rad_180 double precision,
                angle_rad_225 double precision,
                angle_rad_270 double precision,
                angle_rad_315 double precision
            );
        """).format(table=Identifier(table)))

        _copy_csv(pg_conn, horizon_csv, f"models.{table}")

        _sql(pg_conn, SQL("""
            ALTER TABLE models.{table} ADD COLUMN en geometry(Point, 27700);
            UPDATE models.{table} p SET en = ST_SetSRID(ST_MakePoint(p.easting,p.northing), 27700);
            CREATE INDEX ON models.{table} USING GIST (en);
        """).format(table=Identifier(table)))
    finally:
        pg_conn.close()


def _copy_csv(pg_conn, file_name: str, table: str, encoding='utf-8'):
    with pg_conn.cursor() as cursor:
        with open(file_name, encoding=encoding) as f:
            copy_sql = SQL("COPY {} FROM stdin (FORMAT 'csv', HEADER)").format(
                Identifier(*table.split(".")))
            cursor.copy_expert(copy_sql, f)
            pg_conn.commit()


def _sql(pg_conn, sql):
    with pg_conn.cursor() as cursor:
        cursor.execute(sql)
        pg_conn.commit()
