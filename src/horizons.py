import subprocess

import psycopg2
from psycopg2.sql import SQL, Identifier

import tables


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
    print(res.stderr)
    if res.returncode != 0:
        raise ValueError(res.stderr)


def load_horizons_to_db(pg_uri: str, job_id: int, horizon_csv: str, horizon_slices: int):
    pg_conn = psycopg2.connect(pg_uri)
    schema = tables.schema(job_id)
    pixel_horizons_table = tables.PIXEL_HORIZON_TABLE
    horizon_cols = ','.join([f'horizon_slice_{i} double precision' for i in range(0, horizon_slices)])
    try:
        _sql(pg_conn, SQL("""
            CREATE TABLE {pixel_horizons} (
                x bigint,
                y bigint,
                easting double precision,
                northing double precision,
                slope double precision,
                aspect double precision,
                sky_view_factor double precision,
                percent_visible double precision,
        """ + horizon_cols + ");").format(pixel_horizons=Identifier(schema, pixel_horizons_table)))

        _copy_csv(pg_conn, horizon_csv, f"{schema}.{pixel_horizons_table}")

        _sql(pg_conn, SQL("""
            ALTER TABLE {pixel_horizons} ADD COLUMN en geometry(Point, 27700);
            UPDATE {pixel_horizons} p SET en = ST_SetSRID(ST_MakePoint(p.easting,p.northing), 27700);
            CREATE INDEX ON {pixel_horizons} USING GIST (en);
        """).format(pixel_horizons=Identifier(schema, pixel_horizons_table)))
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
