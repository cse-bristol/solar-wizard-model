import subprocess
from os.path import join

from psycopg2.sql import SQL, Identifier

import albion_models.solar_pv.tables as tables
from albion_models.db_funcs import sql_script, copy_csv, connect


def get_horizons(lidar_tif: str, solar_dir: str, mask_tif: str, csv_out: str, search_radius: int, slices: int, retrying: bool = False):
    command = f'saga_cmd ta_lighting 3 '
    f'-DEM {lidar_tif} '
    f'-VISIBLE {join(solar_dir, "vis_out.tiff")} '
    f'-SVF {join(solar_dir, "svf_out.tiff")} '
    f'-CSV {csv_out} '
    f'-MASK {mask_tif} '
    f'-RADIUS {search_radius} '
    f'-NDIRS {slices} '

    res = subprocess.run(command, capture_output=True, text=True, shell=True)
    print(res.stderr)
    if res.returncode != 0:
        # Seems like SAGA GIS very rarely crashes during cleanup due to some c++ use-after-free bug:
        if "corrupted double-linked list" in res.stderr and not retrying:
            get_horizons(lidar_tif, solar_dir, mask_tif, csv_out, search_radius, slices, True)
        else:
            raise ValueError(res.stderr)


def load_horizons_to_db(pg_uri: str, job_id: int, horizon_csv: str, horizon_slices: int):
    pg_conn = connect(pg_uri)
    schema = tables.schema(job_id)
    pixel_horizons_table = tables.PIXEL_HORIZON_TABLE
    horizon_cols = ','.join([f'horizon_slice_{i} double precision' for i in range(0, horizon_slices)])
    try:
        sql_script(
            pg_conn, 'create.pixel-horizons.sql',
            pixel_horizons=Identifier(schema, pixel_horizons_table),
            horizon_cols=SQL(horizon_cols),
        )

        copy_csv(pg_conn, horizon_csv, f"{schema}.{pixel_horizons_table}")

        sql_script(
            pg_conn, 'post-load.pixel-horizons.sql',
            pixel_horizons=Identifier(schema, pixel_horizons_table)
        )
    finally:
        pg_conn.close()
