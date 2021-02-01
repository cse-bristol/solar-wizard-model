import logging
import subprocess
from os.path import join
from typing import List

from psycopg2.sql import SQL, Identifier

import albion_models.solar_pv.tables as tables
from albion_models.db_funcs import sql_script, copy_csv, connect, count
from albion_models.solar_pv import mask, gdal_helpers


def find_horizons(pg_uri: str,
                  job_id: int,
                  solar_dir: str,
                  lidar_paths: List[str],
                  horizon_search_radius: int,
                  horizon_slices: int,
                  masking_strategy: str) -> None:
    """
    Detect the horizon height in degrees for each `horizon_slices` slice of the
    compass for each LIDAR pixel that falls inside one of the OS MasterMap building
    polygons.
    """
    if count(pg_uri, tables.schema(job_id), tables.PIXEL_HORIZON_TABLE) > 0:
        logging.info("Not detecting horizon, horizon data already loaded.")
        return

    vrt_file = join(solar_dir, 'tiles.vrt')
    gdal_helpers.create_vrt(lidar_paths, vrt_file)

    logging.info("Creating raster mask from mastermap.buildings polygon...")
    if masking_strategy == 'building':
        mask_file = mask.create_buildings_mask(job_id, solar_dir, pg_uri, resolution_metres=1)
    elif masking_strategy == 'bounds':
        mask_file = mask.create_bounds_mask(job_id, solar_dir, pg_uri, resolution_metres=1)
    else:
        raise ValueError(f"Unknown masking strategy {masking_strategy}")

    logging.info("Cropping lidar to mask dimensions...")
    gdal_helpers.crop_or_expand(mask_file, vrt_file, mask_file, adjust_resolution=False)

    cropped_lidar = join(solar_dir, 'cropped_lidar.tif')
    gdal_helpers.crop_or_expand(vrt_file, mask_file, cropped_lidar, adjust_resolution=True)

    logging.info("Using 320-albion-saga-gis to find horizons...")
    horizons_csv = join(solar_dir, 'horizons.csv')
    _get_horizons(cropped_lidar, solar_dir, mask_file, horizons_csv, horizon_search_radius, horizon_slices)
    _load_horizons_to_db(pg_uri, job_id, horizons_csv, horizon_slices)


def _get_horizons(lidar_tif: str, solar_dir: str, mask_tif: str, csv_out: str, search_radius: int, slices: int, retrying: bool = False):
    command = f'saga_cmd ta_lighting 3 ' \
              f'-DEM {lidar_tif} ' \
              f'-VISIBLE {join(solar_dir, "vis_out.tiff")} ' \
              f'-SVF {join(solar_dir, "svf_out.tiff")} ' \
              f'-CSV {csv_out} ' \
              f'-MASK {mask_tif} ' \
              f'-RADIUS {search_radius} ' \
              f'-NDIRS {slices} '

    res = subprocess.run(command, capture_output=True, text=True, shell=True)
    print(res.stdout)
    print(res.stderr)
    if res.returncode != 0:
        # Seems like SAGA GIS very rarely crashes during cleanup due to some c++ use-after-free bug:
        if "corrupted double-linked list" in res.stderr and not retrying:
            _get_horizons(lidar_tif, solar_dir, mask_tif, csv_out, search_radius, slices, True)
        else:
            raise ValueError(res.stderr)


def _load_horizons_to_db(pg_uri: str, job_id: int, horizon_csv: str, horizon_slices: int):
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
