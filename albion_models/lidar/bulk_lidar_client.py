from os.path import join

import logging
import os
from typing import List

from albion_models.db_funcs import sql_script
from albion_models.postgis import load_lidar
from albion_models.lidar.defra_lidar_api_client import get_all_lidar
from albion_models.lidar.en_to_lidar_zip_id import en_to_lidar_zip_id, \
    en_to_welsh_lidar_zip_id
from albion_models.lidar.lidar import Resolution, zip_to_geotiffs, ZippedTiles

_BULK_LIDAR_YEAR = 2017


def load_from_bulk(pg_conn, job_id: int, lidar_dir: str, bulk_lidar_dir: str) -> None:
    """
    Load LiDAR from the bulk LiDAR we have from DEFRA on bolt at `/srv/lidar`.
    """
    job_tmp_dir = join(lidar_dir, f"job_{job_id}")

    job_tiles = []
    for tiles in lidar_tiles(pg_conn, job_id, bulk_lidar_dir, lidar_dir, 'ENGLAND'):
        job_tiles.extend(tiles)
    for tiles in lidar_tiles(pg_conn, job_id, bulk_lidar_dir, lidar_dir, 'WALES'):
        job_tiles.extend(tiles)

    if len(job_tiles) == 0:
        # Fallback to LiDAR API client if no tiles found
        logging.info("No LiDAR intersecting job bounds found in bulk LiDAR, "
                     "falling back to DEFRA API")
        return get_all_lidar(pg_conn, job_id, lidar_dir)

    load_lidar(pg_conn, job_tiles, job_tmp_dir)

    logging.info(f"Prepared LiDAR")


def lidar_tiles(pg_conn, job_id: int, bulk_lidar_dir: str, lidar_dir: str, country: str):
    zip_ids = _get_zip_ids(pg_conn, job_id, country)
    for zip_id in zip_ids:
        for res in Resolution:
            zip_path = _get_zip_path(bulk_lidar_dir, zip_id, res, country)
            if os.path.exists(zip_path):
                logging.info(f"Using LiDAR zip {zip_id} at res {res.value}m")
                zt = ZippedTiles.from_filename(zip_path, _BULK_LIDAR_YEAR)
                yield zip_to_geotiffs(zt, lidar_dir)
            else:
                logging.info(f"LiDAR zip {zip_id} at res {res.value}m not "
                             f"found in bulk {country} LiDAR")


def _get_zip_ids(pg_conn, job_id: int, country: str) -> List[str]:
    """
    Get the easting and northing (in the same format that the zip filenames use:
    e.g. SV54ne) of the bottom left (SW) corner of each LiDAR zip that intersects the
    job bounds.
    """
    if country == 'ENGLAND':
        cell_size = 5000
    elif country == 'WALES':
        cell_size = 10000
    else:
        raise ValueError(f"Unsupported country {country}")

    def extract(row):
        if country == 'ENGLAND':
            return en_to_lidar_zip_id(row[0], row[1])
        elif country == 'WALES':
            return en_to_welsh_lidar_zip_id(row[0], row[1])
        else:
            raise ValueError(f"Unsupported country {country}")

    return sql_script(
        pg_conn, 'lidar_tiles_needed.sql',
        {'job_id': job_id,
         'cell_size': cell_size},
        result_extractor=lambda res: [extract(row) for row in res])


def _get_zip_path(bulk_lidar_dir: str, zip_id: str, res: Resolution, country: str) -> str:
    res_str = res.name[2:]
    if country == 'ENGLAND':
        return join(
            bulk_lidar_dir,
            f"LIDAR-DSM-{res_str}-ENGLAND-EA",
            f"LIDAR-DSM-{res_str}-{zip_id}.zip")
    elif country == 'WALES':
        return join(
            bulk_lidar_dir,
            f"wales",
            f"{res_str.lower()}_res_{zip_id}_dsm.zip")
    else:
        raise ValueError(f"Unsupported country {country}")
