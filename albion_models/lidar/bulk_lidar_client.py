import logging
import os
from os.path import join
from typing import List

from albion_models import paths
from albion_models.lidar.defra_lidar_api_client import get_all_lidar
from albion_models.lidar.en_to_lidar_zip_id import en_to_lidar_zip_id
from albion_models.lidar.lidar import LidarJobTiles, LIDAR_VRT, LIDAR_COV_VRT, \
    Resolution, zip_to_geotiffs, ZippedTiles

_BULK_LIDAR_YEAR = 2017


def load_from_bulk(pg_conn, job_id: int, lidar_dir: str, bulk_lidar_dir: str) -> str:
    """
    Load LiDAR from the bulk LiDAR we have from DEFRA on bolt at `/srv/lidar`.
    """
    job_lidar_dir = join(lidar_dir, f"job_{job_id}")
    job_lidar_vrt = join(job_lidar_dir, LIDAR_VRT)
    coverage_vrt = join(job_lidar_dir, LIDAR_COV_VRT)

    if os.path.exists(job_lidar_vrt) and os.path.exists(coverage_vrt):
        logging.info("LiDAR .vrts exist, using files referenced")
        return job_lidar_vrt

    job_tiles = LidarJobTiles()
    zip_ids = _get_zip_ids(pg_conn, job_id)
    for zip_id in zip_ids:
        for res in Resolution:
            zip_path = _get_zip_path(bulk_lidar_dir, zip_id, res)
            if os.path.exists(zip_path):
                logging.info(f"Using LiDAR zip {zip_id} at res {res.value}m")
                zt = ZippedTiles.from_filename(zip_path, _BULK_LIDAR_YEAR)
                job_tiles.add_tiles(zip_to_geotiffs(pg_conn, job_id, zt, job_lidar_dir))
            else:
                logging.info(f"LiDAR zip {zip_id} at res {res.value}m not "
                             f"found in bulk LiDAR")

    if len(job_tiles.all_filenames()) == 0:
        # Fallback to LiDAR API client if no tiles found
        logging.info("No LiDAR intersecting job bounds found in bulk LiDAR, "
                     "falling back to DEFRA API")
        return get_all_lidar(pg_conn, job_id, lidar_dir)

    job_tiles.create_merged_vrt(job_lidar_dir, job_lidar_vrt, coverage_vrt)
    job_tiles.delete_unmerged_tiles()
    logging.info(f"Created LiDAR vrt {job_lidar_vrt}")
    return job_lidar_vrt


def _get_zip_ids(pg_conn, job_id: int) -> List[str]:
    """
    Get the easting and northing (in the same format that the zip filenames use:
    e.g. SV54ne) of the bottom left corner of each LiDAR zip that intersects the
    job bounds.
    """
    with pg_conn.cursor() as cursor:
        with open(join(paths.SQL_DIR, 'lidar_tiles_needed.sql')) as schema_file:
            cursor.execute(schema_file.read(), {'job_id': job_id})
            rows = cursor.fetchall()
            pg_conn.commit()
            return [en_to_lidar_zip_id(row[0], row[1]) for row in rows]


def _get_zip_path(bulk_lidar_dir: str, zip_id: str, res: Resolution) -> str:
    res_str = res.name[2:]
    return join(
        bulk_lidar_dir,
        f"LIDAR-DSM-{res_str}-ENGLAND-EA",
        f"LIDAR-DSM-{res_str}-{zip_id}.zip")
