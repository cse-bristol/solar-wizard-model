"""
DEFRA LiDAR API client
"""
import json
import logging
import os
import time
from collections import defaultdict
from datetime import datetime
from os.path import join
from typing import List, Dict

import requests
from psycopg2.sql import SQL, Identifier

from albion_models.lidar.lidar import ZippedTiles, LidarTile, LidarJobTiles, LIDAR_VRT, \
    LIDAR_COV_VRT, zip_to_geotiffs
from albion_models.paths import SQL_DIR

_DEFRA_API = "https://environment.data.gov.uk/arcgis/rest"


def get_all_lidar(pg_conn, job_id: int, lidar_dir: str) -> str:
    """
    Download LIDAR tiles unless already present, or if newer/better resolution
    than those already downloaded.
    """
    job_lidar_dir = join(lidar_dir, f"job_{job_id}")
    job_lidar_vrt = join(job_lidar_dir, LIDAR_VRT)
    coverage_vrt = join(job_lidar_dir, LIDAR_COV_VRT)

    if os.path.exists(job_lidar_vrt) and os.path.exists(coverage_vrt):
        logging.info("LiDAR .vrts exist, using files referenced")
        return job_lidar_vrt

    gridded_bounds = _get_gridded_bounds(pg_conn, job_id)
    job_tiles = LidarJobTiles()
    logging.info(f"{len(gridded_bounds)} LiDAR jobs to run")
    for rings in gridded_bounds:
        job_tiles.merge(_get_lidar(pg_conn, job_id=job_id, rings=rings, lidar_dir=lidar_dir))

    job_tiles.create_merged_vrt(job_lidar_dir, job_lidar_vrt, coverage_vrt)
    job_tiles.delete_unmerged_tiles()
    logging.info(f"Created LiDAR vrt {job_lidar_vrt}")
    return job_lidar_vrt


def _get_gridded_bounds(pg_conn, job_id: int) -> List[List[List[float]]]:
    """
    Cut the job polygon into 20km by 20km squares - otherwise the defra API rejects
    the request as covering too large an area.

    Also takes the convex hull of the polygon to simplify the request.
    """
    with pg_conn.cursor() as cursor:
        with open(join(SQL_DIR, 'grid-for-lidar.sql')) as schema_file:
            cursor.execute(SQL(schema_file.read()).format(
                grid_table=Identifier(f'lidar_grid_{job_id}')), {'job_id': job_id})
            rows = cursor.fetchall()
            pg_conn.commit()
            return [_wkt_to_rings(row[0]) for row in rows]


def _wkt_to_rings(wkt: str) -> List[List[float]]:
    if wkt.startswith("POLYGON"):
        pairs = wkt.replace("POLYGON", "").replace("(", "").replace(")", "").split(",")
        rings = []
        for pair in pairs:
            split = pair.split()
            rings.append([float(split[0].strip()), float(split[1].strip())])
        return rings
    else:
        logging.warning(f"LiDAR area was not a polygon. Occasional points and "
                        f"linestrings might be possible results of intersecting "
                        f"the grid with the bounding polygon: {wkt}")
        return []


def _get_lidar(pg_conn, job_id: int, rings: List[List[float]], lidar_dir: str) -> LidarJobTiles:
    """
    Get Lidar data from the defra internal API.

    Bounding box coordinates should be in SRS 27700.
    """
    os.makedirs(lidar_dir, exist_ok=True)

    lidar_job_id = _start_job(rings)
    logging.info(f"Submitted lidar job {lidar_job_id}")

    status = _wait_for_job(lidar_job_id)
    if status == 'esriJobFailed':
        raise ValueError(f"Lidar job {lidar_job_id} failed: status {status}")
    logging.info(f"LiDAR job {lidar_job_id} completed with status {status}, downloading...")

    job_tiles = _download_tiles(pg_conn, job_id, lidar_job_id, lidar_dir)
    logging.info(f"LiDAR data for {lidar_job_id} downloaded")
    return job_tiles


def _start_job(rings: List[List[float]]) -> str:
    url = f'{_DEFRA_API}/services/gp/DataDownload/GPServer/DataDownload/submitJob'
    res = requests.get(url, params={
        "f":  "json",
        "OutputFormat": 0,
        "RequestMode":  "Survey",
        "AOI": json.dumps({
            "geometryType": "esriGeometryPolygon",
            "features": [{
                    "geometry": {
                        "rings": [rings],
                        "spatialReference": {
                            "wkid": 27700,
                            "latestWkid": 27700
                        }
                    }
                }],
            "sr": {
                "wkid": 27700,
                "latestWkid": 27700
            }
        }),
    })
    res.raise_for_status()
    body = res.json()
    if 'jobId' in body:
        return body['jobId']
    else:
        raise ValueError(f"Received unhandled response while submitting LiDAR job: {body}")


def _wait_for_job(lidar_job_id: str) -> str:
    while True:
        status = _check_job_status(lidar_job_id)
        if status not in ('esriJobSubmitted', 'esriJobExecuting'):
            break
        time.sleep(5)
    return status


def _check_job_status(lidar_job_id: str) -> str:
    url = f'{_DEFRA_API}/services/gp/DataDownload/GPServer/DataDownload/jobs/{lidar_job_id}'
    res = requests.get(url, params={"f": "json"})
    res.raise_for_status()
    body = res.json()
    if 'jobStatus' in body:
        return body['jobStatus']
    else:
        raise ValueError(f"Received unhandled response while checking LiDAR job status: {body}")


def _download_tiles(pg_conn, job_id: int, lidar_job_id: str, lidar_dir: str) -> LidarJobTiles:
    url = f'{_DEFRA_API}/directories/arcgisjobs/gp/datadownload_gpserver/{lidar_job_id}/scratch/results.json'
    res = requests.get(url)
    res.raise_for_status()
    body = res.json()

    products = [p for p in body['data'] if p['productName'] == "LIDAR Composite DSM"]

    def year_to_key(y):
        year = y['year']
        return 9999 if year == 'Latest' else int(year)
    latest = [max(p['years'], key=lambda year: year_to_key(year)) for p in products]

    all_files = os.listdir(lidar_dir)

    existing_zips: Dict[str, List[ZippedTiles]] = defaultdict(list)
    for f in all_files:
        if f.endswith(".zip"):
            zt = ZippedTiles.from_filename(f)
            existing_zips[zt.zip_id].append(zt)

    job_tiles = LidarJobTiles()
    for la in latest:
        for resolution in la['resolutions']:
            for tile in resolution['tiles']:
                year = int(la['year']) if la['year'] != 'Latest' else datetime.now().year
                url = tile['url']
                zt = ZippedTiles.from_url(url, year)
                if zt:
                    job_tiles.add_tiles(_download_zip(pg_conn, job_id, zt, lidar_dir, existing_zips[zt.zip_id]))

    return job_tiles


def _download_zip(pg_conn,
                  job_id: int,
                  zt: ZippedTiles,
                  lidar_dir: str,
                  existing_zips: List[ZippedTiles]) -> List[LidarTile]:
    """
    Check if the zip should be used instead of existing versions,
    extract the .asc files, and convert them to geotiffs.
    """
    zips_already_downloaded = [z for z in existing_zips if z.resolution == zt.resolution]
    best_zip = sorted(zips_already_downloaded + [zt], key=lambda zzt: -zzt.year)[0]

    if best_zip != zt or zt in zips_already_downloaded:
        logging.info(f"Skipping download of {zt.url}, already have {best_zip.filename}")
    else:
        res = requests.get(zt.url)
        res.raise_for_status()
        with open(join(lidar_dir, zt.filename), 'wb') as wz:
            wz.write(res.content)
        logging.info(f"Downloaded {zt.url}")

    tiff_paths = zip_to_geotiffs(pg_conn, job_id, best_zip, lidar_dir)

    return tiff_paths
