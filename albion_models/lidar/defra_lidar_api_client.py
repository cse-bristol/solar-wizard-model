"""
DEFRA LiDAR API client
"""
import json
import logging
import os
import time
from datetime import datetime
from os.path import join
from typing import List

import requests
from psycopg2.sql import SQL, Identifier

from albion_models.postgis import load_lidar
from albion_models.lidar.lidar import ZippedTiles, LidarTile, zip_to_geotiffs
from albion_models.paths import SQL_DIR

_DEFRA_API = "https://environment.data.gov.uk/arcgis/rest"


def get_all_lidar(pg_conn, job_id: int, lidar_dir: str) -> None:
    """
    Download LIDAR tiles unless already present, or if newer/better resolution
    than those already downloaded.
    """
    job_tmp_dir = join(lidar_dir, f"job_{job_id}")

    gridded_bounds = _get_gridded_bounds(pg_conn, job_id)
    job_tiles = []
    logging.info(f"{len(gridded_bounds)} LiDAR jobs to run")
    for rings in gridded_bounds:
        job_tiles.extend(_get_lidar(rings=rings, lidar_dir=lidar_dir))

    load_lidar(pg_conn, job_tiles, job_tmp_dir)

    logging.info("Downloaded LiDAR")


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


def _get_lidar(rings: List[List[float]], lidar_dir: str) -> List[LidarTile]:
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

    job_tiles = _download_tiles(lidar_job_id, lidar_dir)
    logging.info(f"LiDAR data for {lidar_job_id} downloaded")
    return job_tiles


def _start_job(rings: List[List[float]]) -> str:
    url = f'{_DEFRA_API}/services/gp/DataDownload/GPServer/DataDownload/submitJob'
    res = requests.get(url, params={
        "f": "json",
        "OutputFormat": 0,
        "RequestMode": "Survey",
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


def _download_tiles(lidar_job_id: str, lidar_dir: str) -> List[LidarTile]:
    url = f'{_DEFRA_API}/directories/arcgisjobs/gp/datadownload_gpserver/{lidar_job_id}/scratch/results.json'
    res = requests.get(url)
    res.raise_for_status()
    body = res.json()

    products = [p for p in body['data'] if p['productName'] == "LIDAR Composite DSM"]

    def year_to_key(y):
        year = y['year']
        return 9999 if year == 'Latest' else int(year)
    latest = [max(p['years'], key=lambda year: year_to_key(year)) for p in products]

    job_tiles = []
    for la in latest:
        for resolution in la['resolutions']:
            for tile in resolution['tiles']:
                year = int(la['year']) if la['year'] != 'Latest' else datetime.now().year
                url = tile['url']
                zt = ZippedTiles.from_url(url, year)
                if zt:
                    job_tiles.extend(_download_zip(zt, lidar_dir))

    return job_tiles


def _download_zip(zt: ZippedTiles, lidar_dir: str) -> List[LidarTile]:
    """
    Check if the zip should be used instead of existing versions,
    extract the .asc files, and convert them to geotiffs.
    """
    res = requests.get(zt.url)
    res.raise_for_status()
    zip_path = join(lidar_dir, zt.filename)
    with open(zip_path, 'wb') as wz:
        wz.write(res.content)
    logging.info(f"Downloaded {zt.url}")

    tiff_paths = zip_to_geotiffs(zt, lidar_dir)

    try:
        os.remove(zip_path)
    except OSError:
        pass

    return tiff_paths
