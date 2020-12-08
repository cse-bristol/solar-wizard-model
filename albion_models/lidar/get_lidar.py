import json
import logging
import os
import time
import zipfile
from collections import defaultdict
from datetime import datetime
from os.path import join
from typing import List

from osgeo import gdal, osr
import requests


def get_all_lidar(pg_conn, job_id, lidar_dir: str) -> List[str]:
    with pg_conn.cursor() as cursor:
        cursor.execute(
            "SELECT "
            "ST_Xmin(ST_Envelope(geom)) as xmin, "
            "ST_Ymin(ST_Envelope(geom)) as ymin, "
            "ST_Xmax(ST_Envelope(geom)) as xmax, "
            "ST_Ymax(ST_Envelope(geom)) as ymax "
            "FROM (SELECT (ST_Dump(bounds)).geom FROM models.job_queue WHERE job_id = %(job_id)s) a ",
            {'job_id': job_id}
        )
        bboxes = cursor.fetchall()
        tiff_paths = []
        for bbox in bboxes:
            tiff_paths.extend(get_lidar(
                xmin=bbox['xmin'],
                ymin=bbox['ymin'],
                xmax=bbox['xmax'],
                ymax=bbox['ymax'],
                lidar_dir=lidar_dir,
            ))
        pg_conn.commit()
        return tiff_paths


def get_lidar(xmin: float, ymin: float, xmax: float, ymax: float, lidar_dir: str) -> List[str]:
    """
    Get Lidar data from the defra internal API.

    Bounding box coordinates should be in SRS 27700.
    """
    os.makedirs(lidar_dir, exist_ok=True)

    lidar_job_id = _start_job(xmin, ymin, xmax, ymax)
    logging.info(f"Submitted lidar job {lidar_job_id}")

    status = _wait_for_job(lidar_job_id)
    if status == 'esriJobFailed':
        raise ValueError(f"Lidar job {lidar_job_id} failed: status {status}")
    logging.info(f"Lidar job {lidar_job_id} completed with status {status}, downloading...")

    tiff_paths = _download_tiles(lidar_job_id, lidar_dir)
    logging.info(f"Lidar data for {lidar_job_id} downloaded")
    return tiff_paths


def _start_job(xmin: float, ymin: float, xmax: float, ymax: float) -> str:
    url = 'https://environment.data.gov.uk/arcgis/rest/services/gp/DataDownload/GPServer/DataDownload/submitJob'
    res = requests.get(url, params={
        "f":  "json",
        "OutputFormat": 0,
        "RequestMode":  "Survey",
        "AOI": json.dumps({
            "geometryType": "esriGeometryPolygon",
            "features": [{
                    "geometry": {
                        "rings": [[
                            [xmin, ymax],
                            [xmax, ymax],
                            [xmax, ymin],
                            [xmin, ymin],
                            [xmin, ymax]]],
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
    return body['jobId']


def _wait_for_job(lidar_job_id: str):
    while True:
        status = _check_job_status(lidar_job_id)
        if status not in ('esriJobSubmitted', 'esriJobExecuting'):
            break
        time.sleep(5)
    return status


def _check_job_status(lidar_job_id: str):
    url = f'https://environment.data.gov.uk/arcgis/rest/services/gp/DataDownload/GPServer/DataDownload/jobs/{lidar_job_id}'
    res = requests.get(url, params={
        "f": "json"
    })
    body = res.json()
    return body['jobStatus']


def _download_tiles(lidar_job_id: str, lidar_dir: str) -> List[str]:
    url = f'https://environment.data.gov.uk/arcgis/rest/directories/arcgisjobs/gp/datadownload_gpserver/{lidar_job_id}/scratch/results.json'
    res = requests.get(url)
    body = res.json()

    products = [p for p in body['data'] if p['productName'] == "LIDAR Composite DSM"]

    def year_to_key(y):
        year = y['year']
        return 9999 if year == 'Latest' else int(year)
    latest = [max(p['years'], key=lambda year: year_to_key(year)) for p in products]

    tiles_dict = defaultdict(dict)
    for la in latest:
        for resolution in la['resolutions']:
            for tile in resolution['tiles']:
                split_name = tile['tileName'].split('-')
                tile_id = split_name[-1]
                tile_res = split_name[-2]
                year = int(la['year']) if la['year'] != 'Latest' else datetime.now().year
                tiles_dict[tile_id][tile_res] = {
                    'url': tile['url'],
                    'year': year,
                }

    tiff_paths = []
    for tile_id, resolutions in tiles_dict.items():
        tile = resolutions['1M'] if '1M' in resolutions else resolutions['2M']
        tiff_paths.extend(_download_tile(tile['url'], tile['year'], lidar_dir))
    return tiff_paths


def _download_tile(url: str, year: int, lidar_dir: str) -> List[str]:
    """
    Check if the zip should be used instead of existing versions,
    extract the .asc files, and convert them to geotiffs.
    """
    zip_name = url.split('/')[-1]
    zip_id = zip_name.split('.')[0].split('-')[-1]
    zip_name_to_write = f"{year}-{zip_name}"
    zips_already_downloaded = [d for d in os.listdir(lidar_dir) if d.endswith(".zip") and zip_id in d]
    best_zip = _find_best(zips_already_downloaded + [zip_name_to_write])
    if best_zip != zip_name_to_write or zip_name_to_write in zips_already_downloaded:
        logging.info(f"Skipping download of {url}, already have {best_zip}")
    else:
        res = requests.get(url)
        with open(join(lidar_dir, zip_name_to_write), 'wb') as wz:
            wz.write(res.content)
        logging.info(f"Downloaded {url}")

    all_already_downloaded = [d for d in os.listdir(lidar_dir) if d.endswith(".tiff")]

    tiff_paths = []
    with zipfile.ZipFile(join(lidar_dir, best_zip)) as z:
        for zipinfo in z.infolist():
            z.extract(zipinfo, lidar_dir)
            # Convert to geotiff and add SRS metadata:
            tiff_filename = _asc_to_geotiff(lidar_dir, zipinfo.filename)
            tiff_paths.append(join(lidar_dir, tiff_filename))
            tile_id = zipinfo.filename.split('_')[0]

            already_downloaded = [a for a in all_already_downloaded if tile_id in a]
            for existing in already_downloaded:
                if existing != tiff_filename:
                    os.remove(join(lidar_dir, existing))
                    logging.info(f"Removing old tile {existing}")

    return tiff_paths


def _find_best(filenames: List[str], delim : str = '-') -> str:
    """
    1. Prefers 1M resolution over 2M
    2. Prefers newer over older

    So will not choose a 2M resolution even if it is newer,
    but otherwise will only choose newer versions.
    """
    def _get_file_year(filename: str) -> int:
        return int(filename.split(delim)[0])

    def _get_file_res(filename: str) -> int:
        return int(filename.split(delim)[-2][0])

    if len(filenames) == 0:
        raise ValueError("Expects arrays of length >= 1")
    return sorted(filenames, key=lambda name: (_get_file_res(name), -_get_file_year(name)))[0]


def _asc_to_geotiff(lidar_dir: str, asc_filename: str) -> str:
    """
    Convert asc file to geotiff, and add SRS metadata to file.
    """
    drv = gdal.GetDriverByName('GTiff')
    gdal_asc_file = gdal.Open(join(lidar_dir, asc_filename))
    tiff_filename = asc_filename.split('.')[0] + '.tiff'
    gdal_tiff_file = drv.CreateCopy(join(lidar_dir, tiff_filename), gdal_asc_file)
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(27700)
    gdal_tiff_file.SetProjection(srs.ExportToWkt())
    # https://gdal.org/api/python_gotchas.html
    gdal_asc_file = None
    gdal_tiff_file = None
    os.remove(join(lidar_dir, asc_filename))
    return tiff_filename
