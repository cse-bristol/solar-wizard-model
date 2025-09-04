# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
"""
DEFRA LiDAR API client
"""
import logging
import os
import zipfile
from collections import defaultdict

import shapely
from os.path import join
from typing import List

import requests
from shapely import Polygon

from solar_pv.gdal_helpers import set_nodata_value
from solar_pv.geos import get_grid_cells, fill_holes, project_geom
from solar_pv.lidar.lidar import LidarTile
from solar_pv.postgis import load_lidar
from typing import TypedDict


class APIField(TypedDict):
    id: str
    label: str


class APITile(TypedDict):
    """API JSON response"""
    product: APIField
    year: APIField
    resolution: APIField
    tile: APIField
    label: str
    uri: str


def get_all_lidar(pg_conn, bounds: Polygon, lidar_dir: str) -> None:
    """
    Download LIDAR tiles unless already present.

    TODO ideally this would track year of existing tile rather than going purely
         by filename.
    """
    os.makedirs(lidar_dir, exist_ok=True)

    gridded_bounds = _get_gridded_bounds(bounds)
    job_tiles = []
    logging.info(f"Chopped boundary into {len(gridded_bounds)} chunks ")
    for poly in gridded_bounds:

        tiles = _get_tiles(poly)
        for tile in tiles:
            job_tiles.append(_download_tile(tile, lidar_dir))

    load_lidar(pg_conn, job_tiles)


def _get_gridded_bounds(bounds: Polygon) -> List[Polygon]:
    """
    Cut the job polygon into 20km by 20km squares - otherwise the defra API rejects
    the request as covering too large an area.
    """
    grid = get_grid_cells(bounds, 20000, 20000)
    gridded = []
    for cell in grid:
        p = cell.intersection(bounds)
        if p.geom_type == 'Polygon':
            p = fill_holes(p).simplify(500)
            p = project_geom(p, 27700, 4326)
            gridded.append(p)

    return gridded

# types of product and resolution we like, in order of preference:
_preferred_tiles = [
    ["lidar_composite_last_return_dsm", 1],
    ["national_lidar_programme_dsm", 1],
    ["lidar_composite_last_return_dsm", 1],
    ["lidar_composite_last_return_dsm", 2],
    ["national_lidar_programme_dsm", 2],
    ["lidar_composite_last_return_dsm", 2],
]


def _matching_tile(tiles: List[APITile], product: str, res: int):
    for tile in tiles:
        if tile["product"]["id"] == product and tile["resolution"]["id"] == str(res):
            return tile


def _get_tiles(bounds: Polygon) -> list:
    """
    Get the list of available tiles and try and find one download per tile that we like.
    _preferred_tiles above defines the products and resolutions we prefer. In case
    there is more than one tile with a given product/resolution, the latest is preferred.
    """
    url = 'https://environment.data.gov.uk/backend/catalog/api/tiles/collections/survey/search'
    res = requests.post(url, json={
        "type": "Polygon",
        "coordinates": [shapely.get_coordinates(bounds).tolist()],
    }, headers={
        "Content-Type": "application/geo+json",
        # "Host": "environment.data.gov.uk",
        # "Referer": "https://environment.data.gov.uk/survey",
        # "Origin": "https://environment.data.gov.uk",
    })
    res.raise_for_status()
    body = res.json()
    tiles: List[APITile] = body['results']

    by_tile = defaultdict(list)
    for tile in tiles:
        tile_id = tile['tile']['id']
        by_tile[tile_id].append(tile)

    to_download = []
    for tile_id, tiles in by_tile.items():
        tiles = sorted(tiles, key=lambda t: int(t["year"]["id"]), reverse=True)
        for preferred_product, preferred_res in _preferred_tiles:
            match = _matching_tile(tiles, preferred_product, preferred_res)
            if match:
                to_download.append(match)
                break

    return to_download


def _download_tile(tile: APITile, lidar_dir: str) -> LidarTile:
    """
    Download a tile, if it doesn't already exist.
    """
    fname = tile["label"] + ".zip"
    zip_path = join(lidar_dir, fname)

    if not os.path.exists(zip_path):
        logging.info(f"Downloading {tile['uri']} ...")
        res = requests.get(tile["uri"], params={"subscription-key": "public"})
        res.raise_for_status()
        with open(zip_path, 'wb') as wz:
            wz.write(res.content)
        logging.info(f"Got {tile['uri']}")
    else:
        logging.info(f"Skipped {tile['uri']} - {fname} already exists")

    return _extract_tile(tile, lidar_dir, fname)


def _extract_tile(tile: APITile, lidar_dir: str, zip_fname: str) -> LidarTile:
    """
    Doesn't extract the tile if a file already exists with the same name.
    Assumes each zipfile only has one tile in it.
    """
    zip_path = join(lidar_dir, zip_fname)
    tiff = None
    with zipfile.ZipFile(zip_path) as z:
        for zipinfo in z.infolist():
            if zipinfo.filename.split(".")[-1] in ("tif", "tiff"):
                tiff_path = join(lidar_dir, zipinfo.filename)
                tiff = LidarTile.from_filename(tiff_path, int(tile["year"]["id"]), tile["product"]["id"])
                if not os.path.exists(tiff_path):
                    z.extract(zipinfo, lidar_dir)
                    set_nodata_value(tiff_path)
                break

    if not tiff:
        raise ValueError(f"No tiff found in zip {zip_path}")
    return tiff
