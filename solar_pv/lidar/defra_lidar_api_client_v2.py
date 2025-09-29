# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
"""
DEFRA LiDAR API client v2.

Simpler than v1 and avoids having to use the search functionality, which seems to
often return 500 errors. But only supports trying to get one product/year/resolution
combination.
"""
import logging
import os
import zipfile

import shapely
from os.path import join
from typing import List

import requests
from shapely import Polygon

from solar_pv import paths
from solar_pv.gdal_helpers import set_nodata_value
from solar_pv.geos import get_grid_cells, fill_holes, project_geom, get_grid_refs
from solar_pv.lidar.en_to_grid_ref import en_to_grid_ref
from solar_pv.lidar.grid_ref import os_grid_ref_to_en
from solar_pv.lidar.lidar import LidarTile, Resolution
from solar_pv.postgis import load_lidar


_PRODUCT = "lidar_composite_last_return_dsm"
_YEAR = 2022
_RES = 1


class EmptyZipError(Exception):
    pass


def get_all_lidar(pg_conn, bounds: Polygon, lidar_dir: str) -> None:
    """
    Download LIDAR tiles unless already present.
    """
    with open(join(paths.RESOURCES_DIR, "england.wkt")) as wkt:
        england = shapely.from_wkt(wkt.read())

    if not bounds.intersects(england):
        logging.info("Skipping DEFRA API client, bounds do not intersect England")
        return

    os.makedirs(lidar_dir, exist_ok=True)

    gridded_bounds = _get_gridded_bounds(bounds)
    job_tiles = []
    logging.info(f"Chopped boundary into {len(gridded_bounds)} chunks ")
    for poly in gridded_bounds:
        job_tiles.extend(_download_tiles(poly, lidar_dir))

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
            gridded.append(p)

    return gridded


def _get_url(product: str, year: int, res: int, grid_ref: str) -> str:
    return f"https://api.agrimetrics.co.uk/tiles/collections/survey/{product}/{year}/{res}/{grid_ref}"


def _get_fname(product: str, year: int, res: int, grid_ref: str) -> str:
    return f"{product}-{year}-{res}m-{grid_ref}.zip"


def _download_tiles(bounds: Polygon, lidar_dir: str) -> List[LidarTile]:
    """
    Get the list of available tiles and try and find one download per tile that we like.
    _preferred_tiles above defines the products and resolutions we prefer. In case
    there is more than one tile with a given product/resolution, the latest is preferred.
    """
    downloaded = []
    grid_refs = get_grid_refs(bounds, cell_size=5000, grid_size=1000)
    for grid_ref in grid_refs:
        try:
            tile = _download_tile(lidar_dir, grid_ref)
            downloaded.append(tile)
        except EmptyZipError:
            pass

    return downloaded


def _download_tile(lidar_dir: str, grid_ref: str) -> LidarTile:
    """
    Download a tile, if it doesn't already exist.
    """
    uri = _get_url(_PRODUCT, _YEAR, _RES, grid_ref)
    e, n, _ = os_grid_ref_to_en(grid_ref)
    # existing file names use the 5k grid refs, but the URLs use 1k grid refs (despite covering 5k squares...)
    grid_ref_5k = en_to_grid_ref(e, n, 5000)
    fname = _get_fname(_PRODUCT, _YEAR, _RES, grid_ref_5k)
    zip_path = join(lidar_dir, fname)

    if not os.path.exists(zip_path):
        logging.info(f"Downloading {uri} ...")
        res = requests.get(uri, params={"subscription-key": "public"})
        res.raise_for_status()
        with open(zip_path, 'wb') as wz:
            wz.write(res.content)
        logging.info(f"Got {uri}")
    else:
        logging.info(f"Skipped {uri} - {fname} already exists")

    return _extract_tile(lidar_dir, fname, grid_ref_5k)


def _extract_tile(lidar_dir: str, zip_fname: str, grid_ref: str) -> LidarTile:
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
                tiff = LidarTile(
                    tile_id=grid_ref,
                    year=_YEAR,
                    resolution=Resolution(float(_RES)),
                    filename=tiff_path,
                    product=_PRODUCT)
                if not os.path.exists(tiff_path):
                    z.extract(zipinfo, lidar_dir)
                    set_nodata_value(tiff_path)
                break

    if not tiff:
        raise EmptyZipError(f"No tiff found in zip {zip_path}")
    return tiff
