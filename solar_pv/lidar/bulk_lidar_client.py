# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import enum
import logging
import os
import shutil
from os.path import join
from typing import List

from osgeo import gdal
from shapely import Polygon

from solar_pv import gdal_helpers
from solar_pv.geos import get_grid_refs
from solar_pv.lidar.lidar import Resolution, zip_to_geotiffs, ZippedTiles, \
    LidarTile, _file_res
from solar_pv.postgis import load_lidar


class LidarSource(enum.Enum):

    ENGLAND = ("ENGLAND", 5000, 2022, "lidar_composite_last_return_dsm")
    WALES = ("WALES", 10000, 2015, 'Wales LidarCompositeDataset (2015)')
    # Scottish LiDAR is in 5 separate phases, sometimes overlapping in tiles
    # covered (but with different portions of the tile containing data, so we
    # do still need the earlier phases):
    SCOTLAND_1 = ("SCOTLAND_1", 10000, 2012, 'LiDAR for Scotland Phase I DSM', False, [Resolution.R_1M])
    SCOTLAND_2 = ("SCOTLAND_2", 10000, 2014, 'LiDAR for Scotland Phase II DSM', False, [Resolution.R_1M])
    SCOTLAND_3 = ("SCOTLAND_3", 5000, 2016, 'LiDAR for Scotland Phase III DSM', False, [Resolution.R_50CM])
    SCOTLAND_4 = ("SCOTLAND_4", 5000, 2019, 'LiDAR for Scotland Phase IV DSM', False, [Resolution.R_50CM])
    SCOTLAND_5 = ("SCOTLAND_5", 5000, 2021, 'LiDAR for Scotland Phase V DSM', False, [Resolution.R_50CM])

    def __init__(self, country: str, cell_size: int, year: int, product: str,
                 zipped: bool = True,
                 resolutions: List[Resolution] = None):
        if resolutions is None:
            resolutions = [Resolution.R_50CM, Resolution.R_1M, Resolution.R_2M]

        self.country = country
        self.cell_size = cell_size
        self.year = year
        self.product = product
        self.zipped = zipped
        self.resolutions = resolutions

    def filepath(self, bulk_lidar_dir: str, grid_ref: str, res: Resolution):
        res_str = res.name[2:]
        if self == LidarSource.ENGLAND:
            return join(bulk_lidar_dir, "england", f"LIDAR-LZ_DSM-{res_str}-2022-{grid_ref}.zip")
        if self == LidarSource.WALES:
            return join(bulk_lidar_dir, "wales", f"{res_str.lower()}_res_{grid_ref}_dsm.zip")
        elif self == LidarSource.SCOTLAND_1:
            return join(bulk_lidar_dir, "scotland", f"{grid_ref.upper()}_{res_str}_DSM_PHASE1.tif")
        elif self == LidarSource.SCOTLAND_2:
            return join(bulk_lidar_dir, "scotland", f"{grid_ref.upper()}_{res_str}_DSM_PHASE2.tif")
        elif self == LidarSource.SCOTLAND_3:
            return join(bulk_lidar_dir, "scotland", f"{grid_ref.upper()}_{res_str}_DSM_PHASE3.tif")
        elif self == LidarSource.SCOTLAND_4:
            return join(bulk_lidar_dir, "scotland", f"{grid_ref.upper()}_{res_str}_DSM_PHASE4.tif")
        elif self == LidarSource.SCOTLAND_5:
            return join(bulk_lidar_dir, "scotland", f"{grid_ref.upper()}_{res_str}_DSM_PHASE5.tif")
        else:
            raise ValueError(f"Unsupported Lidar source {self}")


def load_from_bulk(pg_conn, bounds_poly: Polygon, lidar_dir: str, bulk_lidar_dir: str) -> None:
    """
    Load LiDAR from bulk.
    """

    job_tiles = []
    for source in LidarSource:
        for tiles in lidar_tiles(bounds_poly, bulk_lidar_dir, lidar_dir, source):
            job_tiles.extend(tiles)

    for tile in job_tiles:
        if not tile.filename.startswith(lidar_dir):
            raise ValueError(f"LiDAR tiles must be in {lidar_dir} as otherwise they "
                             f"are not available to postGIS")

    load_lidar(pg_conn, job_tiles)

    logging.info(f"Prepared LiDAR")


def lidar_tiles(bounds_poly: Polygon, bulk_lidar_dir: str, lidar_dir: str, source: LidarSource):
    grid_refs = get_grid_refs(bounds_poly, source.cell_size)

    for grid_ref in grid_refs:
        for res in source.resolutions:
            filepath = source.filepath(bulk_lidar_dir, grid_ref, res)
            if os.path.exists(filepath):
                logging.info(f"Using LiDAR {'zip' if source.zipped else 'tile'} {filepath} "
                             f"from bulk LiDAR source {source}")
                if source.zipped:
                    zt = ZippedTiles.from_filename(filepath, source.year, source.product)
                    yield zip_to_geotiffs(zt, lidar_dir)
                else:
                    dst_filepath = join(lidar_dir, os.path.basename(filepath))
                    _fix_lidar_res(filepath)
                    shutil.copyfile(filepath, dst_filepath)
                    yield [LidarTile.from_filename(dst_filepath, source.year, source.product)]


def _fix_lidar_res(filepath: str):
    """
    Scottish phase 1 LiDAR resolution is something like 1.000002, not 1. postGIS
    doesn't like that.
    """
    curr_res = gdal_helpers.get_res_unchecked(filepath)
    filename = os.path.basename(filepath)
    filename_res = _file_res(filename).value

    if curr_res != filename_res:
        logging.info(f"LiDAR res was {curr_res}, but filename says {filename_res} - fixing")
        gdal.UseExceptions()
        in_f = gdal.Open(filepath)

        ulx, xres, xskew, uly, yskew, yres = in_f.GetGeoTransform()

        xres = -filename_res if xres < 0 else filename_res
        yres = -filename_res if yres < 0 else filename_res
        ulx = int(ulx)
        uly = int(uly)
        lrx = ulx + int(in_f.RasterXSize * xres)
        lry = uly + int(in_f.RasterYSize * yres)

        gdal.Warp(filepath, filepath,
                  outputBounds=(ulx, lry, lrx, uly),
                  xRes=filename_res, yRes=filename_res,
                  creationOptions=['TILED=YES', 'COMPRESS=PACKBITS'])
