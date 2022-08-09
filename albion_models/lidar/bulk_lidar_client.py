import enum
import logging
import os
from os.path import join

import gdal

from albion_models import gdal_helpers
from albion_models.geos import bounds_polygon, get_grid_refs
from albion_models.lidar.defra_lidar_api_client import get_all_lidar
from albion_models.lidar.lidar import Resolution, zip_to_geotiffs, ZippedTiles, \
    LidarTile, _file_res
from albion_models.postgis import load_lidar


class LidarSource(enum.Enum):

    ENGLAND = ("ENGLAND", 5000, 2017)
    WALES = ("WALES", 10000, 2015)
    SCOTLAND_1 = ("SCOTLAND_1", 10000, 2009, False)
    SCOTLAND_2 = ("SCOTLAND_2", 10000, 2009, False)
    SCOTLAND_3 = ("SCOTLAND_3", 5000, 2016, False)
    SCOTLAND_4 = ("SCOTLAND_4", 5000, 2019, False)
    SCOTLAND_5 = ("SCOTLAND_5", 5000, 2021, False)

    def __init__(self, country: str, cell_size: int, year: int, zipped: bool = True):
        self.country = country
        self.cell_size = cell_size
        self.year = year
        self.zipped = zipped

    def filepath(self, bulk_lidar_dir: str, grid_ref: str, res: Resolution):
        res_str = res.name[2:]
        if self == LidarSource.ENGLAND:
            return join(bulk_lidar_dir, f"LIDAR-DSM-{res_str}-ENGLAND-EA", f"LIDAR-DSM-{res_str}-{grid_ref}.zip")
        elif self == LidarSource.WALES:
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


def load_from_bulk(pg_conn, job_id: int, lidar_dir: str, bulk_lidar_dir: str) -> None:
    """
    Load LiDAR from the bulk LiDAR we have from DEFRA on bolt at `/srv/lidar`.
    """
    job_tmp_dir = join(lidar_dir, f"job_{job_id}")

    job_tiles = []
    for source in LidarSource:
        for tiles in lidar_tiles(pg_conn, job_id, bulk_lidar_dir, lidar_dir, source):
            job_tiles.extend(tiles)

    if len(job_tiles) == 0:
        # Fallback to LiDAR API client if no tiles found
        logging.info("No LiDAR intersecting job bounds found in bulk LiDAR, "
                     "falling back to DEFRA API")
        return get_all_lidar(pg_conn, job_id, lidar_dir)

    load_lidar(pg_conn, job_tiles, job_tmp_dir)

    logging.info(f"Prepared LiDAR")


def lidar_tiles(pg_conn, job_id: int, bulk_lidar_dir: str, lidar_dir: str, source: LidarSource):
    bounds_poly = bounds_polygon(pg_conn, job_id)
    grid_refs = get_grid_refs(bounds_poly, source.cell_size)

    for grid_ref in grid_refs:
        for res in Resolution:
            filepath = source.filepath(bulk_lidar_dir, grid_ref, res)
            if os.path.exists(filepath):
                logging.info(f"Using LiDAR zip {grid_ref} at res {res.value}m "
                             f"from bulk LiDAR source {source}")
                if source.zipped:
                    zt = ZippedTiles.from_filename(filepath, source.year)
                    yield zip_to_geotiffs(zt, lidar_dir)
                else:
                    _fix_lidar_res(filepath)
                    yield [LidarTile.from_filename(filepath, source.year)]


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
                  xRes=filename_res, yRes=filename_res)
