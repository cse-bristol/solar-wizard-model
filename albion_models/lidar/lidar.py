from dataclasses import dataclass
from enum import Enum
from os.path import join

import logging
import os
import re
import zipfile
from osgeo import gdal, osr
from typing import Optional, List

from albion_models import gdal_helpers

LIDAR_NODATA = -9999
"""
NODATA value used in LiDAR tiffs
"""

USE_50CM_THRESHOLD = 0.25
"""
At least this % of the job area must be covered by 50cm LiDAR to
start using it. Otherwise only 1m and 2m will be used, as having to work
at 50cm resolution slows lots of things down (RANSAC, horizon detection,
mapping roof planes -> pv installations).
"""

LIDAR_VRT = "tiles.vrt"
LIDAR_COV_VRT = "per_res_coverage.vrt"


class Resolution(Enum):
    """
    The LiDAR resolutions used in Albion
    """
    R_50CM = 0.5
    R_1M = 1.0
    R_2M = 2.0

    @classmethod
    def from_string(cls, string: str) -> Optional['Resolution']:
        string = string.upper()
        if string == '50CM':
            return Resolution.R_50CM
        elif string == '1M':
            return Resolution.R_1M
        elif string == '2M':
            return Resolution.R_2M
        else:
            return None


@dataclass
class ZippedTiles:
    """
    Represents a zip of LiDAR rasters from the DEFRA API or on disk.
    """
    zip_id: str
    year: int
    resolution: Resolution
    url: Optional[str]
    filename: str

    @classmethod
    def from_url(cls, url: str, year: int):
        zip_name = url.split('/')[-1]
        zip_id = _zipfile_id(zip_name)
        filename = f"{year}-{zip_name}"
        resolution = _file_res(filename)
        if zip_id is None:
            raise ValueError(f"Could not read zip ID from file: {filename}")
        if resolution is None:
            # A resolution we don't care about: ignore
            return None

        return ZippedTiles(
            zip_id=zip_id,
            year=year,
            resolution=resolution,
            url=url,
            filename=filename)

    @classmethod
    def from_filename(cls, filename: str, year: int = None):
        basename = os.path.basename(filename)
        zip_id = _zipfile_id(basename)
        resolution = _file_res(basename)
        year = year or _zip_year(basename)
        if zip_id is None:
            raise ValueError(f"Could not read zip ID from file: {basename}")
        if resolution is None:
            raise ValueError(f"Could not read resolution from file: {basename}")
        if year is None:
            raise ValueError(f"Could not read year from file: {basename}")
        return ZippedTiles(
            zip_id=zip_id,
            year=year,
            resolution=resolution,
            url=None,
            filename=filename)


@dataclass
class LidarTile:
    """
    Represents a LiDAR .tiff raster on disk.
    """
    tile_id: str
    year: int
    resolution: Resolution
    filename: str

    @classmethod
    def from_filename(cls, filename: str, year: int):
        basename = os.path.basename(filename)
        tile_id = _tile_id(basename)
        resolution = _file_res(basename)
        if tile_id is None:
            raise ValueError(f"Could not read tile ID from file: {basename}")
        if resolution is None:
            raise ValueError(f"Could not read resolution from file: {basename}")
        return LidarTile(
            tile_id=tile_id,
            year=year,
            resolution=resolution,
            filename=filename)

    def __str__(self) -> str:
        return self.filename


def _zipfile_id(filename: str):
    """
    Matches the zip file ID in a filename like '2017-LIDAR-DSM-1M-SD72se.zip'
    or 50cm_res_SM70_dsm.zip (Wales)
    """
    match = re.search("[a-z]{2}[0-9]{2}(?:se|sw|ne|nw)?", filename, re.IGNORECASE)
    return match.group() if match is not None else None


def _zip_year(filename: str) -> Optional[int]:
    """
    Matches the year in a filename like '2017-LIDAR-DSM-1M-SD72se.zip'.
    This is added to the names after they are downloaded so will not work
    on the filenames in the URLs in the JSON API responses or the bulk lidar.
    """
    match = re.search(r"^[0-9]{4}", filename, re.IGNORECASE)
    return int(match.group()) if match is not None else None


def _file_res(filename: str) -> Optional[Resolution]:
    """
    Matches the resolution in a filename like '2017-LIDAR-DSM-1M-SD72se.zip'
    or 'so8707_DSM_1M.tiff' or 50cm_res_SM70_dsm.zip (Wales)
    """
    match = re.search(r"(?:-|_|^)(1M|2M|50CM)[\-_.]", filename, re.IGNORECASE)
    return Resolution.from_string(match.group(1)) if match is not None else None


def _tile_id(filename: str):
    """
    Matches the tile ID in a filename like 'so8707_DSM_1M.tiff' or
    (for Welsh 50cm only: sm7924se_dsm_50cm.tiff
    """
    match = re.search("[a-z]{2}[0-9]{4}(?:se|sw|ne|nw)?", filename, re.IGNORECASE)
    return match.group() if match is not None else None


def zip_to_geotiffs(zt: ZippedTiles, lidar_dir: str) -> List[LidarTile]:
    tiff_paths = []
    with zipfile.ZipFile(join(lidar_dir, zt.filename)) as z:
        for zipinfo in z.infolist():
            # Convert to geotiff and add SRS metadata:
            asc_filename = zipinfo.filename
            tiff_filename = _get_tiff_filename(asc_filename)
            tiff_path = join(lidar_dir, tiff_filename)
            if not os.path.exists(tiff_path):
                z.extract(zipinfo, lidar_dir)
                tile = LidarTile.from_filename(tiff_path, zt.year)
                _asc_to_geotiff(lidar_dir, asc_filename, tiff_filename)
                tiff_paths.append(tile)
            else:
                logging.info(f"Skipping extraction of {tiff_filename}, already exists")
                tiff_paths.append(LidarTile.from_filename(join(lidar_dir, tiff_filename), zt.year))

    return tiff_paths


def _get_tiff_filename(asc_filename: str) -> str:
    return asc_filename.split('.')[0] + '.tiff'


def _asc_to_geotiff(lidar_dir: str, asc_filename: str, tiff_filename: str) -> None:
    """
    Convert asc file to geotiff, and add SRS metadata to file.
    """
    gdal.UseExceptions()

    drv = gdal.GetDriverByName('GTiff')
    gdal_asc_file = gdal.Open(join(lidar_dir, asc_filename))
    gdal_tiff_file = drv.CreateCopy(join(lidar_dir, tiff_filename), gdal_asc_file)
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(27700)
    gdal_tiff_file.SetProjection(srs.ExportToWkt())
    # https://gdal.org/api/python_gotchas.html
    gdal_asc_file = None
    gdal_tiff_file = None
    _try_remove(join(lidar_dir, asc_filename))


def _try_remove(filepath: str):
    try:
        os.remove(filepath)
    except OSError:
        pass
