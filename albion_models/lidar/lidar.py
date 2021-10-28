import itertools
import logging
import os
import re
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from os.path import join
from typing import Optional, List, Dict

from albion_models import gdal_helpers

LIDAR_NODATA = -9999
"""
NODATA value used in LiDAR tiffs
"""

_USE_50CM_THRESHOLD = 0.25
"""
At least this % of the job area must be covered by 50cm LiDAR to
start using it. Otherwise only 1m and 2m will be used, as having to work
at 50cm resolution slows lots of things down (RANSAC, horizon detection, 
mapping roof planes -> pv installations).
"""


class Resolution(Enum):
    """
    The LiDAR resolutions used in Albion
    """
    R_50CM = 0.5
    R_1M = 1.0
    R_2M = 2.0

    @classmethod
    def from_string(cls, string: str) -> Optional['Resolution']:
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
    def from_filename(cls, filename: str):
        basename = os.path.basename(filename)
        zip_id = _zipfile_id(basename)
        resolution = _file_res(basename)
        year = _zip_year(basename)
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


@dataclass
class LidarJobTiles:
    """
    All the LiDAR tiles that cover the area of a job, at 50cm, 1m and 2m resolutions.
    """
    tiles_50cm: List[LidarTile] = field(default_factory=list)
    tiles_1m: List[LidarTile] = field(default_factory=list)
    tiles_2m: List[LidarTile] = field(default_factory=list)

    def merge(self, other_paths: 'LidarJobTiles'):
        self.tiles_50cm += other_paths.tiles_50cm
        self.tiles_1m += other_paths.tiles_1m
        self.tiles_2m += other_paths.tiles_2m

    def all_filenames(self) -> List[str]:
        return [tile.filename for tile in itertools.chain(self.tiles_2m, self.tiles_1m, self.tiles_50cm)]

    def add_tiles(self, tiles: List[LidarTile]):
        for tile in tiles:
            tile_list = self._get_tile_list(tile.resolution)
            tile_list.append(tile)

    def _get_tile_list(self, res: Resolution):
        if res == Resolution.R_50CM:
            return self.tiles_50cm
        elif res == Resolution.R_1M:
            return self.tiles_1m
        elif res == Resolution.R_2M:
            return self.tiles_2m
        else:
            raise ValueError(f"Unknown resolution {res}")

    def _get_tile(self, tile_id: str, res: Resolution) -> Optional[LidarTile]:
        tiles = self._get_tile_list(res)
        matches = [t for t in tiles if t.tile_id == tile_id]
        return matches[0] if len(matches) > 0 else None

    def create_merged_vrt(self, job_lidar_dir: str, vrt_name: str):
        """
        For each tile, merge the 50cm, 1m and 2m resolution versions into a single
        tile (giving priority to higher resolutions).

        Excludes 50cm LiDAR if the 50cm LiDAR has less than 25% pixel coverage of the
        entire area (as using 50cm LiDAR slows down several parts of the PV model, so
        it's a waste to include it if it barely covers any of the area)
        """
        os.makedirs(job_lidar_dir, exist_ok=True)
        by_id: Dict[str, List[LidarTile]] = defaultdict(list)
        _50cm_cov = self._get_50cm_coverage()
        if _50cm_cov > _USE_50CM_THRESHOLD:
            logging.info(f"50cm LiDAR has coverage over {_USE_50CM_THRESHOLD} % for job "
                         f"({_50cm_cov}): including it in merged LiDAR")
            all_tiles = itertools.chain(self.tiles_2m, self.tiles_1m, self.tiles_50cm)
        else:
            logging.info(f"50cm LiDAR has coverage < {_USE_50CM_THRESHOLD} % for job "
                         f"({_50cm_cov}): not including it in merged LiDAR")
            all_tiles = itertools.chain(self.tiles_2m, self.tiles_1m)

        for tile in all_tiles:
            by_id[tile.tile_id].append(tile)

        if len(by_id) == 0:
            return

        merged_tiles = []
        for tile_id, tiles in by_id.items():
            if len(tiles) == 1:
                merged_tiles.append(tiles[0])
            elif len(tiles) > 1:
                res = tiles[-1].resolution.value
                merged = gdal_helpers.merge(
                    [tile.filename for tile in tiles],
                    join(job_lidar_dir, f"{tile_id}_DSM_merged.tiff"),
                    res=res,
                    nodata=LIDAR_NODATA)
                merged_tiles.append(merged)

        gdal_helpers.create_vrt(merged_tiles, vrt_name)

    def _get_50cm_coverage(self) -> float:
        """
        Get the 50cm resolution coverage of the area as a percentage
        of the 1m resolution coverage.
        """
        by_id: Dict[str, List[LidarTile]] = defaultdict(list)
        for tile in itertools.chain(self.tiles_2m, self.tiles_1m, self.tiles_50cm):
            by_id[tile.tile_id].append(tile)

        if len(by_id) == 0:
            return 0.0

        tile_coverage = []
        for tile_id, tiles in by_id.items():
            tile_50cm = self._get_tile(tile_id, Resolution.R_50CM)
            tile_1m = self._get_tile(tile_id, Resolution.R_1M)
            if not tile_50cm:
                tile_coverage.append(0.0)
            elif not tile_1m:
                tile_coverage.append(1.0)
            else:
                nodata_50cm = gdal_helpers.count_raster_pixels_pct(tile_50cm.filename, LIDAR_NODATA)
                nodata_1m = gdal_helpers.count_raster_pixels_pct(tile_1m.filename, LIDAR_NODATA)
                tile_coverage.append(min((1.0 - nodata_50cm) / (1.0 - nodata_1m), 1.0))
        return sum(tile_coverage) / len(tile_coverage)

    def delete_unmerged_tiles(self):
        for tile in itertools.chain(self.tiles_2m, self.tiles_1m, self.tiles_50cm):
            os.remove(tile.filename)


def _zipfile_id(filename: str):
    """
    Matches the zip file ID in a filename like '2017-LIDAR-DSM-1M-SD72se.zip'
    """
    match = re.search("[a-z]{2}[0-9]{2}(?:se|sw|ne|nw)", filename, re.IGNORECASE)
    return match.group() if match is not None else None


def _zip_year(filename: str) -> Optional[int]:
    """
    Matches the year in a filename like '2017-LIDAR-DSM-1M-SD72se.zip'.
    This is added to the names after they are downloaded so will not work
    on the filenames in the URLs in the JSON API responses.
    """
    match = re.search(r"^[0-9]{4}", filename, re.IGNORECASE)
    return int(match.group()) if match is not None else None


def _file_res(filename: str) -> Optional[Resolution]:
    """
    Matches the resolution in a filename like '2017-LIDAR-DSM-1M-SD72se.zip'
    or 'so8707_DSM_1M.tiff'
    """
    match = re.search(r"[\-_](1M|2M|50CM)[\-_.]", filename, re.IGNORECASE)
    return Resolution.from_string(match.group(1)) if match is not None else None


def _tile_id(filename: str):
    """
    Matches the tile ID in a filename like 'so8707_DSM_1M.tiff'
    """
    match = re.search("[a-z]{2}[0-9]{4}", filename, re.IGNORECASE)
    return match.group() if match is not None else None
