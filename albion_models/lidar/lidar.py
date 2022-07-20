import shutil

import itertools
import logging
import os
import re
import zipfile
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from os.path import join
from typing import Optional, List, Dict

from osgeo import gdal, osr

from albion_models import gdal_helpers
from albion_models.lidar.grid_ref import os_grid_ref_to_wkt

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

    def create_merged_vrt(self, job_lidar_dir: str, vrt_name: str, coverage_vrt_name: str):
        """
        For each tile, merge the 50cm, 1m and 2m resolution versions into a single
        tile (giving priority to higher resolutions).

        Also creates a coverage raster for each tile showing what the highest resolution
        available is for each pixel.

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
            logging.warning("No tiles found, cannot create vrts")
            return

        merged_tiles = []
        coverage_tiles = []
        for tile_id, tiles in by_id.items():
            merged_filename = join(job_lidar_dir, f"{tile_id}_DSM_merged.tiff")
            if not os.path.exists(merged_filename):
                if len(tiles) == 1:
                    shutil.copy(tiles[0].filename, merged_filename)
                elif len(tiles) > 1:
                    res = tiles[-1].resolution.value
                    filenames = [tile.filename for tile in tiles]
                    gdal_helpers.merge(
                        filenames,
                        merged_filename,
                        res=res,
                        nodata=LIDAR_NODATA)
            else:
                logging.info(f"Skipping merge to {merged_filename}, already exists")

            merged_tiles.append(merged_filename)

            per_res_cov_raster = lidar_per_res_coverage(
                tiles,
                join(job_lidar_dir, f"{tile_id}_DSM_res_cov.tiff"),
                nodata=LIDAR_NODATA)
            coverage_tiles.append(per_res_cov_raster)

        gdal_helpers.create_vrt(merged_tiles, vrt_name)
        gdal_helpers.create_vrt(coverage_tiles, coverage_vrt_name)

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
            _try_remove(tile.filename)


def lidar_per_res_coverage(resolutions: List[LidarTile], outfile: str, nodata: int):
    """
    Create a raster which represents the per-resolution coverage of the tile.
    Each pixel will have the value of the highest-resolution coverage of that pixel.
    (e.g. either 0.5, 1.0 or 2.0)
    """
    to_merge = [gdal_helpers.create_resolution_raster(
                t.filename, t.filename + '.rmap.tiff', t.resolution.value, nodata)
                for t in resolutions]

    highest_res = resolutions[-1].resolution.value
    gdal_helpers.merge(to_merge, outfile, highest_res, nodata)

    for f in to_merge:
        _try_remove(f)

    return outfile


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


def zip_to_geotiffs(pg_conn, job_id: int, zt: ZippedTiles, lidar_dir: str) -> List[LidarTile]:
    tiff_paths = []
    with zipfile.ZipFile(join(lidar_dir, zt.filename)) as z:
        for zipinfo in z.infolist():
            # Convert to geotiff and add SRS metadata:
            tiff_filename = _get_tiff_filename(zipinfo.filename)
            if not os.path.exists(join(lidar_dir, tiff_filename)):
                z.extract(zipinfo, lidar_dir)
                tile = LidarTile.from_filename(join(lidar_dir, tiff_filename), zt.year)
                if _tile_intersects_bounds(pg_conn, job_id, tile.tile_id):
                    _asc_to_geotiff(lidar_dir, zipinfo.filename, tiff_filename)
                    tiff_paths.append(tile)
                else:
                    _try_remove(join(lidar_dir, zipinfo.filename))
            else:
                logging.info(f"Skipping extraction of {tiff_filename}, already exists")

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


def _tile_intersects_bounds(pg_conn, job_id: int, tile_id: str) -> bool:
    with pg_conn.cursor() as cursor:
        cursor.execute("""
            SELECT ST_Intersects(
                ST_Buffer(bounds, coalesce((params->>'horizon_search_radius')::int, 0)),
                ST_GeomFromText(%(tile_wkt)s, 27700))
            FROM models.job_queue WHERE job_id = %(job_id)s
        """, {
            'tile_wkt': os_grid_ref_to_wkt(tile_id),
            'job_id': job_id,
        })
        pg_conn.commit()
        return cursor.fetchone()[0]


def _try_remove(filepath: str):
    try:
        os.remove(filepath)
    except OSError:
        pass
