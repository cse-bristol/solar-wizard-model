# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import json
import logging
import os
import shlex
import subprocess
import textwrap
from typing import List, Tuple, Union, Callable

import math
import numpy as np
from osgeo import gdal

from solar_pv.util import esc_double_quotes


def create_vrt(tiles: List[str], vrt_file: str):
    logging.info("Creating vrt...")
    if tiles and len(tiles) > 0:
        command = f"gdalbuildvrt -resolution highest {vrt_file} {' '.join(tiles)}"

        logging.info("Creating .vrt")

        res = subprocess.run(shlex.split(command), capture_output=True, text=True)
        print(res.stdout.strip())
        if res.returncode != 0:
            print(res.stderr.strip())
            raise ValueError(res.stderr)
    else:
        logging.warning("No tiles passed, not creating vrt")


def files_in_vrt(vrt_file: str) -> List[str]:
    """Given a .vrt file, return a list of the files it references."""
    if not os.path.exists(vrt_file):
        logging.warning(f"Vrt {vrt_file} does not exist, not extracting file list")
        return []

    res = subprocess.run(f"gdalinfo -json {vrt_file}",
                         capture_output=True, text=True, shell=True)
    if res.returncode != 0:
        print(res.stderr)
        raise ValueError(res.stderr)
    json_out = json.loads(res.stdout)
    return [f for f in json_out['files'] if f != os.path.basename(f)]


def get_res(filename: str) -> float:
    gdal.UseExceptions()

    f = gdal.Open(filename)
    _, xres, _, _, _, yres = f.GetGeoTransform()
    xres = round(xres, 10)
    yres = round(yres, 10)
    if abs(xres) == abs(yres):
        return abs(xres)
    else:
        raise ValueError(f"Solar model does not currently support non-equal x- and y- resolutions."
                         f"File {filename} had xres {abs(xres)}, yres {abs(yres)}")


def get_xres_yres(filename: str) -> (float, float):
    gdal.UseExceptions()

    f = gdal.Open(filename)
    _, xres, _, _, _, yres = f.GetGeoTransform()
    xres = round(xres, 10)
    yres = round(yres, 10)
    return xres, yres


def get_res_unchecked(filename: str) -> float:
    """
    Get the resolution of the raster, and do not raise an error
    if the x and y resolutions differ - return the x res.
    """
    gdal.UseExceptions()

    f = gdal.Open(filename)
    _, xres, _, _, _, yres = f.GetGeoTransform()
    return abs(xres)


def get_srs_units(filename: str) -> Tuple[float, str]:
    gdal.UseExceptions()

    f = gdal.Open(filename)
    sref = f.GetSpatialRef()
    sref.AutoIdentifyEPSG()
    return float(sref.GetLinearUnits()), sref.GetLinearUnitsName()


def get_srid(filename: str, fallback: int = None) -> int:
    gdal.UseExceptions()

    f = gdal.Open(filename)
    sref = f.GetSpatialRef()
    sref.AutoIdentifyEPSG()
    code = sref.GetAuthorityCode(None)
    if code:
        logging.info(f"SRID of {filename} detected: {code}")
        return int(code)

    if fallback:
        logging.info(f"Failed to detect SRID of {filename}, assuming {fallback}")
        return fallback

    raise ValueError(f"Failed to detect SRID of {filename} and no fallback set!")


def rasterize(pg_uri: str, mask_sql: str, mask_file: str, res: float, srid: int):
    cmd = shlex.split(f"""
        gdal_rasterize
        -sql "{mask_sql}"
        -burn 1 -tr {res} {res}
        -init 0 -ot Int16
        -of GTiff -a_srs EPSG:{srid}
        -tap
        "PG:{pg_uri}"
        {mask_file}
        """)
    res = subprocess.run(cmd, capture_output=True, text=True)

    print(res.stdout)
    print(res.stderr)
    if res.returncode != 0:
        raise ValueError(res.stderr)


def rasterize_3d(pg_uri: str,
                 mask_sql: str,
                 mask_file: str,
                 res: Union[float, Tuple[float, float]],
                 srid: int,
                 output_type: str = "Float64"):
    """
    Creates a new raster using the Z value for the burn value for each polygon & nan outside of polygons
    """
    if isinstance(res, float):
        xres = res
        yres = res
    else:
        xres = res[0]
        yres = res[1]

    res = subprocess.run(f"""
        gdal_rasterize
        -sql "{esc_double_quotes(mask_sql)}"
        -3d -tr {xres} {yres}
        -init {math.nan} -ot {output_type}
        -of GTiff -a_srs EPSG:{srid}
        "PG:{pg_uri}"
        {mask_file}
        """.replace("\n", " "), capture_output=True, text=True, shell=True)
    print(res.stdout)
    print(res.stderr)
    if res.returncode != 0:
        raise ValueError(res.stderr)


def rasterize_3d_update(pg_uri: str, mask_sql: str, raster_to_update_filename: str):
    """
    Updates a raster. Uses the Z value for the burn value for each polygon inplace into raster_to_update_filename
    """
    res = subprocess.run(f"""
        gdal_rasterize
        -sql "{esc_double_quotes(mask_sql)}"
        -3d 
        "PG:{pg_uri}"
        {raster_to_update_filename}
        """.replace("\n", " "), capture_output=True, text=True, shell=True)
    print(res.stdout)
    print(res.stderr)
    if res.returncode != 0:
        raise ValueError(res.stderr)


def calc(raster_a: str, raster_b: str, expression: str, raster_out: str):
    """Create a new raster from 2 others merged using an expression
    """
    res = subprocess.run(f"""
        gdal_calc.py
        --calc="{expression}"
        -A "{raster_a}"
        -B "{raster_b}"
        --outfile="{raster_out}"
        --type=Float32
        --format=GTiff
        --extent=union
        --projectionCheck
        """.replace("\n", " "), capture_output=True, text=True, shell=True)
    print(res.stdout)
    print(res.stderr)
    if res.returncode != 0:
        raise ValueError(res.stderr)


def crop_or_expand(file_to_crop: str,
                   reference_file: str,
                   out_tiff: str,
                   adjust_resolution: bool):
    """
    Crop or expand a file of a type GDAL can open to match the dimensions of a reference file,
    and output to a tiff file.

    If adjust_resolution is set, the resolution of the output will match the reference file
    """
    gdal.UseExceptions()

    to_crop = gdal.Open(file_to_crop)
    ref = gdal.Open(reference_file)
    ulx, xres, xskew, uly, yskew, yres = ref.GetGeoTransform()
    lrx = ulx + (ref.RasterXSize * xres)
    lry = uly + (ref.RasterYSize * yres)
    if adjust_resolution:
        gdal.Warp(out_tiff, to_crop, outputBounds=(ulx, lry, lrx, uly), xRes=xres, yRes=yres,
                  creationOptions=['TILED=YES', 'COMPRESS=PACKBITS', 'BIGTIFF=YES'])
    else:
        gdal.Warp(out_tiff, to_crop, outputBounds=(ulx, lry, lrx, uly),
                  creationOptions=['TILED=YES', 'COMPRESS=PACKBITS', 'BIGTIFF=YES'])


def expand(raster_in: str, raster_out: str, buffer: int):
    """Assumes buffer is in same unit as SRS"""
    gdal.UseExceptions()

    ref = gdal.Open(raster_in)
    ulx, xres, xskew, uly, yskew, yres = ref.GetGeoTransform()

    # negative xres or yres indicates that values increase going W or N respectively
    # e.g. for 27700, xres is +ve and yres is -ve
    x_buffer = buffer if xres >= 0 else -buffer
    y_buffer = buffer if yres >= 0 else -buffer

    lrx = ulx + (ref.RasterXSize * xres) + x_buffer
    lry = uly + (ref.RasterYSize * yres) + y_buffer
    gdal.Warp(raster_out, raster_in,
              outputBounds=(ulx - x_buffer, lry, lrx, uly - y_buffer),
              creationOptions=['TILED=YES', 'COMPRESS=PACKBITS', 'BIGTIFF=YES'])


def reproject(raster_in: str, raster_out: str, src_srs: str, dst_srs: str):
    """
    Reproject a raster. Will keep the same number of pixels as before.
    """
    ref = gdal.Open(raster_in)
    ulx, xres, xskew, uly, yskew, yres = ref.GetGeoTransform()
    lrx = ulx + (ref.RasterXSize * xres)
    lry = uly + (ref.RasterYSize * yres)

    gdal.Warp(raster_out, raster_in, dstSRS=dst_srs, srcSRS=src_srs,
              width=ref.RasterXSize, height=ref.RasterYSize,
              # resampleAlg="bilinear",
              outputBounds=(ulx, lry, lrx, uly), outputBoundsSRS=src_srs,
              creationOptions=['TILED=YES', 'COMPRESS=PACKBITS', 'BIGTIFF=YES'])


def reproject_within_bounds(raster_in: str, raster_out: str, src_srs: str, dst_srs: str,
                            bounds: Tuple[float, float, float, float],
                            width: int, height: int):
    """
    Reproject a raster. By default, will keep the same number of pixels as before.
    :param bounds: Tuple, (ulx, lry, lrx, uly) in destination CRS units
    """
    gdal.Warp(raster_out, raster_in, dstSRS=dst_srs, srcSRS=src_srs,
              width=width, height=height,
              outputBounds=bounds,
              creationOptions=['TILED=YES', 'COMPRESS=PACKBITS'])


def set_resolution(in_tiff: str,
                   out_tiff: str,
                   res: float):
    """
    Output a new version of a raster with the specified resolution
    """
    gdal.UseExceptions()
    in_f = gdal.Open(in_tiff)
    _, xres, _, _, _, yres = in_f.GetGeoTransform()
    gdal.Warp(out_tiff, in_f, xRes=res, yRes=res,
              creationOptions=['TILED=YES', 'COMPRESS=PACKBITS', 'BIGTIFF=YES'])
    return out_tiff


def aspect(cropped_lidar: str, aspect_file: str):
    run(f"gdaldem aspect {cropped_lidar} {aspect_file} -of GTiff -b 1 -zero_for_flat -co \"COMPRESS=PACKBITS\" -co \"TILED=YES\" -co \"BIGTIFF=YES\"")


def slope(cropped_lidar: str, slope_file: str):
    run(f"gdaldem slope {cropped_lidar} {slope_file} -of GTiff -b 1  -co \"COMPRESS=PACKBITS\" -co \"TILED=YES\" -co \"BIGTIFF=YES\"")


def merge(files: List[str], output_file: str, res: float, nodata: int):
    """
    Merge raster tiles. They do not need to have the same resolution.
    Tiles later in the list will overwrite tiles earlier in the list
    (except where the earlier tile pixel is NODATA)
    """
    logging.info(f"Merging tiles {files} into {output_file}...")
    run(f"gdal_merge.py -ps {res} {res} -n {nodata} -a_nodata {nodata} -o {output_file} {' '.join(files)}")
    return output_file


def count_raster_pixels(tiff: str, value, band: int = 1) -> int:
    """
    Count the pixels in a raster that have value `value`
    """
    file = gdal.Open(tiff)
    band = file.GetRasterBand(band)
    a = band.ReadAsArray()
    return (a == value).sum()


def count_raster_pixels_pct(tiff: str, value, band: int = 1) -> float:
    """
    Count the percentage of pixels in a raster that have value `value`
    """
    file = gdal.Open(tiff)
    band = file.GetRasterBand(band)
    a = band.ReadAsArray()
    return (a == value).sum() / a.size


def run(command: str):
    command = textwrap.dedent(command).replace("\n", " ").strip()
    res = subprocess.run(command, capture_output=True, text=True, shell=True)
    stdout = res.stdout.strip()
    if stdout:
        print(stdout)
    if res.returncode != 0:
        stderr = res.stderr.strip()
        if stderr:
            print(stderr)
        raise ValueError(res.stderr)


def check_raster_alignment(ds1, ds2) -> None:
    ulx, xres, xskew, uly, yskew, yres = ds1.GetGeoTransform()
    _ulx, _xres, _xskew, _uly, _yskew, _yres = ds2.GetGeoTransform()
    x_size = ds1.RasterXSize
    y_size = ds1.RasterYSize
    _x_size = ds2.RasterXSize
    _y_size = ds2.RasterYSize
    if x_size != _x_size or y_size != _y_size:
        raise ValueError(f"rasters must have same size, were "
                         f"{(x_size, y_size)} and "
                         f"{(_x_size, _y_size)}")
    if ulx != _ulx or xres != _xres or xskew != _xskew or \
            uly != _uly or yskew != _yskew or yres != _yres:
        raise ValueError(f"rasters must be same alignment, were "
                         f"{(ulx, xres, xskew, uly, yskew, yres)} and "
                         f"{(_ulx, _xres, _xskew, _uly, _yskew, _yres)}")
