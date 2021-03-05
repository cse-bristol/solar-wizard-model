import logging
import subprocess
from typing import List

import gdal


def create_vrt(tiles: List[str], vrt_file: str):
    logging.info("Creating vrt...")
    _run(f"gdalbuildvrt {vrt_file} {' '.join(tiles)}")


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


def rasterize(pg_uri: str, mask_sql: str, mask_file: str, resolution: float, srid: int):
    res = subprocess.run(f"""
        gdal_rasterize 
        -sql '{mask_sql}' 
        -burn 1 -tr {resolution} {resolution} 
        -init 0 -ot Int16 
        -of GTiff -a_srs EPSG:{srid} 
        "PG:{pg_uri}" 
        {mask_file}
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
        ds = gdal.Warp(out_tiff, to_crop, outputBounds=(ulx, lry, lrx, uly), xRes=xres, yRes=yres)
    else:
        ds = gdal.Warp(out_tiff, to_crop, outputBounds=(ulx, lry, lrx, uly))
    ds = None
    ref = None
    to_crop = None


def aspect(cropped_lidar: str, aspect_file: str):
    _run(f"gdaldem aspect {cropped_lidar} {aspect_file} -of GTiff -b 1 -zero_for_flat")


def _run(command: str):
    res = subprocess.run(command, capture_output=True, text=True, shell=True)
    print(res.stdout)
    print(res.stderr)
    if res.returncode != 0:
        raise ValueError(res.stderr)
