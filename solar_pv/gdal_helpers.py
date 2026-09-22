# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import logging
import os
import shutil
import subprocess
from typing import List, Optional, Tuple, Union

import math
from osgeo import gdal


class RasterizeError(ValueError):
    pass


def _run(cmd: List[str], error_cls: type = ValueError) -> None:
    """
    Run `cmd` as an argv list, echo its output, and raise `error_cls` with  
    stderr on a non-zero exit.
    """
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.stdout.strip():
        print(res.stdout.strip())
    if res.returncode != 0:
        if res.stderr.strip():
            print(res.stderr.strip())
        raise error_cls(res.stderr)


def create_vrt(tiles: List[str], vrt_file: str):
    if not tiles:
        logging.warning("No tiles passed, not creating vrt")
        return
    logging.info("Creating vrt...")
    _run(["gdalbuildvrt", "-resolution", "highest", vrt_file, *tiles])


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
    res = abs(res)
    _run([
        "gdal_rasterize",
        "-sql", mask_sql,
        "-burn", "1", "-tr", str(res), str(res),
        "-init", "0", "-ot", "Int16",
        "-of", "GTiff", "-a_srs", f"EPSG:{srid}",
        "-tap",
        f"PG:{pg_uri}",
        mask_file,
    ], error_cls=RasterizeError)


def rasterize_3d(pg_uri: str,
                 mask_sql: str,
                 mask_file: str,
                 res: Union[float, Tuple[float, float]],
                 srid: int,
                 output_type: str = "Float64",
                 bounds: Optional[Tuple[float, float, float, float]] = None):
    """
    Creates a new raster using the Z value for the burn value for each polygon & nan outside of polygons.

    :param bounds: optional (xmin, ymin, xmax, ymax) target extent. When given (with res), the
        output lands on exactly that grid, so it can be read without a further warp.
    """
    if isinstance(res, (tuple, list)):
        xres, yres = res
    else:
        xres = yres = res
    xres = abs(xres)
    yres = abs(yres)

    cmd = ["gdal_rasterize", "-sql", mask_sql, "-3d", "-tr", str(xres), str(yres)]
    if bounds is not None:
        cmd += ["-te", str(bounds[0]), str(bounds[1]), str(bounds[2]), str(bounds[3])]
    cmd += ["-init", str(math.nan), "-ot", output_type,
            "-of", "GTiff", "-a_srs", f"EPSG:{srid}",
            f"PG:{pg_uri}", mask_file]
    _run(cmd, error_cls=RasterizeError)


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


def aspect(cropped_lidar: str, aspect_file: str):
    _run(["gdaldem", "aspect", cropped_lidar, aspect_file,
                  "-of", "GTiff", "-b", "1", "-zero_for_flat",
                  "-co", "COMPRESS=PACKBITS", "-co", "TILED=YES", "-co", "BIGTIFF=YES"])


def slope(cropped_lidar: str, slope_file: str):
    _run(["gdaldem", "slope", cropped_lidar, slope_file,
                  "-of", "GTiff", "-b", "1",
                  "-co", "COMPRESS=PACKBITS", "-co", "TILED=YES", "-co", "BIGTIFF=YES"])


def set_nodata_value(input_tiff: str, nodata: int = -9999, band: int = 1):
    """
    Set the nodata value of a tiff.

    Creates a copy of the tiff, with 'TILED=YES' and 'COMPRESS=PACKBITS' set.
    """
    gdal.UseExceptions()

    dataset = gdal.Open(input_tiff)
    curr_nodata = dataset.GetRasterBand(band).GetNoDataValue()
    dataset = None
    output_tiff = input_tiff + ".fixed.tiff"

    if curr_nodata != nodata:
        gdal.Translate(
            output_tiff,
            input_tiff,
            noData=nodata,
            creationOptions=['TILED=YES', 'COMPRESS=PACKBITS']
        )

        dataset = gdal.Open(output_tiff, gdal.GA_Update)
        band_obj = dataset.GetRasterBand(band)
        array = band_obj.ReadAsArray()
        array[array == curr_nodata] = nodata
        band_obj.WriteArray(array)
        dataset.FlushCache()
        dataset = None

        old_tiff = input_tiff + ".old"
        shutil.move(input_tiff, old_tiff)
        shutil.move(output_tiff, input_tiff)

        if os.path.exists(old_tiff):
            os.remove(old_tiff)
