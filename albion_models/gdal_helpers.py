import json
import logging
import os
import subprocess
import textwrap
from typing import List, Tuple

import gdal
from osgeo import ogr


def create_vrt(tiles: List[str], vrt_file: str):
    logging.info("Creating vrt...")
    if tiles and len(tiles) > 0:
        run(f"gdalbuildvrt -resolution highest {vrt_file} {' '.join(tiles)}")
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
    if abs(xres) == abs(yres):
        return abs(xres)
    else:
        raise ValueError(f"Albion does not currently support non-equal x- and y- resolutions."
                         f"File {filename} had xres {abs(xres)}, yres {abs(yres)}")


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
    res = subprocess.run(f"""
        gdal_rasterize
        -sql '{mask_sql}'
        -burn 1 -tr {res} {res}
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
        gdal.Warp(out_tiff, to_crop, outputBounds=(ulx, lry, lrx, uly), xRes=xres, yRes=yres)
    else:
        gdal.Warp(out_tiff, to_crop, outputBounds=(ulx, lry, lrx, uly))


def set_resolution(in_tiff: str,
                   out_tiff: str,
                   res: float):
    """
    Output a new version of a raster with the specified resolution
    """
    gdal.UseExceptions()
    in_f = gdal.Open(in_tiff)
    _, xres, _, _, _, yres = in_f.GetGeoTransform()
    gdal.Warp(out_tiff, in_f, xRes=res, yRes=res)
    return out_tiff


def aspect(cropped_lidar: str, aspect_file: str):
    run(f"gdaldem aspect {cropped_lidar} {aspect_file} -of GTiff -b 1 -zero_for_flat")


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


def create_resolution_raster(in_tiff: str, out_tiff: str, res: float, nodata: int) -> str:
    run(f'''
        gdal_calc.py
        -A {in_tiff}
        --outfile={out_tiff}
        --quiet
        --NoDataValue={nodata}
        --calc="numpy.where(A!={nodata}, {res}, {nodata})"
    ''')
    return out_tiff


def polygonize(in_tiff: str, out_gpkg: str, out_layer: str, out_field: str, connectedness_8: bool = True):
    """
    Polygonize a raster. See `gdal_polygonize.py` in GDAL.

    This uses `FPolygonize` rather than `Polygonize`, so it doesn't cast float rasters
    to ints before running.

    :param in_tiff: raster to polygonize
    :param out_gpkg: out geopackage (should not exist)
    :param out_layer: name of table in geopackage
    :param out_field: name of field to write pixel values to
    :param connectedness_8: if True, use 8-connectedness to detect polygons. Otherwise
    uses 4-connectedness.
    :return: the name of the geopackage.
    """
    in_file = gdal.Open(in_tiff)
    band = in_file.GetRasterBand(1)

    drv = ogr.GetDriverByName("GPKG")
    dst_ds = drv.CreateDataSource(out_gpkg)
    srs = in_file.GetSpatialRef()
    dst_layer = dst_ds.CreateLayer(out_layer, geom_type=ogr.wkbPolygon, srs=srs)

    fd = ogr.FieldDefn(out_field, ogr.OFTReal)
    dst_layer.CreateField(fd)
    options = ['8CONNECTED=8'] if connectedness_8 else []

    gdal.FPolygonize(band, band.GetMaskBand(), dst_layer, 0, options, callback=None)
    return out_gpkg


def run(command: str):
    command = textwrap.dedent(command).replace("\n", " ").strip()
    res = subprocess.run(command, capture_output=True, text=True, shell=True)
    print(res.stdout.strip())
    if res.returncode != 0:
        print(res.stderr.strip())
        raise ValueError(res.stderr)
