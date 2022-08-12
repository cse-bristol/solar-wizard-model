import logging
import os
from os.path import join

import time

from psycopg2.sql import Identifier

from albion_models.db_funcs import connection, sql_command

import psycopg2.extras

from albion_models.lidar.lidar import LIDAR_NODATA
from albion_models.solar_pv import tables
from albion_models.solar_pv.ransac.run_ransac import _ransac_building


def ransac_toid(pg_uri: str, job_id: int, toid: str, resolution_metres: float, out_dir: str):
    logging.basicConfig(level=logging.DEBUG,
                        format='[%(asctime)s] %(levelname)s: %(message)s')
    os.makedirs(out_dir, exist_ok=True)

    toid, building = _load_toid(pg_uri, job_id, toid)
    planes = _ransac_building(building, toid, resolution_metres)
    if len(planes) == 0 and len(building) > 1000:
        # Retry with relaxed constraints around group checks and with a higher
        # `max_trials` for larger buildings where we care more:
        planes = _ransac_building(building, toid, resolution_metres, max_trials=3000,
                                  include_group_checks=False)

    for plane in planes:
        print(f'toid {plane["toid"]} slope {plane["slope"]} aspect {plane["aspect"]} sd {plane["sd"]} inliers {len(plane["inliers"])}')

    tiff_out = join(out_dir, f"{toid}-{int(time.time())}.tif")
    _write_tiff(tiff_out, resolution_metres, building, planes)


def _write_tiff(filepath: str, res: float, building, planes):
    import numpy
    from osgeo import gdal, osr
    from osgeo.gdalconst import GDT_Int32

    gdal.UseExceptions()

    ulx = min(building, key=lambda p: p['easting'])['easting']
    uly = max(building, key=lambda p: p['northing'])['northing']
    lrx = max(building, key=lambda p: p['easting'])['easting']
    lry = min(building, key=lambda p: p['northing'])['northing']

    xmax = int((lrx - ulx) * res) + 1
    ymax = int((uly - lry) * res) + 1
    print(f"xmax {xmax} ymax {ymax}")

    driver = gdal.GetDriverByName('GTiff')
    out_ds = driver.Create(filepath, xmax, ymax, 1, GDT_Int32)
    band = out_ds.GetRasterBand(1)
    data = numpy.zeros((ymax, xmax), numpy.int16)

    for x in range(0, xmax):
        for y in range(0, ymax):
            data[y, x] = LIDAR_NODATA

    by_pixel_id = {}
    for pixel in building:
        by_pixel_id[pixel['pixel_id']] = pixel
        x = int(pixel["easting"] - ulx)
        y = int(uly - pixel["northing"])
        data[y, x] = 0

    for plane_id, plane in enumerate(planes):
        plane_id += 1
        for inlier in plane["inliers"]:
            pixel = by_pixel_id[inlier]
            x = int(pixel["easting"] - ulx)
            y = int(uly - pixel["northing"])
            data[y, x] = plane_id

    band.WriteArray(data, 0, 0)
    band.FlushCache()
    band.SetNoDataValue(LIDAR_NODATA)
    # ulx, xres, xskew, uly, yskew, yres
    out_ds.SetGeoTransform([ulx - res / 2, res, 0, uly - res / 2, 0, -res])
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(27700)
    out_ds.SetProjection(srs.ExportToWkt())
    print(f"wrote tiff {filepath}")


def _load_toid(pg_uri: str, job_id: int, toid: str):
    """
    Load LIDAR pixel data for RANSAC processing for a specific TOID.
    """
    with connection(pg_uri, cursor_factory=psycopg2.extras.DictCursor) as pg_conn:
        rows = sql_command(
            pg_conn,
            """
            SELECT h.pixel_id, h.easting, h.northing, h.elevation, h.aspect, b.toid
            FROM {buildings} b
            LEFT JOIN {lidar_pixels} h ON h.toid = b.toid
            WHERE h.elevation != %(lidar_nodata)s
            AND b.toid = %(toid)s
            ORDER BY b.toid;
            """,
            {
                "toid": toid,
                "lidar_nodata": LIDAR_NODATA,
            },
            lidar_pixels=Identifier(tables.schema(job_id), tables.LIDAR_PIXEL_TABLE),
            buildings=Identifier(tables.schema(job_id), tables.BUILDINGS_TABLE),
            result_extractor=lambda res: res)

        return toid, rows


if __name__ == "__main__":
    ransac_toid(
        "postgresql://albion_webapp:ydBbE3JCnJ4@localhost:5432/albion?application_name=blah",
        1194,
        "osgb1000019927870",
        1.0,
        "/home/neil/data/albion-models/ransac")
