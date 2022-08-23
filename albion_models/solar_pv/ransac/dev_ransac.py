import logging
import os
from os.path import join

import time

from psycopg2.sql import Identifier

from albion_models.db_funcs import connection, sql_command

import psycopg2.extras
from osgeo import ogr
import gdal

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

    if len(planes) > 0:
        _write_planes(toid, resolution_metres, out_dir, building, planes)
    else:
        print("No planes to write, not creating geoJSON")


def _write_planes(toid: str, resolution_metres: float, out_dir: str, building, planes):
    t = int(time.time())
    tiff_out = join(out_dir, f"{toid}-{t}.tif")
    _write_tiff(tiff_out, resolution_metres, building, planes)
    geojson_out = join(out_dir, f"{toid}-{t}.geojson")
    _write_geojson(tiff_out, geojson_out)
    _write_geojson_fields(geojson_out, planes)
    try:
        os.remove(tiff_out)
    except OSError:
        pass


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
    # print(f"xmax {xmax} ymax {ymax}")

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
        # this sets all non-nodata pixels that aren't in a plane to 0:
        # x = int(pixel["easting"] - ulx)
        # y = int(uly - pixel["northing"])
        # data[y, x] = 0

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
    # print(f"wrote tiff {filepath}")


def _write_geojson(in_tiff: str, out_geojson: str, connectedness_8: bool = True):
    gdal.UseExceptions()

    in_file = gdal.Open(in_tiff)
    band = in_file.GetRasterBand(1)

    drv = ogr.GetDriverByName("GeoJSON")
    dst_ds = drv.CreateDataSource(out_geojson)
    srs = in_file.GetSpatialRef()
    dst_layer = dst_ds.CreateLayer("planes", geom_type=ogr.wkbPolygon, srs=srs)

    fd = ogr.FieldDefn("plane_id", ogr.OFTInteger)
    dst_layer.CreateField(fd)
    options = ['8CONNECTED=8'] if connectedness_8 else []

    gdal.FPolygonize(band, band.GetMaskBand(), dst_layer, 0, options, callback=None)
    print(f"wrote geojson {out_geojson}")


def _write_geojson_fields(geojson: str, planes):
    gdal.UseExceptions()
    driver = ogr.GetDriverByName('GeoJSON')
    dataSource = driver.Open(geojson, 1)

    layer = dataSource.GetLayer()
    layer.CreateField(ogr.FieldDefn("toid", ogr.OFTString))
    layer.CreateField(ogr.FieldDefn("slope", ogr.OFTReal))
    layer.CreateField(ogr.FieldDefn("aspect", ogr.OFTReal))
    layer.CreateField(ogr.FieldDefn("sd", ogr.OFTReal))
    layer.CreateField(ogr.FieldDefn("aspect_circ_mean", ogr.OFTReal))
    layer.CreateField(ogr.FieldDefn("aspect_circ_sd", ogr.OFTReal))
    layer.CreateField(ogr.FieldDefn("inliers", ogr.OFTInteger))

    for feature in layer:
        plane_id = feature.GetField("plane_id")
        if plane_id != 0:
            plane = planes[plane_id - 1]
            feature.SetField("toid", plane["toid"])
            feature.SetField("slope", plane["slope"])
            feature.SetField("aspect", plane["aspect"])
            feature.SetField("sd", plane["sd"])
            feature.SetField("aspect_circ_mean", plane["aspect_circ_mean"])
            feature.SetField("aspect_circ_sd", plane["aspect_circ_sd"])
            feature.SetField("inliers", len(plane["inliers"]))
            layer.SetFeature(feature)


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
        "osgb1000020074938",
        1.0,
        "/home/neil/data/albion-models/ransac")
