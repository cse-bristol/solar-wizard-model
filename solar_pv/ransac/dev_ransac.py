# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import logging
from os.path import join

import time

from solar_pv import paths

from osgeo import ogr, gdal

from solar_pv.lidar.lidar import LIDAR_NODATA
from solar_pv.ransac.run_ransac import _ransac_building, _load


def ransac_toid(pg_uri: str, job_id: int, toid: str, resolution_metres: float, out_dir: str, write_test_data: bool = True):
    logging.basicConfig(level=logging.DEBUG,
                        format='[%(asctime)s] %(levelname)s: %(message)s')
    os.makedirs(out_dir, exist_ok=True)

    by_toid = _load(pg_uri, job_id, page=0, page_size=1000, toids=[toid])
    building = by_toid[toid]
    planes = _ransac_building(building, toid, resolution_metres, debug=True)

    if len(planes) > 0:
        print("RANSAC: all planes:")
        for plane in planes:
            print(f'toid {plane["toid"]} slope {plane["slope"]} aspect {plane["aspect"]} sd {plane["sd"]} inliers {len(plane["inliers_xy"])}')
        _write_planes(toid, resolution_metres, out_dir, building, planes)
    else:
        print("No planes to write, not creating geoJSON")

    if write_test_data:
        _write_test_data(toid, building)


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

    ulx = min(building, key=lambda p: p['x'])['x']
    uly = max(building, key=lambda p: p['y'])['y']
    lrx = max(building, key=lambda p: p['x'])['x']
    lry = min(building, key=lambda p: p['y'])['y']

    xmax = int((lrx - ulx) / res) + 1
    ymax = int((uly - lry) / res) + 1
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
        # x = int(pixel["x"] - ulx)
        # y = int(uly - pixel["y"])
        # data[y, x] = 0

    for plane_id, plane in enumerate(planes):
        plane_id += 1
        toid = plane['toid']
        for inlier in plane["inliers_xy"]:
            pixel = by_pixel_id[f"{toid}:{inlier[0]}:{inlier[1]}"]
            x = int(pixel["x"] - ulx)
            y = int(uly - pixel["y"])
            data[y, x] = plane_id

    band.WriteArray(data, 0, 0)
    band.FlushCache()
    band.SetNoDataValue(LIDAR_NODATA)
    # ulx, xres, xskew, uly, yskew, yres
    out_ds.SetGeoTransform([ulx - res / 2, res, 0, uly + res / 2, 0, -res])
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
    print(f"\nwrote geojson {out_geojson}")


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
            feature.SetField("inliers", len(plane["inliers_xy"]))
            layer.SetFeature(feature)


def _write_test_data(toid, building):
    ransac_test_data_dir = join(paths.TEST_DATA, "ransac")
    csv = join(ransac_test_data_dir, f"{toid}.csv")
    with open(csv, 'w') as f:
        f.write("pixel_id,x,y,elevation,aspect\n")
        for pixel in building:
            f.write(f"{pixel['pixel_id']},{pixel['x']},{pixel['y']},{pixel['elevation']},{pixel['aspect']}\n")
    print(f"Wrote test data to {csv}")


def thinness_ratio_experiments():
    import numpy as np

    def tr(width, height):
        perimeter = width * 2 + height * 2
        area = width * height
        _tr = (4 * np.pi * area) / (perimeter * perimeter)
        print(f"area: {area} ({width} x {height}) TR: {_tr}")
        return _tr

    def trs(area):
        for width in range(1, area):
            height = area / width
            if height < width:
                break
            tr(width, height)

    # Question: for each area, there will be a given threshold that makes sense - what is it?
    # trs(10)  # 0.55
    # trs(20)  # 0.55
    # trs(30)  # 0.5
    # trs(40)  # 0.45
    # trs(50)  # 0.4
    # trs(100)  # 0.25
    # trs(200)  # 0.25
    # trs(300)  # 0.25
    # trs(400)  # 0.2
    # trs(500)  # 0.2
    # trs(750)  # 0.15
    # trs(1000)  # 0.10
    # trs(2000)  # 0.10
    # trs(3000)  # 0.10
    # trs(5000)  # 0.07


if __name__ == "__main__":
    import os
    # thinness_ratio_experiments()
    ransac_toid(
        os.getenv("PGW_URI"),
        1657,
        "osgb1000021681594",
        1.0,
        f"{os.getenv('DEV_DATA_DIR')}/ransac")