# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import logging
from os.path import join
from typing import List, Optional

import time

from solar_pv import paths

from osgeo import ogr, gdal

from solar_pv.lidar.lidar import LIDAR_NODATA
from solar_pv.ransac.run_ransac import _ransac_building, _load


def ransac_toids(pg_uri: str, job_id: int, toids: Optional[List[str]], resolution_metres: float, out_dir: str, write_test_data: bool = True):
    logging.basicConfig(level=logging.INFO,
                        format='[%(asctime)s] %(levelname)s: %(message)s')
    os.makedirs(out_dir, exist_ok=True)

    by_toid = _load(pg_uri, job_id, page=0, page_size=1000, toids=toids, force_load=True)
    all_planes = []
    all_pixels = []
    for toid, building in by_toid.items():
        print(f"\nTOID: {toid}\n")
        planes = _ransac_building(building['pixels'], toid, resolution_metres, building['polygon'], debug=True)
        all_planes.extend(planes)
        all_pixels.extend(building['pixels'])

        if len(planes) > 0:
            print("RANSAC: all planes:")
            for plane in planes:
                print(f'toid {plane["toid"]} slope {plane["slope"]} aspect {plane["aspect"]} sd {plane["sd"]} inliers {len(plane["inliers_xy"])}')
        else:
            print("No planes to write, not creating geoJSON")
        if write_test_data:
            _write_test_data(toid, building)

    if len(all_planes) > 0:
        _write_planes(toids, job_id, resolution_metres, out_dir, all_pixels, all_planes)


def _write_planes(toids: Optional[List[str]], job_id: int, resolution_metres: float, out_dir: str, pixels, planes):
    if toids is None:
        filename = f"job_{job_id}"
    elif len(toids) == 1:
        filename = toids[0]
    else:
        filename = "toids"
    t = int(time.time())
    tiff_out = join(out_dir, f"{filename}-{t}.tif")
    _write_tiff(tiff_out, resolution_metres, pixels, planes)
    geojson_out = join(out_dir, f"{filename}-{t}.geojson")
    _write_geojson(tiff_out, geojson_out)
    _write_geojson_fields(geojson_out, planes)
    try:
        os.remove(tiff_out)
    except OSError:
        pass


def _write_tiff(filepath: str, res: float, pixels, planes):
    import numpy
    from osgeo import gdal, osr
    from osgeo.gdalconst import GDT_Int32

    gdal.UseExceptions()

    ulx = min(pixels, key=lambda p: p['x'])['x']
    uly = max(pixels, key=lambda p: p['y'])['y']
    lrx = max(pixels, key=lambda p: p['x'])['x']
    lry = min(pixels, key=lambda p: p['y'])['y']

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

    for pixel in pixels:
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
    layer.CreateField(ogr.FieldDefn("score", ogr.OFTReal))
    layer.CreateField(ogr.FieldDefn("thinness_ratio", ogr.OFTReal))
    layer.CreateField(ogr.FieldDefn("cv_hull_ratio", ogr.OFTReal))
    layer.CreateField(ogr.FieldDefn("plane_type", ogr.OFTString))
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
            feature.SetField("score", plane["score"])
            feature.SetField("thinness_ratio", plane["thinness_ratio"])
            feature.SetField("cv_hull_ratio", plane["cv_hull_ratio"])
            feature.SetField("plane_type", plane["plane_type"])
            feature.SetField("inliers", len(plane["inliers_xy"]))
            layer.SetFeature(feature)


def _write_test_data(toid, building):
    ransac_test_data_dir = join(paths.TEST_DATA, "ransac")
    csv = join(ransac_test_data_dir, f"{toid}.csv")
    with open(csv, 'w') as f:
        f.write("pixel_id,x,y,elevation,aspect\n")
        for pixel in building['pixels']:
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

    # ransac_toids(
    #     os.getenv("PGW_URI"),
    #     1649,
    #     [
    #         # "osgb1000014994638",
    #         # "osgb1000014994637",
    #         # "osgb1000014994948",
    #         # "osgb1000014994624",
    #         # "osgb1000014994950",
    #         # "osgb1000014994951",
    #
    #         "osgb5000005116861453",
    #         "osgb5000005116861461",
    #         "osgb1000014994628",
    #         "osgb1000014994636",
    #         "osgb1000014994648",
    #         "osgb1000014994630",
    #         "osgb1000014994634",
    #         "osgb1000014994631",
    #         "osgb1000014994632",
    #         "osgb1000014994629",
    #         "osgb1000014994635",
    #         "osgb1000014994633",
    #         "osgb1000014994626",
    #         "osgb1000014994627",
    #         "osgb1000014994624",
    #         "osgb1000014994625",
    #         "osgb1000014994654",
    #         "osgb1000014994658",
    #         "osgb1000014994649",
    #         "osgb1000014994652",
    #         "osgb1000014994646",
    #         "osgb1000014994653",
    #         "osgb1000014994641",
    #         "osgb1000014994651",
    #         "osgb1000014994639",
    #         "osgb1000014994644",
    #         "osgb1000014994637",
    #         "osgb1000014994650",
    #         "osgb1000014994655",
    #         "osgb1000014994657",
    #         "osgb1000014994660",
    #         "osgb1000014994656",
    #         "osgb1000014994647",
    #         "osgb1000014994643",
    #         "osgb1000014994642",
    #         "osgb1000014994645",
    #         "osgb1000014994659",
    #         "osgb1000014994638",
    #         "osgb1000014994640",
    #         "osgb1000014995257",
    #         "osgb5000005116861456",
    #         "osgb1000014995257",
    #
    #         "osgb1000014994950",
    #         "osgb1000014994952",
    #         "osgb1000014994947",
    #         "osgb1000014994949",
    #         "osgb1000014994951",
    #         "osgb1000014994948",
    #
    #         "osgb1000014998052"
    #     ],
    #     1.0,
    #     f"{os.getenv('DEV_DATA_DIR')}/ransac",
    #     write_test_data=False)

    # ransac_toids(
    #     os.getenv("PGW_URI"),
    #     1649,
    #     ["osgb1000014994631"],
    #     # None,
    #     1.0,
    #     f"{os.getenv('DEV_DATA_DIR')}/ransac",
    #     write_test_data=False)

    # ransac_toids(
    #     os.getenv("PGW_URI"),
    #     1657,
    #     # ["osgb1000021672464"],
    #     None,
    #     1.0,
    #     f"{os.getenv('DEV_DATA_DIR')}/ransac",
    #     write_test_data=False)

    ransac_toids(
        os.getenv("PGW_URI"),
        1650,
        [
            "osgb5000005110302956",
            "osgb1000014963168"
        ],
        # None,
        1.0,
        f"{os.getenv('DEV_DATA_DIR')}/ransac",
        write_test_data=False)