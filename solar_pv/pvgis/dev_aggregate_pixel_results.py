# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import json
import time
from os.path import join
from typing import List, Dict

from psycopg2.extras import DictCursor
from shapely import geometry, wkt

from solar_pv import paths
from solar_pv.db_funcs import connection
from solar_pv.postgis import pixels_for_buildings
from solar_pv import tables
from solar_pv.constants import SYSTEM_LOSS
from solar_pv.pvgis.aggregate_pixel_results import _aggregate_pixel_data, \
    _load_roof_planes, _write_results

PIXEL_DATA = join(paths.TEST_DATA, "pixel_aggregation")
RASTER_TABLES = ['kwh_year',
                 'month_01_wh',
                 'month_02_wh',
                 'month_03_wh',
                 'month_04_wh',
                 'month_05_wh',
                 'month_06_wh',
                 'month_07_wh',
                 'month_08_wh',
                 'month_09_wh',
                 'month_10_wh',
                 'month_11_wh',
                 'month_12_wh',
                 'horizon_00',
                 'horizon_01',
                 'horizon_02',
                 'horizon_03',
                 'horizon_04',
                 'horizon_05',
                 'horizon_06',
                 'horizon_07',
                 'horizon_08',
                 'horizon_09',
                 'horizon_10',
                 'horizon_11',
                 'horizon_12',
                 'horizon_13',
                 'horizon_14',
                 'horizon_15',
                 'horizon_16',
                 'horizon_17',
                 'horizon_18',
                 'horizon_19',
                 'horizon_20',
                 'horizon_21',
                 'horizon_22',
                 'horizon_23',
                 'horizon_24',
                 'horizon_25',
                 'horizon_26',
                 'horizon_27',
                 'horizon_28',
                 'horizon_29',
                 'horizon_30',
                 'horizon_31',
                 'horizon_32',
                 'horizon_33',
                 'horizon_34',
                 'horizon_35']


def aggregate_pixels(pg_uri: str, job_id: int, toids: List[str] = None,
                     write_test_data: bool = False, write_geojson: bool = False, write_to_db: bool = False,
                     out_dir: str = None):
    schema = tables.schema(job_id)
    raster_tables = [f"{schema}.{t}" for t in RASTER_TABLES]
    page = 0
    page_size = 1000
    resolution = 1.0
    peak_power_per_m2 = 0.2
    system_loss = SYSTEM_LOSS

    with connection(pg_uri, cursor_factory=DictCursor) as pg_conn:
        print("loading data...")
        all_roofs = _load_roof_planes(pg_conn, job_id, page, page_size, toids=toids)
        all_pixels = pixels_for_buildings(pg_conn, job_id, page, page_size, raster_tables, toids=toids)
        print("loaded data.")
        roofs_to_write = []

        for toid, toid_roof_planes in all_roofs.items():
            try:
                roofs = _aggregate_pixel_data(
                    pixels=all_pixels[toid],
                    roof_planes=toid_roof_planes,
                    job_id=job_id,
                    pixel_fields=[t.split(".")[1] for t in raster_tables],
                    resolution=resolution,
                    peak_power_per_m2=peak_power_per_m2,
                    system_loss=system_loss,
                    debug=True)
                roofs_to_write.extend(roofs)
            except Exception as e:
                print(f"PVMAPS pixel data aggregation failed on building {toid}:")
                print(json.dumps({'pixels': all_pixels[toid], 'roofs': all_roofs[toid]}, sort_keys=True, default=str))
                raise e
            if write_test_data:
                print(f"Writing test data for TOID {toid}...")
                _write_test_data(toid, {'pixels': all_pixels[toid], 'roofs': all_roofs[toid]})

        if write_geojson:
            print("Writing whole job data...")
            t = int(time.time())
            _write_roof_geojson(f"{job_id}_roofs_{t}", out_dir, roofs_to_write)
            _write_pixel_geojson(f"{job_id}_pixels_{t}", out_dir, all_pixels)

        if write_to_db:
            _write_results(pg_conn, job_id, roofs_to_write)


def _write_test_data(toid: str, building: dict):
    """
    Write out a test data CSV that can be used for unit tests.
    See test_aggregate_pixel_results.py
    """
    os.makedirs(PIXEL_DATA, exist_ok=True)
    jsonfile = join(PIXEL_DATA, f"{toid}.json")
    with open(jsonfile, 'w') as f:
        json.dump(building, f, sort_keys=True, default=str)


def _write_roof_geojson(name: str, out_dir: str, to_write: List[dict]):
    geojson_features = []
    for roof in to_write:
        geojson_geom = geometry.mapping(wkt.loads(roof['roof_geom_27700']))
        del roof['roof_geom_27700']
        geojson_feature = {
          "type": "Feature",
          "geometry": geojson_geom,
          "properties": roof
        }
        geojson_features.append(geojson_feature)

    geojson = {
        "type": "FeatureCollection",
        "crs": {"type": "name", "properties": {"name": "urn:ogc:def:crs:EPSG::27700"}},
        "features": geojson_features
    }

    os.makedirs(out_dir, exist_ok=True)
    with open(join(out_dir, f"{name}.geojson"), 'w') as f:
        json.dump(geojson, f)


def _write_pixel_geojson(name: str, out_dir: str, all_pixels: Dict[str, List[dict]]):
    geojson_features = []
    for toid, pixels in all_pixels.items():
        for pixel in pixels:
            geojson_geom = geometry.mapping(wkt.loads(f"POINT ({pixel['x']} {pixel['y']})"))
            geojson_feature = {
              "type": "Feature",
              "geometry": geojson_geom,
              "properties": pixel
            }
            geojson_features.append(geojson_feature)

    geojson = {
        "type": "FeatureCollection",
        "crs": {"type": "name", "properties": {"name": "urn:ogc:def:crs:EPSG::27700"}},
        "features": geojson_features
    }

    os.makedirs(out_dir, exist_ok=True)
    with open(join(out_dir, f"{name}.geojson"), 'w') as f:
        json.dump(geojson, f)


def write_testdata_geojson(toid: str, out_dir: str):
    """Write out panel and pixel geojson for an existing test data file"""
    with open(join(PIXEL_DATA, f"{toid}.json")) as f:
        data = json.load(f)
        pixels = data['pixels']
        panels = data['panels']
        _write_roof_geojson(f"{toid}_panels.geojson", out_dir, panels)
        _write_pixel_geojson(f"{toid}_pixels.geojson", out_dir, {toid: pixels})


if __name__ == "__main__":
    import os
    # aggregate_pixels(
    #     os.getenv("PGW_URI"),
    #     1646,
    #     [
    #         "osgb1000014995063",
    #     ],
    #     write_test_data=True)

    aggregate_pixels(
        os.getenv("PGW_URI"),
        1662,
        out_dir=f"{os.getenv('DEV_DATA_DIR')}/pixel-agg",
        write_to_db=True
    )
    # write_testdata_geojson("osgb1000016884534", f"{os.getenv('DEV_DATA_DIR')}/pixel-agg")
