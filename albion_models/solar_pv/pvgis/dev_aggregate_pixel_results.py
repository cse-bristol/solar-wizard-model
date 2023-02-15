import json
import os
import time
from os.path import join
from typing import List

from psycopg2.extras import DictCursor
from shapely import geometry, wkt

from albion_models import paths
from albion_models.db_funcs import connection
from albion_models.postgis import pixels_for_buildings
from albion_models.solar_pv import tables
from albion_models.solar_pv.constants import SYSTEM_LOSS
from albion_models.solar_pv.pvgis.aggregate_pixel_results import _aggregate_pixel_data, \
    _load_panels, _load_roof_planes

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


def aggregate_toids_pixels(pg_uri: str, job_id: int, toids: List[str], write_test_data: bool):
    schema = tables.schema(job_id)
    raster_tables = [f"{schema}.{t}" for t in RASTER_TABLES]
    page = 0
    page_size = 1000
    resolution = 1.0
    peak_power_per_m2 = 0.2
    system_loss = SYSTEM_LOSS

    with connection(pg_uri, cursor_factory=DictCursor) as pg_conn:
        print("loading data...")
        all_panels = _load_panels(pg_conn, job_id, page, page_size)
        all_roofs = _load_roof_planes(pg_conn, job_id, page, page_size)
        all_pixels = pixels_for_buildings(pg_conn, job_id, page, page_size, raster_tables)
        print("loaded data.")
        panels_to_write = []
        roofs_to_write = []

        for toid, toid_panels in all_panels.items():
            try:
                panels, roofs = _aggregate_pixel_data(
                    panels=toid_panels,
                    pixels=all_pixels[toid],
                    roofs=all_roofs[toid],
                    job_id=job_id,
                    pixel_fields=[t.split(".")[1] for t in raster_tables],
                    resolution=resolution,
                    peak_power_per_m2=peak_power_per_m2,
                    system_loss=system_loss,
                    debug=True)
                panels_to_write.extend(panels)
                roofs_to_write.extend(roofs)
            except Exception as e:
                print(f"PVMAPS pixel data aggregation failed on building {toid}:")
                print(json.dumps({'panels': toid_panels, 'pixels': all_pixels[toid], 'roofs': all_roofs[toid]}, sort_keys=True, default=str))
                raise e
            if write_test_data:
                print("Writing test data...")
                _write_test_data(toid, {'panels': toid_panels, 'pixels': all_pixels[toid]})


def aggregate_job_pixels(pg_uri: str, job_id: int, out_dir: str, write_test_data: bool):
    schema = tables.schema(job_id)
    raster_tables = [f"{schema}.{t}" for t in RASTER_TABLES]
    page = 0
    page_size = 1000
    resolution = 1.0
    peak_power_per_m2 = 0.2
    system_loss = SYSTEM_LOSS

    with connection(pg_uri, cursor_factory=DictCursor) as pg_conn:
        print("loading data...")
        all_panels = _load_panels(pg_conn, job_id, page, page_size)
        all_roofs = _load_roof_planes(pg_conn, job_id, page, page_size)
        all_pixels = pixels_for_buildings(pg_conn, job_id, page, page_size, raster_tables)
        print("loaded data.")
        panels_to_write = []
        roofs_to_write = []

        for toid, toid_panels in all_panels.items():
            try:
                panels, roofs = _aggregate_pixel_data(
                    panels=toid_panels,
                    pixels=all_pixels[toid],
                    roofs=all_roofs[toid],
                    job_id=job_id,
                    pixel_fields=[t.split(".")[1] for t in raster_tables],
                    resolution=resolution,
                    peak_power_per_m2=peak_power_per_m2,
                    system_loss=system_loss,
                    debug=True)
                panels_to_write.extend(panels)
                roofs_to_write.extend(roofs)
            except Exception as e:
                print(f"PVMAPS pixel data aggregation failed on building {toid}:")
                print(json.dumps({'panels': toid_panels, 'pixels': all_pixels[toid], 'roofs': all_roofs[toid]}, sort_keys=True, default=str))
                raise e

        if write_test_data:
            print("Writing test data...")
            t = int(time.time())
            _write_job_geojson(f"{job_id}_panels_{t}", out_dir, panels_to_write)


def _write_test_data(toid: str, building: dict):
    """
    Write out a test data CSV that can be used for unit tests.
    See test_aggregate_pixel_results.py
    """
    test_data_dir = join(paths.TEST_DATA, "pixel_aggregation")
    os.makedirs(test_data_dir, exist_ok=True)
    jsonfile = join(test_data_dir, f"{toid}.json")
    with open(jsonfile, 'w') as f:
        json.dump(building, f, sort_keys=True, default=str)


def _write_job_geojson(name: str, out_dir: str, to_write: List[dict]):
    geojson_features = []
    for panel in to_write:
        geojson_geom = geometry.mapping(wkt.loads(panel['panel']))
        del panel['panel']
        geojson_feature = {
          "type": "Feature",
          "geometry": geojson_geom,
          "properties": panel
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


if __name__ == "__main__":

    # aggregate_toids_pixels(
    #     "postgresql://albion_webapp:ydBbE3JCnJ4@localhost:5432/albion?application_name=blah",
    #     1646,
    #     [
    #         "osgb1000014995063",
    #     ],
    #     write_test_data=True)

    aggregate_job_pixels(
        "postgresql://albion_webapp:ydBbE3JCnJ4@localhost:5432/albion?application_name=blah",
        1647,
        "/home/neil/data/albion-models/pixel-agg",
        write_test_data=True
    )
