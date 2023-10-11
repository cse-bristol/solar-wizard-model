# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
"""Convert raster data output of PVMAPS into per-panel information"""
import json
import logging
import multiprocessing as mp
import os
from os.path import join

import numpy as np
import time
import traceback
from calendar import mdays
from collections import defaultdict
from typing import List, Dict, Tuple

import math
import psycopg2.extras
from psycopg2.extras import Json
from psycopg2.sql import SQL, Identifier, Literal
from shapely import wkt, Polygon
from shapely.geometry import MultiPolygon
from shapely.strtree import STRtree

from solar_pv.db_funcs import count, sql_command, connection
from solar_pv.geos import square
from solar_pv.postgis import pixels_for_buildings
from solar_pv import tables
from solar_pv.util import get_cpu_count


def load_results_cpu_count():
    """Use 3/4s of available CPUs for aggregation (or max of 100)"""
    return min(int(get_cpu_count() * 0.75), 100)


def aggregate_pixel_results(pg_uri: str,
                            job_id: int,
                            raster_tables: List[str],
                            resolution: float,
                            peak_power_per_m2: float,
                            system_loss: float,
                            workers: int = load_results_cpu_count(),
                            page_size: int = 1000):
    """Convert raster data output of PVMAPS into per-roof-plane information"""
    pages = math.ceil(count(pg_uri, tables.schema(job_id), tables.BUILDINGS_TABLE) / page_size)
    workers = min(pages, workers)
    logging.info(f"{pages} pages of size {page_size} buildings to load PVMAPS results for")
    logging.info(f"Using {workers} processes for loading PVMAPS results")

    start_time = time.time()

    with connection(pg_uri) as pg_conn:
        sql_command(
            pg_conn,
            """
            DELETE FROM models.pv_roof_plane WHERE job_id = %(job_id)s;
            DELETE FROM models.pv_building WHERE job_id = %(job_id)s;
            """,
            {"job_id": job_id},
            buildings=Identifier(tables.schema(job_id), tables.BUILDINGS_TABLE))

    with mp.get_context("spawn").Pool(workers) as pool:
        wrapped_iterable = ((pg_uri, job_id, raster_tables, resolution,
                             peak_power_per_m2, system_loss, page, page_size)
                            for page in range(0, pages))
        for res in pool.starmap(_aggregate_results_page, wrapped_iterable, chunksize=1):
            pass

    with connection(pg_uri) as pg_conn:
        sql_command(
            pg_conn,
            """
            INSERT INTO models.pv_building
            SELECT %(job_id)s, toid, exclusion_reason, height
            FROM {buildings};            
            """,
            {"job_id": job_id},
            buildings=Identifier(tables.schema(job_id), tables.BUILDINGS_TABLE))

    logging.info(f"PVMAPS results loaded, took {round(time.time() - start_time, 2)} s.")


def _aggregate_results_page(pg_uri: str,
                            job_id: int,
                            raster_tables: List[str],
                            resolution: float,
                            peak_power_per_m2: float,
                            system_loss: float,
                            page: int,
                            page_size: int):
    start_time = time.time()
    with connection(pg_uri, cursor_factory=psycopg2.extras.DictCursor) as pg_conn:
        all_roof_planes = _load_roof_planes(pg_conn, job_id, page, page_size)
        all_pixels = pixels_for_buildings(pg_conn, job_id, page, page_size, raster_tables)
        roofs_to_write = []

        for toid, toid_roof_planes in all_roof_planes.items():
            try:
                roofs = _aggregate_pixel_data(
                    roof_planes=toid_roof_planes,
                    pixels=all_pixels[toid],
                    job_id=job_id,
                    pixel_fields=[t.split(".")[1] for t in raster_tables],
                    resolution=resolution,
                    peak_power_per_m2=peak_power_per_m2,
                    system_loss=system_loss)
                roofs_to_write.extend(roofs)
            except Exception as e:
                print(f"PVMAPS pixel data aggregation failed on building {toid}:")
                traceback.print_exc()
                _write_test_data({'pixels': all_pixels[toid], 'roofs': toid_roof_planes})
                raise e

        _write_results(pg_conn, job_id, roofs_to_write)
        print(f"Loaded page {page} of PVMAPS results, took {round(time.time() - start_time, 2)} s.")


def _month_field(i: int):
    """
    Convert a 0-indexed month index to the name of the field to store kWh data for
    that month.
    """
    return f"kwh_m{str(i + 1).zfill(2)}"


def _aggregate_pixel_data(roof_planes,
                          pixels,
                          job_id: int,
                          pixel_fields: List[str],
                          resolution: float,
                          peak_power_per_m2: float,
                          system_loss: float,
                          debug: bool = False) -> List[dict]:
    """
    Convert pixel-level data on monthly/yearly kWh output and horizon profile
    to roof-plane-level facts.
    """

    # Pixel fields:
    kwh_year_field = pixel_fields[0]
    wh_month_fields = pixel_fields[1:13]
    horizon_fields = pixel_fields[13:]

    # create squares for each pixel:
    if debug:
        print("creating pixel square geoms...")
    pixel_squares = []
    for p in pixels:
        ps = square(p['x'] - (resolution / 2.0), p['y'] - (resolution / 2.0), resolution)
        pixel_squares.append(ps)

    # For each roof plane: get the pixels that intersect and the extent to which they intersect
    # then use that as a factor to calculate roof plane-level data.
    if debug:
        print("calculating roof-plane-level facts...")

    rtree = STRtree(pixel_squares)
    roofs_to_write = []
    for roof_plane in roof_planes:
        geom_max = wkt.loads(roof_plane['roof_geom_27700'])
        geom_min = wkt.loads(roof_plane['roof_geom_raw_27700'])

        roof_plane['horizon'] = [0 for _ in range(len(horizon_fields))]

        variations = [{'geom': geom_min, 'suffix': 'min'},
                      {'geom': geom_max, 'suffix': 'max'}]

        for variation in variations:
            geom = variation['geom']
            suffix = variation['suffix']
            area = geom.area / math.cos(math.radians(roof_plane['slope']))

            kwh_years = []
            kwh_months = [[] for _ in wh_month_fields]
            weights = []
            for idx in rtree.query(geom, predicate='intersects'):
                pixel = pixel_squares[idx]
                pdata = pixels[idx]
                # PVMAPS produces kWh values per pixel as if a pixel was a 1kWp installation
                # so the values are adjusted accordingly.
                factor = peak_power_per_m2 * (1 - system_loss)
                kwh_years.append(pdata[kwh_year_field] * factor)
                for i, wh_monthday in enumerate(wh_month_fields):
                    # Convert a 1-day Wh to a kWh for the whole month:
                    kwh_months[i].append(pdata[wh_monthday] * 0.001 * mdays[i + 1] * factor)
                weights.append(pixel.intersection(geom).area / pixel.area)

            for i, kwh_month in enumerate(kwh_months):
                roof_plane[f'{_month_field(i)}_{suffix}'] = np.average(np.array(kwh_month) * area, weights=weights)

            roof_plane[f'kwh_year_{suffix}'] = np.average(np.array(kwh_years) * area, weights=weights)
            roof_plane[f'kwp_{suffix}'] = area * peak_power_per_m2
            roof_plane[f'area_{suffix}'] = round(area, 2)

            roof_plane[f'kwh_year_{suffix}'] = round(roof_plane[f'kwh_year_{suffix}'], 2)
            roof_plane[f'kwp_{suffix}'] = round(roof_plane[f'kwp_{suffix}'], 2)
            for i, wh_monthday in enumerate(wh_month_fields):
                roof_plane[f'{_month_field(i)}_{suffix}'] = round(roof_plane[f'{_month_field(i)}_{suffix}'], 2)

        contributing_pixels = 0
        for idx in rtree.query(geom_max, predicate='intersects'):
            pdata = pixels[idx]
            contributing_pixels += 1
            # Sum the horizon values for each slice:
            for i, h in enumerate(horizon_fields):
                roof_plane['horizon'][i] += pdata[h]

        if contributing_pixels > 0:
            # Average each horizon slice:
            roof_plane['horizon'] = [round(h / contributing_pixels, 2) for h in roof_plane['horizon']]
            roof_plane['job_id'] = job_id
            roof_plane['peak_power_per_m2'] = peak_power_per_m2

            roof_plane['area_avg'] = (roof_plane['area_min'] + roof_plane['area_max']) / 2
            roof_plane['kwh_year_avg'] = (roof_plane['kwh_year_min'] + roof_plane['kwh_year_max']) / 2
            roof_plane['kwp_avg'] = (roof_plane['kwp_min'] + roof_plane['kwp_max']) / 2
            roof_plane['kwh_per_kwp'] = roof_plane['kwh_year_avg'] / roof_plane['kwp_avg']

            for i, wh_monthday in enumerate(wh_month_fields):
                _min = roof_plane[f'{_month_field(i)}_min']
                _max = roof_plane[f'{_month_field(i)}_max']
                roof_plane[f'{_month_field(i)}_avg'] = round((_min + _max) / 2, 2)

            roofs_to_write.append(roof_plane)

            if debug:
                print(f"roof plane {roof_plane['roof_plane_id']} "
                      f"kWh min/avg/max: {roof_plane['kwh_year_min']} {roof_plane['kwh_year_avg']} {roof_plane['kwh_year_max']}")
        else:
            print(f"Roof intersected no pixels: roof_plane_id {roof_plane['roof_plane_id']}, toid {roof_plane['toid']}")

    return roofs_to_write


def _write_results(pg_conn, job_id: int, roofs: List[dict]):
    for roof in roofs:
        roof['meta'] = Json(roof['meta'])
    with pg_conn.cursor() as cursor:
        psycopg2.extras.execute_values(
            cursor,
            SQL("""
                INSERT INTO models.pv_roof_plane (
                    toid, 
                    roof_plane_id,
                    job_id, 
                    roof_geom_4326,
                    kwh_jan_min, kwh_jan_avg, kwh_jan_max, kwh_feb_min, kwh_feb_avg, kwh_feb_max, 
                    kwh_mar_min, kwh_mar_avg, kwh_mar_max, kwh_apr_min, kwh_apr_avg, kwh_apr_max,
                    kwh_may_min, kwh_may_avg, kwh_may_max, kwh_jun_min, kwh_jun_avg, kwh_jun_max,
                    kwh_jul_min, kwh_jul_avg, kwh_jul_max, kwh_aug_min, kwh_aug_avg, kwh_aug_max,
                    kwh_sep_min, kwh_sep_avg, kwh_sep_max, kwh_oct_min, kwh_oct_avg, kwh_oct_max,
                    kwh_nov_min, kwh_nov_avg, kwh_nov_max, kwh_dec_min, kwh_dec_avg, kwh_dec_max,
                    kwh_year_min,
                    kwh_year_avg,
                    kwh_year_max,
                    kwp_min,
                    kwp_avg,
                    kwp_max,
                    kwh_per_kwp,
                    horizon,
                    area_min,
                    area_avg,
                    area_max,
                    x_coef,
                    y_coef,
                    intercept,
                    slope,
                    aspect,
                    is_flat,
                    meta
                ) VALUES %s
            """).format(
                buildings=Identifier(tables.schema(job_id), tables.BUILDINGS_TABLE),
            ),
            argslist=roofs,
            template="""(
                %(toid)s, 
                %(roof_plane_id)s, 
                %(job_id)s, 
                ST_SetSrid(
                    ST_Transform(%(roof_geom_27700)s,
                                 '+proj=tmerc +lat_0=49 +lon_0=-2 +k=0.9996012717 +x_0=400000 '
                                 '+y_0=-100000 +datum=OSGB36 +nadgrids=OSTN15_NTv2_OSGBtoETRS.gsb +units=m +no_defs',
                                 4326),
                    4326)::geometry(polygon, 4326),
                %(kwh_m01_min)s, %(kwh_m01_avg)s, %(kwh_m01_max)s, %(kwh_m02_min)s, %(kwh_m02_avg)s, %(kwh_m02_max)s,
                %(kwh_m03_min)s, %(kwh_m03_avg)s, %(kwh_m03_max)s, %(kwh_m04_min)s, %(kwh_m04_avg)s, %(kwh_m04_max)s,
                %(kwh_m05_min)s, %(kwh_m05_avg)s, %(kwh_m05_max)s, %(kwh_m06_min)s, %(kwh_m06_avg)s, %(kwh_m06_max)s,
                %(kwh_m07_min)s, %(kwh_m07_avg)s, %(kwh_m07_max)s, %(kwh_m08_min)s, %(kwh_m08_avg)s, %(kwh_m08_max)s,
                %(kwh_m09_min)s, %(kwh_m09_avg)s, %(kwh_m09_max)s, %(kwh_m10_min)s, %(kwh_m10_avg)s, %(kwh_m10_max)s,
                %(kwh_m11_min)s, %(kwh_m11_avg)s, %(kwh_m11_max)s, %(kwh_m12_min)s, %(kwh_m12_avg)s, %(kwh_m12_max)s, 
                %(kwh_year_min)s, %(kwh_year_avg)s, %(kwh_year_max)s,  
                %(kwp_min)s, %(kwp_avg)s, %(kwp_max)s,  
                %(kwh_per_kwp)s,
                %(horizon)s, 
                %(area_min)s, %(area_avg)s, %(area_max)s,
                %(x_coef)s,
                %(y_coef)s,
                %(intercept)s,
                %(slope)s,
                %(aspect)s,
                %(is_flat)s,
                %(meta)s
                )""")
        pg_conn.commit()


def _load_roof_planes(pg_conn, job_id: int, page: int, page_size: int, toids: List[str] = None) -> Dict[str, List[dict]]:
    if toids:
        toid_filter = SQL("AND b.toid = ANY({toids})").format(toids=Literal(toids))
    else:
        toid_filter = SQL("")

    roofs = sql_command(
        pg_conn,
        """        
        WITH building_page AS (
            SELECT b.toid
            FROM {buildings} b
            WHERE b.exclusion_reason IS NULL
            {toid_filter}
            ORDER BY b.toid
            OFFSET %(offset)s LIMIT %(limit)s
        )
        SELECT
            rp.toid,
            ST_AsText(rp.roof_geom_27700) AS roof_geom_27700,
            ST_AsText(rp.roof_geom_raw_27700) AS roof_geom_raw_27700,
            rp.roof_plane_id,
            rp.slope,
            rp.aspect,
            rp.x_coef,
            rp.y_coef,
            rp.intercept,
            rp.is_flat,
            rp.meta
        FROM building_page b 
        INNER JOIN {roof_polygons} rp ON b.toid = rp.toid
        WHERE rp.usable
        ORDER BY toid;
        """,
        {
            "offset": page * page_size,
            "limit": page_size,
        },
        roof_polygons=Identifier(tables.schema(job_id), tables.ROOF_POLYGON_TABLE),
        buildings=Identifier(tables.schema(job_id), tables.BUILDINGS_TABLE),
        toid_filter=toid_filter,
        result_extractor=lambda rows: rows)

    by_toid = defaultdict(list)
    for roof in roofs:
        by_toid[roof['toid']].append(dict(roof))

    return dict(by_toid)


def _write_test_data(test_data):
    """Write test data for building"""
    debug_data_dir = os.environ.get("DEBUG_DATA_DIR")
    if debug_data_dir:
        fname = join(debug_data_dir, f"pixel_agg_{test_data['toid']}.json", 'w')
        with open(fname) as f:
            json.dump(test_data, f, sort_keys=True, default=str)
        print(f"Wrote debug data to {fname}")
    else:
        print(json.dumps(test_data, sort_keys=True, default=str))
