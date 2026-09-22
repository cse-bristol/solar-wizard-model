# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
"""Aggregate the per-pixel PV model outputs into per-roof-plane information."""
import json
import logging
import multiprocessing as mp
import os
from os.path import join

import numpy as np
import time
import traceback
from calendar import mdays
from collections import defaultdict, deque
from typing import List, Dict, Tuple

import math
import psycopg2.extras
from psycopg2.extras import Json
from psycopg2.sql import SQL, Identifier, Literal
from shapely import wkt, Polygon
from shapely.geometry import MultiPolygon
from shapely.strtree import STRtree

from solar_pv.constants import INTERANNUAL_GHI_COV, Z_P90
from solar_pv.db_funcs import count, sql_command, connection
from solar_pv.geos import square
from solar_pv.pv.confidence import roof_plane_confidence
from solar_pv.pv.pixels import PixelFields, pixels_for_geoms
from solar_pv import tables
from solar_pv.util import get_cpu_count

# Annual P90 derate: a poor-weather year still exceeded ~90% of the time, from
# inter-annual GHI variation only. See solar_pv/constants.py.
P90_DERATE = 1 - Z_P90 * INTERANNUAL_GHI_COV


def load_results_cpu_count():
    """Use 3/4s of available CPUs for aggregation (or max of 100)"""
    return min(int(get_cpu_count() * 0.75), 100)


def aggregate_from_arrays(pg_uri: str,
                          job_id: int,
                          pixel_fields: PixelFields,
                          resolution: float,
                          peak_power_per_m2: float,
                          system_loss: float,
                          page_size: int = 1000,
                          workers: int = None) -> None:
    """
    Aggregate the per-pixel PV outputs to roof planes, taking them as an in-memory PixelFields
    and reusing the _aggregate_pixel_data weighting math.

    Roof planes and building geometries come from the DB; the pixels come from
    pixels_for_geoms(pixel_fields, ...). The field names (kwh_year, month_01_wh..month_12_wh,
    horizon_00..NN, in that order, as run_pv produces them) name the per-pixel fields.

    Paginated over buildings. The main process does the DB reads and the per-page pixel
    extraction (which needs the in-memory fields); the CPU-heavy per-building roof-plane
    aggregation runs on a worker pool. At most ~2*workers pages are in flight, so memory stays
    bounded regardless of job size.
    """
    field_names = list(pixel_fields.values)
    pages = math.ceil(count(pg_uri, tables.schema(job_id), tables.BUILDINGS_TABLE) / page_size)
    if workers is None:
        workers = load_results_cpu_count()
    workers = max(1, min(pages, workers))
    logging.info(f"{pages} pages of {page_size} buildings to aggregate PV results for, "
                 f"{workers} workers")

    start_time = time.time()

    with connection(pg_uri) as pg_conn:
        _delete_existing_results(pg_conn, job_id)

    if pages:
        with connection(pg_uri, cursor_factory=psycopg2.extras.DictCursor) as read_conn, \
                connection(pg_uri) as write_conn:
            # generator: extract each page's roof planes + pixels lazily, in the main process:
            jobs = ((job_id, field_names, resolution, peak_power_per_m2, system_loss,
                     _load_roof_planes(read_conn, job_id, page, page_size),
                     pixels_for_geoms(pixel_fields,
                                      _load_building_geoms(read_conn, job_id, page, page_size)))
                    for page in range(pages))
            with mp.get_context("spawn").Pool(workers) as pool:
                _run_aggregation(pool, jobs, workers, write_conn, job_id)
            _insert_pv_buildings(write_conn, job_id)

    logging.info(f"PV results loaded, took {round(time.time() - start_time, 2)} s.")


def _run_aggregation(pool, jobs, workers: int, write_conn, job_id: int) -> None:
    """Feed page jobs to the pool keeping at most ~2*workers in flight (so only that many pages'
    pixels are extracted and buffered at once), writing each page's roofs as it completes."""
    jobs = iter(jobs)
    inflight = deque()
    for _ in range(workers * 2):
        try:
            inflight.append(pool.apply_async(_aggregate_page, (next(jobs),)))
        except StopIteration:
            break
    while inflight:
        roofs_to_write = inflight.popleft().get()
        _write_results(write_conn, job_id, roofs_to_write)
        try:
            inflight.append(pool.apply_async(_aggregate_page, (next(jobs),)))
        except StopIteration:
            pass


def _aggregate_page(job) -> List[dict]:
    """Worker: aggregate one page's buildings to roof planes. `job` carries the page's already-
    extracted roof planes and pixels (both plain picklable dicts), so the worker touches neither
    the DB nor the in-memory fields."""
    job_id, field_names, resolution, peak_power_per_m2, system_loss, roof_planes, pixels = job
    roofs_to_write = []
    for building_id, building_id_roof_planes in roof_planes.items():
        try:
            roofs = _aggregate_pixel_data(
                roof_planes=building_id_roof_planes,
                pixels=pixels.get(building_id, []),
                job_id=job_id,
                pixel_fields=field_names,
                resolution=resolution,
                peak_power_per_m2=peak_power_per_m2,
                system_loss=system_loss)
            roofs_to_write.extend(roofs)
        except Exception as e:
            print(f"PV pixel data aggregation failed on building {building_id}:")
            traceback.print_exc()
            _write_test_data(building_id, {'pixels': pixels.get(building_id, []), 'roofs': building_id_roof_planes})
            raise e
    return roofs_to_write


def _delete_existing_results(pg_conn, job_id: int) -> None:
    sql_command(
        pg_conn,
        """
        DELETE FROM models.pv_roof_plane WHERE job_id = %(job_id)s;
        DELETE FROM models.pv_building WHERE job_id = %(job_id)s;
        """,
        {"job_id": job_id})


def _insert_pv_buildings(pg_conn, job_id: int) -> None:
    sql_command(
        pg_conn,
        """
        INSERT INTO models.pv_building
        SELECT %(job_id)s, building_id, exclusion_reason, height
        FROM {buildings};
        """,
        {"job_id": job_id},
        buildings=Identifier(tables.schema(job_id), tables.BUILDINGS_TABLE))


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
        geom_grown: Polygon = wkt.loads(roof_plane['roof_geom_27700'])
        geom_raw: Polygon = wkt.loads(roof_plane['roof_geom_raw_27700'])

        if geom_raw.is_empty:
            raise ValueError("Raw roof plane geometry was empty")
        if geom_grown.is_empty:
            raise ValueError("Processed roof plane geometry was empty")

        roof_plane['horizon'] = [0 for _ in range(len(horizon_fields))]

        # Central (P50) estimate, computed over the raw geometry RANSAC actually
        # fit to the LiDAR points
        area = geom_raw.area / math.cos(math.radians(roof_plane['slope']))
        kwh_years = []
        kwh_months = [[] for _ in wh_month_fields]
        weights = []
        for idx in rtree.query(geom_raw, predicate='intersects'):
            pixel = pixel_squares[idx]
            pdata = pixels[idx]
            # PVMAPS produces kWh values per pixel as if a pixel was a 1kWp installation
            # so the values are adjusted accordingly.
            factor = peak_power_per_m2 * (1 - system_loss)
            kwh_years.append(pdata[kwh_year_field] * factor)
            for i, wh_monthday in enumerate(wh_month_fields):
                # Convert a 1-day Wh to a kWh for the whole month:
                kwh_months[i].append(pdata[wh_monthday] * 0.001 * mdays[i + 1] * factor)
            weights.append(pixel.intersection(geom_raw).area / pixel.area)

        for i, kwh_month in enumerate(kwh_months):
            roof_plane[_month_field(i)] = round(np.average(np.array(kwh_month) * area, weights=weights), 2)

        roof_plane['kwh_year'] = round(np.average(np.array(kwh_years) * area, weights=weights), 2)
        roof_plane['kwp'] = round(area * peak_power_per_m2, 2)
        roof_plane['area'] = round(area, 2)

        contributing_pixels = 0
        for idx in rtree.query(geom_grown, predicate='intersects'):
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
            roof_plane['kwh_per_kwp'] = roof_plane['kwh_year'] / roof_plane['kwp']

            # Conservative annual figure capturing inter-annual weather variation only:
            roof_plane['kwh_year_p90'] = round(roof_plane['kwh_year'] * P90_DERATE, 2)

            # Per-roof [0,1] model/data-quality score; sub-scores kept in `meta` so it
            # can be re-derived later without a model re-run:
            confidence, sub_scores = roof_plane_confidence(
                meta=roof_plane['meta'],
                is_flat=roof_plane['is_flat'],
                resolution=resolution,
                area_raw=geom_raw.area,
                area_grown=geom_grown.area)
            roof_plane['meta']['confidence'] = sub_scores
            roof_plane['confidence'] = round(confidence, 4)

            roofs_to_write.append(roof_plane)

            if debug:
                print(f"roof plane {roof_plane['roof_plane_id']} "
                      f"kWh year P50/P90: {roof_plane['kwh_year']} {roof_plane['kwh_year_p90']} "
                      f"confidence: {roof_plane['confidence']}")
        else:
            print(f"Roof intersected no pixels: roof_plane_id {roof_plane['roof_plane_id']}, building_id {roof_plane['building_id']}")

    return roofs_to_write


def _write_results(pg_conn, job_id: int, roofs: List[dict]):
    if not roofs:
        return
    for roof in roofs:
        roof['meta'] = Json(roof['meta'])
    with pg_conn.cursor() as cursor:
        psycopg2.extras.execute_values(
            cursor,
            SQL("""
                INSERT INTO models.pv_roof_plane (
                    building_id,
                    roof_plane_id,
                    job_id,
                    roof_geom_4326,
                    kwh_jan, kwh_feb, kwh_mar, kwh_apr, kwh_may, kwh_jun,
                    kwh_jul, kwh_aug, kwh_sep, kwh_oct, kwh_nov, kwh_dec,
                    kwh_year,
                    kwh_year_p90,
                    kwp,
                    kwh_per_kwp,
                    horizon,
                    area,
                    confidence,
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
                %(building_id)s,
                %(roof_plane_id)s,
                %(job_id)s,
                ST_SetSrid(
                    ST_Transform(%(roof_geom_27700)s,
                                 '+proj=tmerc +lat_0=49 +lon_0=-2 +k=0.9996012717 +x_0=400000 '
                                 '+y_0=-100000 +datum=OSGB36 +nadgrids=OSTN15_NTv2_OSGBtoETRS.gsb +units=m +no_defs',
                                 4326),
                    4326)::geometry(polygon, 4326),
                %(kwh_m01)s, %(kwh_m02)s, %(kwh_m03)s, %(kwh_m04)s, %(kwh_m05)s, %(kwh_m06)s,
                %(kwh_m07)s, %(kwh_m08)s, %(kwh_m09)s, %(kwh_m10)s, %(kwh_m11)s, %(kwh_m12)s,
                %(kwh_year)s,
                %(kwh_year_p90)s,
                %(kwp)s,
                %(kwh_per_kwp)s,
                %(horizon)s,
                %(area)s,
                %(confidence)s,
                %(x_coef)s,
                %(y_coef)s,
                %(intercept)s,
                %(slope)s,
                %(aspect)s,
                %(is_flat)s,
                %(meta)s
                )""")
        pg_conn.commit()


def _load_roof_planes(pg_conn, job_id: int, page: int, page_size: int, building_ids: List[str] = None) -> Dict[str, List[dict]]:
    if building_ids:
        building_id_filter = SQL("AND b.building_id = ANY({building_ids})").format(building_ids=Literal(building_ids))
    else:
        building_id_filter = SQL("")

    roofs = sql_command(
        pg_conn,
        """        
        WITH building_page AS (
            SELECT b.building_id
            FROM {buildings} b
            WHERE b.exclusion_reason IS NULL
            {building_id_filter}
            ORDER BY b.building_id
            OFFSET %(offset)s LIMIT %(limit)s
        )
        SELECT
            rp.building_id,
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
        INNER JOIN {roof_polygons} rp ON b.building_id = rp.building_id
        WHERE rp.usable
        ORDER BY building_id;
        """,
        {
            "offset": page * page_size,
            "limit": page_size,
        },
        roof_polygons=Identifier(tables.schema(job_id), tables.ROOF_POLYGON_TABLE),
        buildings=Identifier(tables.schema(job_id), tables.BUILDINGS_TABLE),
        building_id_filter=building_id_filter,
        result_extractor=lambda rows: rows)

    by_building_id = defaultdict(list)
    for roof in roofs:
        by_building_id[roof['building_id']].append(dict(roof))

    return dict(by_building_id)


def _load_building_geoms(pg_conn, job_id: int, page: int, page_size: int) -> Dict[str, object]:
    """Load the page's (EPSG:27700) building geometries, keyed by building_id. Uses the same
    building_page selection (exclusion_reason IS NULL, ordered by building_id) as _load_roof_planes
    and pixels_for_buildings, so the pages line up."""
    rows = sql_command(
        pg_conn,
        """
        SELECT b.building_id, ST_AsText(b.geom_27700) AS geom
        FROM {buildings} b
        WHERE b.exclusion_reason IS NULL
        ORDER BY b.building_id
        OFFSET %(offset)s LIMIT %(limit)s
        """,
        {"offset": page * page_size, "limit": page_size},
        buildings=Identifier(tables.schema(job_id), tables.BUILDINGS_TABLE),
        result_extractor=lambda rows: rows)
    return {r['building_id']: wkt.loads(r['geom']) for r in rows}


def _write_test_data(building_id, test_data):
    """Dump a failed building's pixels/roofs for debugging: to DEBUG_DATA_DIR if set, else stdout.
    Runs on the aggregation error path, so it must never raise itself and mask the real error."""
    debug_data_dir = os.environ.get("DEBUG_DATA_DIR")
    if debug_data_dir:
        os.makedirs(debug_data_dir, exist_ok=True)
        fname = join(debug_data_dir, f"pixel_agg_{building_id}.json")
        with open(fname, 'w') as f:
            json.dump(test_data, f, sort_keys=True, default=str)
        print(f"Wrote debug data to {fname}")
    else:
        print(json.dumps(test_data, sort_keys=True, default=str))
