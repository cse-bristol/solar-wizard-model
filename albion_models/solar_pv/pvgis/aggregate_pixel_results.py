"""Convert raster data output of PVMAPS into per-panel information"""
import json
import logging
import multiprocessing as mp
import time
from calendar import mdays
from typing import List, Dict

import math
import psycopg2.extras
from psycopg2.sql import SQL, Identifier, Literal
from shapely import wkt
from shapely.strtree import STRtree

from albion_models.db_funcs import count, sql_command, connection
from albion_models.geos import square
from albion_models.postgis import pixels_for_buildings
from albion_models.solar_pv import tables
from albion_models.util import get_cpu_count


def load_results_cpu_count():
    """Use 3/4s of available CPUs for lidar checking"""
    return min(int(get_cpu_count() * 0.75), 100)


def aggregate_pixel_results(pg_uri: str,
                            job_id: int,
                            raster_tables: List[str],
                            resolution: float,
                            peak_power_per_m2: float,
                            system_loss: float,
                            workers: int = load_results_cpu_count(),
                            page_size: int = 1000):
    """Convert raster data output of PVMAPS into per-panel information"""
    pages = math.ceil(
        count(pg_uri, tables.schema(job_id), tables.BUILDINGS_TABLE) / page_size)
    logging.info(
        f"{pages} pages of size {page_size} buildings to load PVMAPS results for")
    logging.info(f"Using {workers} processes for loading PVMAPS results")

    with mp.get_context("spawn").Pool(workers) as pool:
        wrapped_iterable = ((pg_uri, job_id, raster_tables, resolution,
                             peak_power_per_m2, system_loss, page, page_size)
                            for page in range(0, pages))
        for res in pool.starmap(_aggregate_results_page, wrapped_iterable):
            pass

    logging.info(f"PVMAPS results loaded")


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
        buildings = _load_buildings(pg_conn, job_id, raster_tables, page, page_size)
        to_write = []

        for toid, building in buildings.items():
            try:
                panels = _aggregate_panel_data(building,
                                               job_id=job_id,
                                               pixel_fields=[t.split(".")[1] for t in raster_tables],
                                               resolution=resolution,
                                               peak_power_per_m2=peak_power_per_m2,
                                               system_loss=system_loss)
                to_write.extend(panels)
            except Exception as e:
                print(f"outdated LiDAR check failed on building {toid}:")
                print(json.dumps(building, sort_keys=True, default=str))
                raise e

        _write_results(pg_conn, job_id, to_write)
        print(f"Loaded page {page} of PVMAPS results, took {round(time.time() - start_time, 2)} s.")


def _month_field(i: int):
    """
    Convert a 0-indexed month index to the name of the field to store kWh data for
    that month.
    """
    return f"kwh_m{str(i + 1).zfill(2)}"


def _aggregate_panel_data(building,
                          job_id: int,
                          pixel_fields: List[str],
                          resolution: float,
                          peak_power_per_m2: float,
                          system_loss: float,
                          debug: bool = False) -> List[dict]:
    """
    Convert pixel-level data on monthly/yearly kWh output and horizon profile
    to panel-level facts.
    """
    pixels = building['pixels']
    panels = building['panels']

    # Pixel fields:
    kwh_year = pixel_fields[0]
    wh_month = pixel_fields[1:13]
    horizons = pixel_fields[13:]

    # create squares for each pixel:
    pixel_squares = []
    pixel_data = {}
    for p in pixels:
        ps = square(p['x'] - (resolution / 2.0), p['y'] - (resolution / 2.0), resolution)
        pixel_data[id(ps)] = p
        pixel_squares.append(ps)

    # For each panel: get the pixels that intersect and the extent to which they intersect
    # then use that as a factor to calculate panel-level data.
    rtree = STRtree(pixel_squares)
    panels_to_write = []
    for panel in panels:
        panel_geom = wkt.loads(panel['panel'])
        panel['kwh_year'] = 0
        panel['horizon'] = [0 for _ in range(len(horizons))]
        for i, wh_monthday in enumerate(wh_month):
            panel[_month_field(i)] = 0

        contributing_pixels = 0
        for pixel in rtree.query(panel_geom):
            if not pixel.intersects(panel_geom):
                continue

            contributing_pixels += 1
            pct_intersects = pixel.intersection(panel_geom).area / pixel.area
            pdata = pixel_data[id(pixel)]
            # Sum of the kwh of each pixel that intersects the panel,
            # multiplied by the proportion of the pixel that intersects the panel.
            # PVMAPS produces kWh values per pixel as if a pixel was a 1kWp panel
            # so the values are adjusted accordingly.
            # System losses are also applied here.
            factor = pct_intersects * peak_power_per_m2 * (1 - system_loss)
            panel['kwh_year'] += pdata[kwh_year] * factor

            for i, wh_monthday in enumerate(wh_month):
                # Convert a 1-day Wh to a kWh for the whole month:
                panel[_month_field(i)] += pdata[wh_monthday] * 0.001 * mdays[i + 1] * factor

            # Sum the horizon values for each slice:
            for i, h in enumerate(horizons):
                panel['horizon'][i] += pdata[h]

        if contributing_pixels > 0:
            # Average each horizon slice:
            panel['horizon'] = [h / contributing_pixels for h in panel['horizon']]
            panel['job_id'] = job_id
            panel['peak_power_per_m2'] = peak_power_per_m2
            panels_to_write.append(panel)
        else:
            print(f"Panel intersected no pixels: panel_id {panel['panel_id']}, toid {panel['toid']}")

    # TODO roof-level horizons
    return panels_to_write


def _write_results(pg_conn, job_id: int, panels: List[dict]):
    with pg_conn.cursor() as cursor:
        psycopg2.extras.execute_values(
            cursor,
            SQL("""
                INSERT INTO models.pv_panel2 (
                    toid, 
                    roof_plane_id,
                    panel_id,
                    job_id, 
                    panel_geom_4326,
                    kwh_jan,
                    kwh_feb,
                    kwh_mar,
                    kwh_apr,
                    kwh_may,
                    kwh_jun,
                    kwh_jul,
                    kwh_aug,
                    kwh_sep,
                    kwh_oct,
                    kwh_nov,
                    kwh_dec,
                    kwh_year,
                    kwp,
                    horizon,
                    area,
                    footprint
                ) VALUES %s
            """).format(
                buildings=Identifier(tables.schema(job_id), tables.BUILDINGS_TABLE),
            ),
            argslist=panels,
            template="""
                %(toid)s, 
                %(roof_plane_id)s, 
                %(panel_id)s, 
                %(job_id)s, 
                ST_SetSrid(
                    ST_Transform(%(panel)s,
                                 '+proj=tmerc +lat_0=49 +lon_0=-2 +k=0.9996012717 +x_0=400000 '
                                 '+y_0=-100000 +datum=OSGB36 +nadgrids=OSTN15_NTv2_OSGBtoETRS.gsb +units=m +no_defs',
                                 4326),
                    4326)::geometry(polygon, 4326),
                %(kwh_m01)s, 
                %(kwh_m02)s, 
                %(kwh_m03)s, 
                %(kwh_m04)s, 
                %(kwh_m05)s, 
                %(kwh_m06)s, 
                %(kwh_m07)s, 
                %(kwh_m08)s, 
                %(kwh_m09)s, 
                %(kwh_m10)s, 
                %(kwh_m11)s, 
                %(kwh_m12)s, 
                %(kwh_year)s, 
                %(area)s * %(peak_power_per_m2)s, 
                %(horizon)s, 
                %(area)s, 
                %(footprint)s, 
                """)
        pg_conn.commit()


def _load_buildings(pg_conn, job_id: int, raster_tables: List[str], page: int, page_size: int, toids: List[str] = None) -> Dict[str, dict]:
    if toids:
        toid_filter = SQL("WHERE b.toid = ANY({toids})").format(toids=Literal(toids))
    else:
        toid_filter = SQL("")

    panels = sql_command(
        pg_conn,
        """        
        WITH building_page AS (
            SELECT b.toid
            FROM {buildings} b
            {toid_filter}
            ORDER BY b.toid
            OFFSET %(offset)s LIMIT %(limit)s
        )
        SELECT
            pp.panel_id,
            pp.roof_plane_id,
            pp.toid,
            ST_AsText(pp.panel_geom_27700) AS panel,
            area,
            footprint
        FROM building_page b 
        INNER JOIN {roof_polygons} rp ON b.toid = rp.toid
        INNER JOIN {panel_polygons} pp ON pp.roof_plane_id = rp.roof_plane_id
        WHERE rp.usable
        ORDER BY toid;
        """,
        {
            "offset": page * page_size,
            "limit": page_size,
        },
        roof_polygons=Identifier(tables.schema(job_id), tables.ROOF_POLYGON_TABLE),
        panel_polygons=Identifier(tables.schema(job_id), tables.PANEL_POLYGON_TABLE),
        buildings=Identifier(tables.schema(job_id), tables.BUILDINGS_TABLE),
        toid_filter=toid_filter,
        result_extractor=lambda rows: [dict(row) for row in rows])

    pixels_by_toid = pixels_for_buildings(pg_conn, job_id, page, page_size, raster_tables, toids)

    by_toid = {}
    for panel in panels:
        toid = panel['toid']
        if toid not in by_toid:
            by_toid[toid] = {'panels': [panel], 'pixels': pixels_by_toid.get(toid, [])}
        else:
            by_toid[toid]['panels'].append(panel)

    return by_toid
