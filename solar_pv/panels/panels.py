# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import logging
import time
from typing import List, Tuple
import multiprocessing as mp

import math
import psycopg2.extras
from psycopg2.sql import Identifier, SQL
from shapely import affinity, wkt
from shapely.errors import ShapelyError
from shapely.geometry import MultiPolygon, Polygon
from shapely.validation import make_valid

import tables as tables
from solar_pv.db_funcs import sql_command, connection, count
from solar_pv.geos import get_grid_cells, largest_polygon
from solar_pv.util import get_cpu_count


def _panel_placement_cpu_count():
    """Use 3/4s of available CPUs for panel placement"""
    return min(int(get_cpu_count() * 0.75), 100)


def place_panels(pg_uri: str,
                 job_id: int,
                 panel_width_m: float,
                 panel_height_m: float,
                 panel_spacing_m: float,
                 min_roof_area_m: float,
                 workers: int = _panel_placement_cpu_count(),
                 page_size: int = 3000):
    schema = tables.schema(job_id)

    panel_polygon_count = count(pg_uri, schema, tables.PANEL_POLYGON_TABLE)
    if panel_polygon_count > 0:
        logging.info("Not adding PV panels, panels already added")
        return

    logging.info(f"Placing panels using {workers} parallel processes...")

    roof_polygon_count = _roof_polygon_count(pg_uri, schema)
    pages = math.ceil(roof_polygon_count / page_size)
    logging.info(f"{roof_polygon_count} roof polygons, in {pages} batches to process")

    with mp.get_context("spawn").Pool(workers) as pool:
        wrapped_iterable = ((pg_uri, job_id, panel_width_m, panel_height_m, panel_spacing_m, page, page_size)
                            for page in range(0, pages))
        for res in pool.starmap(_place_panel_page, wrapped_iterable):
            pass

    logging.info(f"Panels placed, finalising...")
    _post_load(pg_uri, schema, min_roof_area_m)

    logging.info("Finished placing panels")


def _roof_polygon_count(pg_uri: str, schema: str) -> int:
    with connection(pg_uri) as pg_conn:
        return sql_command(
            pg_conn, "SELECT COUNT(*) FROM {roof_polygons} rp WHERE rp.usable",
            roof_polygons=Identifier(schema, tables.ROOF_POLYGON_TABLE),
            result_extractor=lambda rows: rows[0][0])


def _post_load(pg_uri: str, schema: str, min_roof_area_m: float):
    with connection(pg_uri, cursor_factory=psycopg2.extras.DictCursor) as pg_conn:
        sql_command(
            pg_conn,
            """
            WITH pp AS (
                SELECT 
                    roof_plane_id,
                    SUM(area) AS area,
                    COUNT(*) AS num_panels
                FROM {panel_polygons}
                GROUP BY roof_plane_id)
            UPDATE {roof_polygons} rp
            SET usable = pp.area >= %(min_roof_area_m)s
            FROM pp
            WHERE pp.roof_plane_id = rp.roof_plane_id AND rp.usable = true
            AND (rp.archetype = false OR pp.num_panels < 3);
            
            UPDATE {roof_polygons} rp
            SET usable = false
            WHERE
                rp.usable = true
                AND NOT EXISTS (SELECT FROM {panel_polygons} pp WHERE pp.roof_plane_id = rp.roof_plane_id);
            
            -- Update building.exclusion_reason for any buildings that have roof planes but no
            -- usable ones:
            UPDATE {buildings} b
            SET exclusion_reason = 'ALL_ROOF_PLANES_UNUSABLE'
            WHERE
                NOT EXISTS (SELECT FROM {roof_polygons} rp WHERE rp.usable AND rp.toid = b.toid)
                AND b.exclusion_reason IS NULL;
            """,
            {"min_roof_area_m": min_roof_area_m},
            buildings=Identifier(schema, tables.BUILDINGS_TABLE),
            panel_polygons=Identifier(schema, tables.PANEL_POLYGON_TABLE),
            roof_polygons=Identifier(schema, tables.ROOF_POLYGON_TABLE))


def _place_panel_page(pg_uri: str,
                      job_id: int,
                      panel_width_m: float,
                      panel_height_m: float,
                      panel_spacing_m: float,
                      page: int,
                      page_size: int = 1000):
    schema = tables.schema(job_id)
    start_time = time.time()

    with connection(pg_uri, cursor_factory=psycopg2.extras.DictCursor) as pg_conn:
        roofs = sql_command(
            pg_conn,
            """
            SELECT 
                toid,
                roof_plane_id,
                st_astext(roof_geom_27700) AS roof, 
                aspect, slope, is_flat 
            FROM {roof_polygons}
            -- not really needed as none of them should be null, but oh well:
            WHERE roof_geom_27700 IS NOT NULL 
            AND usable
            ORDER BY roof_plane_id
            OFFSET %(offset)s
            LIMIT %(limit)s
            """,
            bindings={"offset": page * page_size,
                      "limit": page_size},
            result_extractor=lambda rows: rows,
            roof_polygons=Identifier(schema, tables.ROOF_POLYGON_TABLE))

        roof_panels = []
        for roof in roofs:
            try:
                panels = _roof_panels(
                    roof=wkt.loads(roof['roof']),
                    panel_w=panel_width_m,
                    panel_h=panel_height_m,
                    aspect=roof['aspect'],
                    slope=roof['slope'],
                    panel_spacing_m=panel_spacing_m,
                    is_flat=roof['is_flat'])
            except ShapelyError as e:
                print(f"Error on panel placement for roof plane: {roof['roof_plane_id']}")
                print(roof['roof'])
                raise e

            if panels:
                roof_plane_id = roof['roof_plane_id']
                toid = roof['toid']
                for panel in panels:
                    area = panel_width_m * panel_height_m
                    footprint = panel.area
                    roof_panels.append((roof_plane_id,
                                        toid,
                                        panel.wkt,
                                        area,
                                        footprint))

        _write_panels(pg_conn, job_id, roof_panels)
        print(f"Finished panels page {page}, took {round(time.time() - start_time, 2)} s.")


def _write_panels(pg_conn, job_id: int, roofs: List[Tuple[str, str, str, float, float]]):
    schema = tables.schema(job_id)

    with pg_conn.cursor() as cursor:
        psycopg2.extras.execute_values(
            cursor,
            SQL("""
                INSERT INTO {panel_polygons}
                (roof_plane_id, toid, panel_geom_27700, area, footprint)
                VALUES %s
            """).format(
                panel_polygons=Identifier(schema, tables.PANEL_POLYGON_TABLE),
            ), argslist=roofs)
        pg_conn.commit()


def _panels_on_roof(rotated_roof: Polygon, panel_grid: List[Polygon], xoff: float, yoff: float):
    panels = []
    for panel in panel_grid:
        panel_var = affinity.translate(panel, xoff, yoff)
        if panel_var.within(rotated_roof):
            panels.append(panel_var)
    return panels


def _roof_panels(roof: MultiPolygon,
                 panel_w: float,
                 panel_h: float,
                 aspect: float,
                 slope: float,
                 is_flat: bool,
                 panel_spacing_m: float):
    """
    Core roof panel placement algorithm
    """
    roof: Polygon = largest_polygon(roof)
    if roof is None:
        return None
    slope_rads = math.radians(slope)
    sun_angle_for_spacing_calc = math.radians(15)

    # We are working with a birds-eye view, so panels need shortening according to the
    # slope they are on:
    portrait_panel_w = panel_w
    portrait_panel_h = panel_h * math.cos(slope_rads)
    landscape_panel_w = panel_h
    landscape_panel_h = panel_w * math.cos(slope_rads)

    # Panels on flat roofs need a space between each South-facing row so that the
    # row in front does not block the one behind.
    # Panels on flat roofs will always be mounted landscape
    # (on their sides) as this makes the frames and ballast required easier
    spacing_x = panel_spacing_m
    if is_flat:
        spacing_y = (math.sin(slope_rads) * landscape_panel_h) / math.tan(sun_angle_for_spacing_calc)
    else:
        spacing_y = panel_spacing_m

    # Rotate the roof area CCW by aspect, to be gridded easily:
    centroid = roof.centroid
    rotated_roof = affinity.rotate(roof, aspect, origin=centroid)
    rotated_roof = largest_polygon(make_valid(rotated_roof))
    if rotated_roof is None:
        return None

    # Define grids of portrait and landscape panels:
    portrait_grid = get_grid_cells(rotated_roof.buffer(portrait_panel_h), portrait_panel_w, portrait_panel_h, spacing_x, spacing_y, grid_start='bounds')
    landscape_grid = get_grid_cells(rotated_roof.buffer(landscape_panel_w), landscape_panel_w, landscape_panel_h, spacing_x, spacing_y, grid_start='bounds')

    # Define some variations on panel row positioning to try and fit
    # more panels on each roof:
    variations = [
        (0,                        0),
        (-portrait_panel_w * 0.5,  0),
        (0,                        -portrait_panel_h * 0.5),
        (-portrait_panel_w * 0.5,  -portrait_panel_h * 0.5),
        (-portrait_panel_w * 0.33, 0),
        (0,                        -portrait_panel_h * 0.33),
        (-portrait_panel_w * 0.33, -portrait_panel_h * 0.33),
        (-portrait_panel_w * 0.66, 0),
        (0,                        -portrait_panel_h * 0.66),
        (-portrait_panel_w * 0.66, -portrait_panel_h * 0.66)]

    # Try each variation defined above and find the best:
    best_var = None
    panel_count = 0
    for xoff, yoff in variations:
        # Panels on flat roofs will always be mounted landscape
        # as this makes the frames and ballast required easier:
        if not is_flat:
            pg_var = _panels_on_roof(rotated_roof, portrait_grid, xoff, yoff)
            if len(pg_var) > panel_count:
                panel_count = len(pg_var)
                best_var = pg_var

        lg_var = _panels_on_roof(rotated_roof, landscape_grid, xoff, yoff)
        if len(lg_var) > panel_count:
            panel_count = len(lg_var)
            best_var = lg_var

    if best_var:
        return affinity.rotate(MultiPolygon(best_var), -aspect, origin=centroid).geoms
    else:
        return None
