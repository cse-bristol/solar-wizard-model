# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
"""Building input seam: load caller-supplied buildings into the per-job schema."""
import logging
from dataclasses import dataclass
from typing import Iterable, Optional, Union

from psycopg2.extras import execute_values
from psycopg2.sql import Identifier, SQL
from shapely.geometry.base import BaseGeometry

from solar_pv import tables
from solar_pv.db_funcs import connection, sql_command


@dataclass
class BuildingInput:
    """A single building to model. Geometry is EPSG:27700 (the model's internal CRS)."""
    building_id: str
    geom_27700: Union[str, BaseGeometry]
    """WKT string or shapely geometry in EPSG:27700."""
    height: Optional[float] = None
    """Building height, used to patch the elevation raster where LiDAR is outdated."""


def load_buildings(pg_uri: str, job_id: int, buildings: Iterable[BuildingInput]) -> None:
    """
    Populate the per-job `buildings` table from the caller-supplied buildings, then
    derive the job bounds (`bounds_27700`) from their extent.

    Idempotent: if the buildings table has already been populated (e.g. a resumed run
    in debug mode) this is a no-op.
    """
    buildings_table = Identifier(tables.schema(job_id), tables.BUILDINGS_TABLE)
    bounds_table = Identifier(tables.schema(job_id), tables.BOUNDS_TABLE)

    with connection(pg_uri) as pg_conn:
        already_loaded = sql_command(
            pg_conn,
            "SELECT COUNT(*) FROM {buildings}",
            buildings=buildings_table,
            result_extractor=lambda rows: rows[0][0])
        if already_loaded > 0:
            logging.info("Buildings already loaded, skipping")
            return

        rows = [(b.building_id, _to_wkt(b.geom_27700), b.height) for b in buildings]

        # exclusion_reason (set by _mark_buildings_too_small / the lidar checks) and
        # min/max_ground_height (computed by outdated_lidar_check) are left NULL here.
        with pg_conn.cursor() as cursor:
            execute_values(
                cursor,
                SQL("""
                    INSERT INTO {buildings} (building_id, geom_27700, height) VALUES %s
                """).format(buildings=buildings_table),
                argslist=rows,
                template="(%s, ST_GeomFromText(%s, 27700), %s)")
        pg_conn.commit()

        logging.info(f"Loaded {len(rows)} buildings")

        # The 5m 'moat' used to detect outdated LiDAR:
        sql_command(
            pg_conn,
            "UPDATE {buildings} SET geom_27700_buffered_5 = "
            "ST_Buffer(geom_27700, 5, 'endcap=square join=mitre quad_segs=2')",
            buildings=buildings_table)

        sql_command(
            pg_conn,
            """
            INSERT INTO {bounds_27700} (job_id, bounds_27700)
            SELECT %(job_id)s,
                   ST_Multi(ST_SetSRID(ST_Extent(geom_27700)::geometry, 27700))::geometry(multipolygon, 27700)
            FROM {buildings};
            """,
            {"job_id": job_id},
            bounds_27700=bounds_table,
            buildings=buildings_table)


def _to_wkt(geom: Union[str, BaseGeometry]) -> str:
    if isinstance(geom, BaseGeometry):
        return geom.wkt
    return geom
