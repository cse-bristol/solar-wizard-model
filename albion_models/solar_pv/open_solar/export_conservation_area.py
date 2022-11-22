import logging
import os

from psycopg2.sql import Identifier, SQL

from albion_models.db_funcs import command_to_gpkg
from albion_models.solar_pv.open_solar.mapshaper import ms_simplify

_CONS_AREAS = "conservation_areas"

_UID_SQL = " SUBSTR(country::text, 1, 1) || ':' || uid "

_EXPORT_SELECT: SQL = SQL("""
SELECT 
 {_UID_SQL} AS uid, 
 c.name, 
 c.date_of_designation, 
 c.date_updated, 
 c.capture_scale, 
 c.local_planning_authority, 
 c.geom_4326 AS geom, 
 ST_AsGeoJSON(c.geom_4326) as geom_str, 
 s.geojson as geom_str_simplified 
FROM conservation_areas.conservation_areas c 
JOIN conservation_areas.simple_conservation_areas s ON (s.id = {_UID_SQL})
""".replace("\n", " ")).format(_UID_SQL=SQL(_UID_SQL))


def export(pg_conn, pg_uri: str, gpkg_fname: str, regenerate: bool):
    if regenerate or not os.path.isfile(gpkg_fname):
        ms_simplify(
            pg_conn,
            Identifier("conservation_areas", "simple_conservation_areas"),
            "FROM conservation_areas.conservation_areas",
            _UID_SQL,
            Identifier("geom_4326"))

        if command_to_gpkg(
            pg_conn, pg_uri, gpkg_fname, _CONS_AREAS,
            src_srs=4326, dst_srs=4326,
            overwrite=True,
            command=_EXPORT_SELECT.as_string(pg_conn),
        ) is not None:
            raise RuntimeError(f"Error running ogr2ogr")
    else:
        logging.info(f"Not regenerating existing {gpkg_fname}")
