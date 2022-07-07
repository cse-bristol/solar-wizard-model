import logging
from psycopg2.sql import Identifier

import albion_models.solar_pv.tables as tables
from albion_models.db_funcs import connect, sql_script_with_bindings, count


def add_panels(pg_uri: str,
               job_id: int,
               min_roof_area_m: int,
               panel_width_m: float,
               panel_height_m: float,
               panel_spacing_m: float):
    schema = tables.schema(job_id)

    if count(pg_uri, schema, tables.PANEL_POLYGON_TABLE) > 0:
        logging.info("Not adding PV panels, panels already added")
        return

    pg_conn = connect(pg_uri)

    try:
        sql_script_with_bindings(
            pg_conn, 'pv/create.panels.sql',
            {
                "job_id": job_id,
                "min_roof_area_m": min_roof_area_m,
                "panel_width_m": panel_width_m,
                "panel_height_m": panel_height_m,
                "panel_spacing_m": panel_spacing_m,
            },
            roof_polygons=Identifier(schema, tables.ROOF_POLYGON_TABLE),
            panel_polygons=Identifier(schema, tables.PANEL_POLYGON_TABLE),
            building_exclusion_reasons=Identifier(schema, tables.BUILDING_EXCLUSION_REASONS_TABLE),
        )
    finally:
        pg_conn.close()
