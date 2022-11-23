import logging

from psycopg2.extras import DictCursor
from psycopg2.sql import Identifier
from shapely import wkt

from albion_models.db_funcs import connection, sql_command
from albion_models.solar_pv import tables
from albion_models.solar_pv.panels.panels import _roof_panels


def load_roof_plane(pg_uri: str, job_id: int, roof_plane_id: int) -> dict:
    schema = tables.schema(job_id)

    with connection(pg_uri, cursor_factory=DictCursor) as pg_conn:
        roof = sql_command(
            pg_conn,
            """
            SELECT 
                roof_plane_id,
                st_astext(roof_geom_27700) AS roof, 
                aspect, slope, is_flat 
            FROM {roof_polygons}
            -- not really needed as none of them should be null, but oh well:
            WHERE roof_plane_id =  %(roof_plane_id)s
            ORDER BY roof_plane_id
            """,
            bindings={"roof_plane_id": roof_plane_id},
            result_extractor=lambda rows: rows[0],
            roof_polygons=Identifier(schema, tables.ROOF_POLYGON_TABLE))

        return roof


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format='[%(asctime)s] %(levelname)s: %(message)s')
    roof = load_roof_plane(
        'postgresql://albion_webapp:ydBbE3JCnJ4@localhost:5432/albion?application_name=blah',
        1617, 4564)
    panels = _roof_panels(
        roof=wkt.loads(roof['roof']),
        panel_w=0.99,
        panel_h=1.64,
        aspect=roof['aspect'],
        slope=roof['slope'],
        panel_spacing_m=0.01,
        is_flat=roof['is_flat'])
