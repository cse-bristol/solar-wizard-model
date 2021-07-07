import logging
from typing import List
from psycopg2.sql import SQL, Identifier

import albion_models.solar_pv.tables as tables
from albion_models.db_funcs import connect, sql_script_with_bindings, count


def aggregate_horizons(pg_uri: str,
                       job_id: int,
                       horizon_slices: int,
                       max_roof_slope_degrees: int,
                       min_roof_area_m: int,
                       min_roof_degrees_from_north: int,
                       flat_roof_degrees: int,
                       max_avg_southerly_horizon_degrees: int,
                       panel_width_m: float,
                       panel_height_m: float,
                       resolution_metres: float):
    schema = tables.schema(job_id)

    if count(pg_uri, schema, tables.PANEL_HORIZON_TABLE) > 0:
        logging.info("Not aggregating horizon info, horizons already aggregated.")
        return

    pg_conn = connect(pg_uri)
    aggregated_horizon_cols = _aggregated_horizon_cols(horizon_slices, 'avg')

    try:
        sql_script_with_bindings(
            pg_conn, 'create.roof-horizons.sql',
            {
                "job_id": job_id,
                "max_roof_slope_degrees": max_roof_slope_degrees,
                "min_roof_area_m": min_roof_area_m,
                "min_roof_degrees_from_north": min_roof_degrees_from_north,
                "flat_roof_degrees": flat_roof_degrees,
                "max_avg_southerly_horizon_degrees": max_avg_southerly_horizon_degrees,
                "panel_width_m": panel_width_m,
                "panel_height_m": panel_height_m,
                "resolution": resolution_metres,
            },
            schema=Identifier(schema),
            pixel_horizons=Identifier(schema, tables.PIXEL_HORIZON_TABLE),
            roof_planes=Identifier(schema, tables.ROOF_PLANE_TABLE),
            roof_horizons=Identifier(schema, tables.ROOF_HORIZON_TABLE),
            panel_horizons=Identifier(schema, tables.PANEL_HORIZON_TABLE),
            buildings=Identifier(schema, tables.BUILDINGS_TABLE),
            aggregated_horizon_cols=SQL(aggregated_horizon_cols),
            avg_southerly_horizon_rads=SQL(_avg_southerly_horizon_rads(horizon_slices)),
            horizon_cols=SQL(','.join(_horizon_cols(horizon_slices))),
            southerly_horizon_cols=SQL(','.join(_southerly_horizon_cols(horizon_slices))),
        )
    finally:
        pg_conn.close()


def aggregate_user_submitted_polygon_horizons(pg_uri: str,
                                              job_id: int,
                                              horizon_slices: int,
                                              flat_roof_degrees: int,
                                              aggregate_fn: str):
    pg_conn = connect(pg_uri)
    schema = tables.schema(job_id)
    aggregated_horizon_cols = _aggregated_horizon_cols(horizon_slices, aggregate_fn)

    try:
        sql_script_with_bindings(
            pg_conn, 'create.user-submitted-polygon-horizons.sql',
            {
                "job_id": job_id,
                "flat_roof_degrees": flat_roof_degrees,
            },
            schema=Identifier(schema),
            pixel_horizons=Identifier(schema, tables.PIXEL_HORIZON_TABLE),
            roof_horizons=Identifier(schema, tables.ROOF_HORIZON_TABLE),
            aggregated_horizon_cols=SQL(aggregated_horizon_cols),
        )
    finally:
        pg_conn.close()


def _aggregated_horizon_cols(horizon_slices: int, aggregate_fn: str) -> str:
    horizon_slices = int(horizon_slices)
    if aggregate_fn not in ("avg", "min", "max"):
        raise ValueError(f"Invalid horizon aggregate function '{aggregate_fn}")
    return ','.join([f'{aggregate_fn}(h.horizon_slice_{i}) AS horizon_slice_{i}' for i in range(0, horizon_slices)])


def _horizon_cols(horizon_slices: int) -> List[str]:
    horizon_slices = int(horizon_slices)
    return [f'h.horizon_slice_{i}' for i in range(0, horizon_slices)]


def _southerly_horizon_cols(horizon_slices: int, degrees_around_south: int = 135) -> List[str]:
    """
    Get horizon cols which count as southerly.
    By default southerly is defined as the 135 degrees between ESE and WSW inclusive.
    """
    horizon_slices = int(horizon_slices)
    degrees_around_south = int(degrees_around_south)

    centre = horizon_slices / 2  # South
    segment_size = 360 / horizon_slices
    segment_start = centre - (degrees_around_south / 2 / segment_size)
    segment_end = centre + (degrees_around_south / 2 / segment_size)
    return [f'h.horizon_slice_{i}' for i in range(0, horizon_slices) if segment_start <= i <= segment_end]


def _avg_southerly_horizon_rads(horizon_slices: int, degrees_around_south: int = 135) -> str:
    """
    Get the SQL for calculating the average southerly horizon radians.
    By default southerly is defined as the 135 degrees between ESE and WSW inclusive.
    """
    horizon_slices = int(horizon_slices)
    degrees_around_south = int(degrees_around_south)

    cols = _southerly_horizon_cols(horizon_slices, degrees_around_south)
    return f"({' + '.join(cols)}) / {len(cols)}"
