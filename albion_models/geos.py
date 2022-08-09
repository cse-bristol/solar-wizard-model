from typing import List

from shapely.strtree import STRtree

from albion_models.db_funcs import sql_command

from shapely import wkt

from albion_models.lidar.en_to_grid_ref import en_to_grid_ref
from albion_models.util import round_down_to, round_up_to


def square(x: int, y: int, edge: int):
    return wkt.loads(
        f'POLYGON(({x} {y}, '
        f'{x} {y + edge}, '
        f'{x + edge} {y + edge}, '
        f'{x + edge} {y}, '
        f'{x} {y}))')


def bounds_polygon(pg_conn, job_id: int):
    """
    Returns a shapely polygon ofthe job bounds, which will be buffered
    by the horizon_search_distance if it's a PV job.
    """
    text = sql_command(
        pg_conn,
        """
        SELECT 
           ST_AsText(ST_Buffer(bounds, coalesce((params->>'horizon_search_radius')::int, 0))) AS bounds
        FROM models.job_queue
        WHERE job_id = %(job_id)s 
        """,
        bindings={"job_id": job_id},
        result_extractor=lambda res: res[0][0]
    )
    return wkt.loads(text)


def get_grid_refs(poly, cell_size: int) -> List[str]:
    """
    Get grid regs (in the same format that the LiDAR filenames use:
    e.g. SV54ne, or SM66) of the bottom left (SW) corner of each grid ref tile
    that intersects the polygon (which should be in srid 27700)
    """
    e_min, n_min, e_max, n_max = poly.bounds
    e_min = round_down_to(e_min, cell_size)
    n_min = round_down_to(n_min, cell_size)
    e_max = round_up_to(e_max, cell_size)
    n_max = round_up_to(n_max, cell_size)

    cells = []
    for easting in range(e_min, e_max, cell_size):
        for northing in range(n_min, n_max, cell_size):
            grid_ref = en_to_grid_ref(easting, northing, cell_size)
            cell = square(easting, northing, cell_size)
            cell.grid_ref = grid_ref
            cells.append(cell)
    rtree = STRtree(cells)
    grid_refs = [p.grid_ref for p in rtree.query(poly)]
    return grid_refs
