import json
from typing import List, Tuple, Union

import math
from shapely.strtree import STRtree
from shapely.geometry import Polygon, shape, MultiPolygon
from shapely import wkt

from albion_models.db_funcs import sql_command


from albion_models.lidar.en_to_grid_ref import en_to_grid_ref
from albion_models.util import round_down_to, round_up_to, frange


def rect(x: float, y: float, w: float, h: float) -> Polygon:
    return Polygon([(x, y),
                    (x, y + h),
                    (x + w, y + h),
                    (x + w, y),
                    (x, y)])


def square(x: float, y: float, edge: float) -> Polygon:
    return rect(x, y, edge, edge)


def from_geojson(geojson):
    if isinstance(geojson, str):
        geojson = json.loads(geojson)
    return shape(geojson)


def from_geojson_file(geojson_file: str):
    with open(geojson_file) as f:
        return from_geojson(f.read())


def bounds_polygon(pg_conn, job_id: int) -> Polygon:
    """
    Returns a shapely polygon of the job bounds, which will be buffered
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


def get_grid_cells(poly, cell_w, cell_h, spacing_w=0, spacing_h=0, grid_start: str = 'origin') -> List[Polygon]:
    """
    Get the cells of a grid that intersect with `poly` as polygons

    param `grid_start` == 'origin' means that the grid would intersect
    (0,0) if extended (useful for making OS grid refs). Otherwise
    if `grid_start` == 'bounds' grid starts at (xmin, ymin) of poly.
    """
    xmin, ymin, xmax, ymax = poly.bounds
    if grid_start == 'origin':
        xmin = round_down_to(xmin, cell_w + spacing_w)
        ymin = round_down_to(ymin, cell_h + spacing_h)
        xmax = round_up_to(xmax, cell_w + spacing_w)
        ymax = round_up_to(ymax, cell_h + spacing_h)
    elif grid_start == 'bounds-buffered':
        # add a 1-cell buffer:
        xmin -= cell_w + spacing_w
        ymin -= cell_h + spacing_h
        xmax += cell_w + spacing_w
        ymax += cell_h + spacing_h
    elif grid_start != 'bounds':
        raise ValueError(f"Unrecognised grid_start: {grid_start}")

    cells = []
    for x in frange(xmin, xmax, cell_w + spacing_w):
        for y in frange(ymin, ymax, cell_h + spacing_h):
            cells.append(rect(x, y, cell_w, cell_h))
    rtree = STRtree(cells)
    return [p for p in rtree.query(poly) if p.intersects(poly)]


def get_grid_refs(poly, cell_size: int) -> List[str]:
    """
    Get grid regs (in the same format that the LiDAR filenames use:
    e.g. SV54ne, or SM66) of the bottom left (SW) corner of each grid ref tile
    that intersects the polygon (which should be in srid 27700)
    """
    grid_refs = []
    for cell in get_grid_cells(poly, cell_size, cell_size):
        x, y, _, _ = cell.bounds
        grid_refs.append(en_to_grid_ref(x, y, cell_size))
    return grid_refs


def largest_polygon(multi: Union[MultiPolygon, Polygon]):
    if multi.type == 'Polygon':
        return multi
    return max(multi.geoms, key=lambda poly: poly.area)


def azimuth(p1: Tuple[float, float], p2: Tuple[float, float]) -> float:
    angle = math.atan2(p2[0] - p1[0], p2[1] - p1[1])
    return math.degrees(angle) if angle > 0 else math.degrees(angle) + 180
