from dataclasses import dataclass
from typing import List, Optional

from shapely import ops, affinity
from shapely.geometry import Polygon

from solar_model.geos import rect


@dataclass
class Archetype:
    pattern: List[List[int]]
    polygon: Polygon
    portrait: bool


def _deep_copy(pattern: List[List[int]]):
    return [row.copy() for row in pattern]


def _pattern_variations(pattern: List[List[int]]):
    """
    Generate some variations on a pattern (cutting each corner off;
    cutting combinations of corners off; cutting larger corners off)
    """
    t = _deep_copy(pattern)
    t[0][0] = 0
    t[0][-1] = 0

    tl = _deep_copy(pattern)
    tl[0][0] = 0

    tr = _deep_copy(pattern)
    tr[0][-1] = 0

    b = _deep_copy(pattern)
    b[-1][0] = 0
    b[-1][-1] = 0

    bl = _deep_copy(pattern)
    bl[-1][0] = 0

    br = _deep_copy(pattern)
    br[-1][-1] = 0

    if len(pattern) < 3 or len(pattern[0]) < 3:
        return [pattern, t, tl, tr, b, bl, br]

    ttl = _deep_copy(pattern)
    ttl[0][0] = 0
    ttl[1][0] = 0
    ttl[0][1] = 0

    ttr = _deep_copy(pattern)
    ttr[0][-1] = 0
    ttr[1][-1] = 0
    ttr[0][-2] = 0

    bbl = _deep_copy(pattern)
    bbl[-1][0] = 0
    bbl[-2][0] = 0
    bbl[-1][1] = 0

    bbr = _deep_copy(pattern)
    bbr[-1][-1] = 0
    bbr[-2][-1] = 0
    bbr[-1][-2] = 0

    return [pattern, t, tl, tr, b, bl, br, ttl, ttr, bbl, bbr]


# Each premade represents a standard panel layout to test against
# a roof polygon to see if it is a similar-enough shape:
ARCHETYPE_PATTERNS = [
    [[1, 1, 1]],
    [[1, 1, 1, 1]],
    [[1, 1, 1, 1, 1]],
    [[1, 1, 1, 1, 1, 1]],
    [[1, 1, 1, 1, 1, 1, 1]],
    [[1, 1, 1, 1, 1, 1, 1, 1]],
    [[1, 1, 1, 1, 1, 1, 1, 1, 1]],

    [[1, 1],
     [1, 1]],

    [[1, 1, 1],
     [1, 1, 1]],

    [[1, 1, 1],
     [0, 1, 1]],

    [[1, 1, 1],
     [1, 1, 0]],

    [[0, 1, 0],
     [1, 1, 1]],

    [[1, 1],
     [1, 1],
     [1, 1]],

    [[1, 1, 1, 1, 1, 1],
     [1, 1, 0, 0, 1, 1]],

    [[1, 1, 1, 1, 1, 1, 1],
     [1, 1, 0, 0, 0, 1, 1]],

    [[1, 1, 1, 1, 1, 1, 1, 1],
     [1, 1, 0, 0, 0, 0, 1, 1]],

    [[1, 1, 1, 1, 1, 1],
     [1, 1, 1, 1, 1, 1],
     [1, 1, 0, 0, 1, 1]],

    [[1, 1, 1, 1, 1, 1],
     [1, 1, 0, 0, 1, 1],
     [1, 0, 0, 0, 0, 1]],

    [[0, 1, 1, 1, 0],
     [1, 1, 1, 1, 1],
     [1, 1, 0, 1, 1],
     [1, 0, 0, 0, 1]],

    [[1, 1, 1, 1, 1],
     [1, 1, 1, 1, 1],
     [1, 1, 0, 1, 1],
     [1, 0, 0, 0, 1]],
] + _pattern_variations(
    [[1, 1, 1, 1],
     [1, 1, 1, 1]]
) + _pattern_variations(
    [[1, 1, 1, 1, 1],
     [1, 1, 1, 1, 1]],
) + _pattern_variations(
    [[1, 1, 1, 1, 1, 1],
     [1, 1, 1, 1, 1, 1]],
) + _pattern_variations(
    [[1, 1, 1, 1, 1, 1, 1],
     [1, 1, 1, 1, 1, 1, 1]],
) + _pattern_variations(
    [[1, 1, 1, 1, 1, 1, 1, 1],
     [1, 1, 1, 1, 1, 1, 1, 1]],
) + _pattern_variations(
    [[1, 1, 1],
     [1, 1, 1],
     [1, 1, 1]]
) + _pattern_variations(
    [[1, 1, 1, 1],
     [1, 1, 1, 1],
     [1, 1, 1, 1]],
) + _pattern_variations(
    [[1, 1, 1, 1],
     [1, 1, 1, 1],
     [1, 1, 1, 1],
     [1, 1, 1, 1]],
) + _pattern_variations(
    [[1, 1, 1, 1, 1],
     [1, 1, 1, 1, 1],
     [1, 1, 1, 1, 1]],
) + _pattern_variations(
    [[1, 1, 1, 1, 1, 1],
     [1, 1, 1, 1, 1, 1],
     [1, 1, 1, 1, 1, 1]],
) + _pattern_variations(
    [[1, 1, 1, 1, 1],
     [1, 1, 1, 1, 1],
     [1, 1, 1, 1, 1],
     [1, 1, 1, 1, 1]],
)


def construct_archetype(pattern, panel_w: float, panel_h: float, portrait: bool) -> Archetype:
    """
    Construct a pre-made roof polygon archetype from a pattern and some info about panels
    """
    cells = []

    for y in range(0, len(pattern)):
        row = pattern[y]
        for x in range(0, len(row)):
            if row[x] == 1:
                if portrait:
                    cells.append(rect(x * panel_w, y * panel_h, panel_w, panel_h))
                else:
                    cells.append(rect(x * panel_h, y * panel_w, panel_h, panel_w))

    premade = ops.unary_union(cells)
    return Archetype(
        pattern=pattern,
        polygon=affinity.translate(premade, -premade.centroid.x, -premade.centroid.y),
        portrait=portrait)


def construct_archetypes(panel_w: float, panel_h: float) -> List[Archetype]:
    """
    Construct pre-made roof polygon archetypes from patterns and some info about panels
    """
    premades = []
    for pattern in ARCHETYPE_PATTERNS:
        premades.append(construct_archetype(pattern, panel_w, panel_h, portrait=True))
        premades.append(construct_archetype(pattern, panel_w, panel_h, portrait=False))
    premades.sort(key=lambda p: -p.polygon.area)
    return premades


def _weighted_pct_sym_difference(p1: Polygon, p2: Polygon, w1: float, w2: float) -> float:
    """
    % difference in area between p1 and p2, with w1 applied as a weight to the area of
    p1 that does not intersect p2, and w2 applied as a weight to the area of p2 that does
    not intersect p1.
    """
    a1 = p1.difference(p2).area * w1
    a2 = p2.difference(p1).area * w2
    area_diff = a1 + a2
    pct_diff = area_diff / p1.area
    return pct_diff


# Various magic numbers reached via trial and error:

# Don't use an archetype that is this much bigger or smaller than the roof polygon:
_MAX_ABS_AREA_DIFF = 4.0
# Don't use an archetype whose weighted symmetric % difference from the roof polygon is >= this:
_MIN_PCT_AREA_DIFF = 0.68
# weight for parts of roof_poly that do not intersect archetype (not such a problem - bits of
# roof_poly are sticking out the sides of archetype):
_PCT_SYM_DIFF_WEIGHT_1 = 0.75
# weight for the parts of archetype that do not intersect roof poly (this is worse -
# archetype is sticking out the sides of roof_poly here - so make it count more):
_PCT_SYM_DIFF_WEIGHT_2 = 1.8


def get_archetype(roof_polygon: Polygon, archetypes: List[Archetype], aspect) -> Optional[Archetype]:
    min_diff = _MIN_PCT_AREA_DIFF
    best_archetype = None
    ra = roof_polygon.area
    rx = roof_polygon.centroid.x
    ry = roof_polygon.centroid.y

    prepared_archetypes = []
    for archetype in archetypes:
        p = archetype.polygon
        # skip any that are way too big or too small:
        if abs(p.area - ra) >= _MAX_ABS_AREA_DIFF:
            continue
        # move the archetype so the centroid is the same as the existing poly:
        p = affinity.translate(p, rx, ry)
        # rotate to match the aspect:
        p = affinity.rotate(p, -aspect, origin=p.centroid)
        prepared_archetypes.append(Archetype(polygon=p, pattern=archetype.pattern, portrait=archetype.portrait))

    for archetype in prepared_archetypes:
        arch_poly = archetype.polygon
        pct_diff = _weighted_pct_sym_difference(roof_polygon, arch_poly, _PCT_SYM_DIFF_WEIGHT_1, _PCT_SYM_DIFF_WEIGHT_2)

        if arch_poly.difference(roof_polygon).area > _MAX_ABS_AREA_DIFF:
            continue
        if pct_diff < min_diff:
            min_diff = pct_diff
            best_archetype = archetype
        if best_archetype and round(pct_diff, 2) == round(min_diff, 2) and arch_poly.area > best_archetype.polygon.area:
            min_diff = pct_diff
            best_archetype = archetype

    if best_archetype:
        return best_archetype

    # If we didn't find one, try again, but only scoring archetypes based on how much
    # they fit within the roof plane.
    # This relies on the list of archetypes being ordered by area descending, as otherwise
    # the smallest one might be checked first and always win.
    for archetype in prepared_archetypes:
        arch_poly = archetype.polygon
        area_diff = arch_poly.difference(roof_polygon).area
        pct_diff = area_diff / ra

        if pct_diff < min_diff:
            min_diff = pct_diff
            best_archetype = archetype
        if best_archetype and round(pct_diff, 2) == round(min_diff, 2) and arch_poly.area > best_archetype.polygon.area:
            min_diff = pct_diff
            best_archetype = archetype

    return best_archetype


def _write_archetypes(name: str, archetypes: List[Polygon]):
    from shapely import geometry
    import json

    geojson_features = []
    for a in archetypes:
        geojson_geom = geometry.mapping(a)
        geojson_feature = {
          "type": "Feature",
          "geometry": geojson_geom,
        }
        geojson_features.append(geojson_feature)

    geojson = {
        "type": "FeatureCollection",
        "crs": {"type": "name", "properties": {"name": "urn:ogc:def:crs:EPSG::27700"}},
        "features": geojson_features
    }
    with open(f"{name}.geojson", 'w') as f:
        json.dump(geojson, f)
