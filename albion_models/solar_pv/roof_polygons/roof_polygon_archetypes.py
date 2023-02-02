from typing import List, Optional

from shapely import ops, affinity
from shapely.geometry import Polygon

from albion_models.geos import rect

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

    [[0, 1, 0],
     [1, 1, 1]],

    # [[0, 1, 1],
    #  [1, 1, 1]],
    #
    # [[1, 1, 0],
    #  [1, 1, 1]],

    [[0, 1, 1, 0],
     [1, 1, 1, 1]],

    # [[1, 1, 1, 0],
    #  [1, 1, 1, 1]],
    #
    # [[0, 1, 1, 1],
    #  [1, 1, 1, 1]],

    [[1, 1, 1, 1],
     [1, 1, 1, 1]],

    [[0, 1, 1, 1, 0],
     [1, 1, 1, 1, 1]],

    [[1, 1, 1, 1, 1],
     [1, 1, 1, 1, 1]],

    [[1, 1, 1, 1, 1, 1],
     [1, 1, 1, 1, 1, 1]],

    [[0, 1, 1, 1, 1, 0],
     [1, 1, 1, 1, 1, 1]],

    [[1, 1, 1, 1, 1, 1, 1],
     [1, 1, 1, 1, 1, 1, 1]],

    [[0, 1, 1, 1, 1, 1, 0],
     [1, 1, 1, 1, 1, 1, 1]],

    [[0, 0, 1, 1, 1, 0, 0],
     [1, 1, 1, 1, 1, 1, 1]],

    [[0, 1, 1, 1, 1, 1, 1, 0],
     [1, 1, 1, 1, 1, 1, 1, 1]],

    [[0, 0, 1, 1, 1, 1, 0, 0],
     [1, 1, 1, 1, 1, 1, 1, 1]],

    [[1, 1, 1, 1, 1, 1, 1, 1],
     [1, 1, 1, 1, 1, 1, 1, 1]],

    [[1, 1],
     [1, 1],
     [1, 1]],

    # [[1, 0],
    #  [1, 1],
    #  [1, 1]],
    #
    # [[0, 1],
    #  [1, 1],
    #  [1, 1]],
    #
    # [[1, 1],
    #  [1, 1],
    #  [1, 0]],
    #
    # [[1, 1],
    #  [1, 1],
    #  [0, 1]],

    # [[1, 1],
    #  [1, 1],
    #  [1, 1],
    #  [1, 1]],

    # [[1, 1],
    #  [1, 1],
    #  [1, 1],
    #  [1, 0]],
    #
    # [[1, 1],
    #  [1, 1],
    #  [1, 1],
    #  [0, 1]],

    [[0, 1, 0],
     [1, 1, 1],
     [1, 1, 1]],

    # [[1, 1, 0],
    #  [1, 1, 1],
    #  [1, 1, 1]],
    #
    # [[0, 1, 1],
    #  [1, 1, 1],
    #  [1, 1, 1]],

    [[0, 0, 1],
     [0, 1, 1],
     [1, 1, 1]],

    [[1, 0, 0],
     [1, 1, 0],
     [1, 1, 1]],

    [[1, 1, 1],
     [1, 1, 1],
     [1, 1, 1]],

    [[1, 1, 1, 1],
     [1, 1, 1, 1],
     [1, 1, 1, 1]],

    [[0, 1, 1, 0],
     [1, 1, 1, 1],
     [1, 1, 1, 1]],

    [[0, 0, 1, 1],
     [0, 1, 1, 1],
     [1, 1, 1, 1]],

    [[1, 1, 0, 0],
     [1, 1, 1, 0],
     [1, 1, 1, 1]],

    [[1, 1, 1, 1],
     [1, 1, 1, 1],
     [1, 1, 1, 1],
     [1, 1, 1, 1]],

    [[0, 1, 1, 0],
     [1, 1, 1, 1],
     [1, 1, 1, 1],
     [1, 1, 1, 1]],

    [[1, 1, 1, 1, 1],
     [1, 1, 1, 1, 1],
     [1, 1, 1, 1, 1]],

    [[0, 1, 1, 1, 0],
     [1, 1, 1, 1, 1],
     [1, 1, 1, 1, 1]],

    [[1, 1, 1, 1, 1, 1],
     [1, 1, 1, 1, 1, 1],
     [1, 1, 1, 1, 1, 1]],

    [[0, 1, 1, 1, 0],
     [1, 1, 1, 1, 1],
     [1, 1, 1, 1, 1],
     [1, 1, 1, 1, 1]],

    [[1, 1, 1, 1, 1],
     [1, 1, 1, 1, 1],
     [1, 1, 1, 1, 1],
     [1, 1, 1, 1, 1]],
]


def construct_archetype(pattern, panel_w: float, panel_h: float, portrait: bool) -> Polygon:
    """
    Construct a pre-made roof polygon archetype from a pattern and some info about panels
    """
    cells = []

    for y in range(0, len(pattern)):
        row = pattern[y]
        for x in range(0, len(row)):
            if row[x] == 1:
                if portrait:
                    cells.append(rect(x * panel_h, y * panel_w, (x + 1) * panel_h, (y + 1) * panel_w))
                else:
                    cells.append(rect(x * panel_w, y * panel_h, (x + 1) * panel_w, (y + 1) * panel_h))

    premade = ops.unary_union(cells)
    return affinity.translate(premade, -premade.centroid.x, -premade.centroid.y)


def construct_archetypes(panel_w: float, panel_h: float) -> List[Polygon]:
    """
    Construct pre-made roof polygon archetypes from patterns and some info about panels
    """
    premades = []
    for pattern in ARCHETYPE_PATTERNS:
        premades.append(construct_archetype(pattern, panel_w, panel_h, portrait=True))
        premades.append(construct_archetype(pattern, panel_w, panel_h, portrait=False))
    premades.sort(key=lambda p: -p.area)
    return premades


def get_archetype(roof_polygon: Polygon, archetypes: List[Polygon], aspect) -> Optional[Polygon]:
    min_diff = 0.68
    best_archetype = None

    prepared_archetypes = []
    for archetype in archetypes:
        # move the archetype so the centroid is the same as the existing poly:
        archetype = affinity.translate(archetype, roof_polygon.centroid.x, roof_polygon.centroid.y)
        # rotate to match the aspect:
        archetype = affinity.rotate(archetype, -aspect, origin=archetype.centroid)
        prepared_archetypes.append(archetype)

    for archetype in prepared_archetypes:
        # the parts of roof_poly that do not intersect archetype (not such a problem - bits of
        # roof_poly are sticking out the sides of archetype):
        a1 = roof_polygon.difference(archetype).area * 0.75
        # the parts of archetype that do not intersect roof poly (this is worse -
        # archetype is sticking out the sides of roof_poly here - so make it count more):
        a2 = archetype.difference(roof_polygon).area * 1.8
        area_diff = a1 + a2
        pct_diff = area_diff / roof_polygon.area

        if pct_diff < min_diff:
            min_diff = pct_diff
            best_archetype = archetype
        if best_archetype and round(pct_diff, 2) == round(min_diff, 2) and archetype.area > best_archetype.area:
            min_diff = pct_diff
            best_archetype = archetype

    if best_archetype:
        return best_archetype

    # If we didn't find one, try again, but only scoring archetypes based on how much
    # they fit within the roof plane.
    # This relies on the list of archetypes being ordered by area descending, as otherwise
    # the smallest one might be checked first and always win.
    for archetype in prepared_archetypes:
        pct_diff = archetype.difference(roof_polygon).area / roof_polygon.area

        if pct_diff < min_diff:
            min_diff = pct_diff
            best_archetype = archetype
        if best_archetype and round(pct_diff, 2) == round(min_diff, 2) and archetype.area > best_archetype.area:
            min_diff = pct_diff
            best_archetype = archetype

    return best_archetype
