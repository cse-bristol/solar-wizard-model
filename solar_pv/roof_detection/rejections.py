# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
"""
Record of the roof planes that roof detection and roof polygon creation throw away, and
why - for debugging, not used by the model itself. Functions that can reject planes take
an optional `rejected` list to append to; if it's None, nothing is recorded.
"""
from typing import List, Optional

# Left with fewer than 3 pixels when pixels were reassigned
TOO_FEW_PIXELS = "TOO_FEW_PIXELS"
# Not aligned with any of the building's faces
NO_CLOSE_ASPECT = "NO_CLOSE_ASPECT"
# Shape checks on candidate regions that fit their plane poorly
CONVEX_HULL_RATIO = "CONVEX_HULL_RATIO"
THINNESS_RATIO = "THINNESS_RATIO"
# A pitched plane most of whose locally planar pixels face another way
NORMALS_DISAGREE = "NORMALS_DISAGREE"
# A small region that fits its plane poorly
SMALL_POOR_FIT = "SMALL_POOR_FIT"
# Not worth a plane: its pixels are explained nearly as well without it
UNPROFITABLE = "UNPROFITABLE"
CLUTTER = "CLUTTER"
MESSY_FLAT_ROOF = "MESSY_FLAT_ROOF"
# Not a plane: the parts of flat roofs left unassigned for mess, and the obstacles cut out
# of kept flat roofs' polygons
MESSY_PART = "MESSY_PART"
NO_POLYGON = "NO_POLYGON"


def record(rejected: Optional[List[dict]], plane: dict, reason: str, **details) -> None:
    if rejected is not None:
        rejected.append({**plane, "rejection_reason": reason, "rejection_details": details})

