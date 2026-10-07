from typing import TypedDict, List

import numpy as np
from shapely.geometry import Polygon, MultiPolygon


class RoofDetBuilding(TypedDict):
    """Building-level inputs to roof-plane detection"""
    building_id: str
    pixels: List[dict]
    polygon: Polygon
    min_ground_height: float
    max_ground_height: float


class RoofPlane(TypedDict):
    """Outputs from roof-plane detection"""
    building_id: str
    plane_type: str
    plane_id: str

    slope: float
    is_flat: bool
    aspect: int
    aspect_raw: float

    x_coef: float
    y_coef: float
    intercept: float
    inliers_xy: np.ndarray

    r2: float
    mae: float
    mse: float
    rmse: float
    msle: float
    mape: float
    sd: float
    score: float

    aspect_circ_mean: float
    aspect_circ_sd: float
    thinness_ratio: float
    cv_hull_ratio: float


class RoofPolygon(RoofPlane):
    """Outputs from roof plane -> roof polygon algorithm"""

    # the plane's pixels (as squares) within roof_geom_27700: its area is the plane's area
    roof_geom_raw_27700: MultiPolygon
    roof_geom_27700: Polygon
    # The tilt panels are modelled at: `flat_roof_degrees` on flat roofs, else `slope`
    panel_slope: float
    usable: bool
    not_usable_reason: str
