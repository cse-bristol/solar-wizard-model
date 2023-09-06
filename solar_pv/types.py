from typing import TypedDict, List

import numpy as np
from shapely.geometry import Polygon


class RoofDetBuilding(TypedDict):
    """Building-level inputs to roof-plane detection"""
    toid: str
    pixels: List[dict]
    polygon: Polygon
    min_ground_height: float
    max_ground_height: float


# TODO combine these two types
class RoofPlane(TypedDict):
    """Outputs from roof-plane detection"""
    toid: str
    plane_type: str
    plane_id: str

    slope: float
    aspect: float
    aspect_adjusted: int

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


class RoofPolygon(TypedDict):
    """Outputs from roof plane -> roof polygon algorithm"""
    toid: str
    roof_geom_27700: str
    plane_type: str
    plane_id: str

    easting: float
    northing: float
    raw_footprint: float
    raw_area: float
    usable: bool
    not_usable_reason: str

    slope: float
    is_flat: bool
    aspect_raw: float
    aspect: int

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
