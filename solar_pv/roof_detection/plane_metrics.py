# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
"""
A roof plane's fit to its pixels, and the measures of it that roof detection gates on,
stores in the plane's `meta`, and scores confidence from.
"""
import math

import numpy as np
from skimage import morphology
from skimage.measure import perimeter_crofton

from solar_pv.constants import FLAT_ROOF_DEGREES_THRESHOLD
from solar_pv.geos import slope_deg, aspect_deg, circular_mean_rad, circular_sd_rad
from solar_pv.roof_detection.plane_fit import PlaneFit, mean_absolute_error, r2_score, mean_squared_error, \
    root_mean_squared_error, mean_squared_log_error, mean_absolute_percentage_error


def set_plane_fit(plane: dict, xy: np.ndarray, z: np.ndarray, aspect: np.ndarray, res: float) -> None:
    """
    Refit `plane` to the pixels `xy`/`z` (with per-pixel `aspect`), and recompute
    everything on it that derives from its pixels: coefficients, slope, fit metrics,
    shape ratios and aspect statistics. `inliers_xy` becomes `xy`.
    """
    lr = PlaneFit()
    lr.fit(xy, z)
    z_pred = lr.predict(xy)

    plane['x_coef'] = lr.coef_[0]
    plane['y_coef'] = lr.coef_[1]
    plane['intercept'] = lr.intercept_
    plane['slope'] = slope_deg(lr.coef_[0], lr.coef_[1])
    plane['is_flat'] = plane['slope'] <= FLAT_ROOF_DEGREES_THRESHOLD
    plane['aspect_raw'] = aspect_deg(lr.coef_[0], lr.coef_[1])
    plane['inliers_xy'] = xy
    set_fit_metrics(plane, z, z_pred)

    plane_mask = _mask(xy, res)
    roof_plane_area = np.count_nonzero(plane_mask)
    convex_hull = morphology.convex_hull_image(plane_mask)
    plane["cv_hull_ratio"] = roof_plane_area / np.count_nonzero(convex_hull)

    perimeter = perimeter_crofton(plane_mask, directions=4)
    plane["thinness_ratio"] = (4 * np.pi * roof_plane_area) / (perimeter * perimeter)

    # mean in degrees, sd in radians
    aspect_rads = np.radians(aspect)
    plane["aspect_circ_mean"] = math.degrees(circular_mean_rad(aspect_rads))
    plane["aspect_circ_sd"] = circular_sd_rad(aspect_rads)


def set_fit_metrics(plane: dict, z: np.ndarray, z_pred: np.ndarray) -> None:
    """The fit metrics of `plane` (`score` is its MAE) for pixel elevations `z`, which it predicts are `z_pred`."""
    plane["score"] = plane["mae"] = mean_absolute_error(z, z_pred)
    plane["r2"] = r2_score(z, z_pred)
    plane["mse"] = mean_squared_error(z, z_pred)
    plane["rmse"] = root_mean_squared_error(z, z_pred)
    plane["msle"] = mean_squared_log_error(z, z_pred)
    plane["mape"] = mean_absolute_percentage_error(z, z_pred)
    plane["sd"] = np.std(np.abs(z - z_pred))


def min_thinness_ratio(n_pixels) -> float:
    """
    The lowest thinness ratio (4 * pi * area / perimeter^2, which is low for long, thin
    shapes) a plane of this many pixels may have. Larger planes are naturally less compact.
    """
    if n_pixels <= 40:
        return 0.45
    elif n_pixels <= 50:
        return 0.4
    elif n_pixels <= 300:
        return 0.24
    elif n_pixels <= 500:
        return 0.2
    elif n_pixels <= 750:
        return 0.15
    elif n_pixels <= 3000:
        return 0.10
    else:
        return 0.07


def _mask(xy: np.ndarray, res: float) -> np.ndarray:
    """An image of which pixels are among `xy`, north up."""
    normed = ((xy - xy.min(axis=0)) / res).astype(int)
    mask = np.zeros((normed[:, 1].max() + 1, normed[:, 0].max() + 1), dtype=bool)
    mask[normed[:, 1], normed[:, 0]] = True
    return np.flip(mask, axis=0)
