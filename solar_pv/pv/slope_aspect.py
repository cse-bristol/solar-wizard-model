# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
"""
Prepare slope and aspect for the PV calculation: merge the roof-plane slope/aspect overrides
onto the GDAL base rasters as numpy where-ops.

gdaldem already produces compass aspect (0 = N clockwise, 0 also marking flat via
-zero_for_flat), which is the convention the PV calculation consumes, so no aspect-convention
conversion is needed. Slope/aspect themselves stay on GDAL (gdaldem, produced by
generate_rasters); this only does the override merge. The flat-roof pitch is applied upstream —
roof_polygons writes flat_roof_degrees into the plane slope the override raster is built from —
so it is already baked into the overrides here.
"""
from typing import Tuple

import numpy as np


def apply_correction(slope_deg: np.ndarray,
                     aspect_compass_deg: np.ndarray,
                     aspect_override_compass_deg: np.ndarray,
                     slope_override_deg: np.ndarray
                     ) -> Tuple[np.ndarray, np.ndarray]:
    """
    :param slope_deg: GDAL slope (degrees).
    :param aspect_compass_deg: GDAL compass aspect (degrees, 0 = N clockwise / flat).
    :param aspect_override_compass_deg: per-pixel roof-plane aspect (compass degrees), NaN
        outside usable planes.
    :param slope_override_deg: per-pixel roof-plane slope (degrees), NaN outside usable planes.
    :return: (slope_adjusted_deg, aspect_adjusted_compass_deg).
    """
    slope = np.asarray(slope_deg, dtype=np.float64)
    aspect = np.asarray(aspect_compass_deg, dtype=np.float64)
    aspect_override = np.asarray(aspect_override_compass_deg, dtype=np.float64)
    slope_override = np.asarray(slope_override_deg, dtype=np.float64)

    # use the override where a usable plane covers the pixel (non-NaN), else the base value:
    aspect_adjusted = np.where(np.isfinite(aspect_override), aspect_override, aspect)
    slope_adjusted = np.where(np.isfinite(slope_override), slope_override, slope)
    return slope_adjusted, aspect_adjusted
