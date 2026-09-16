# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
"""
Prepare slope and aspect for the PV calculation: apply the flat-roof default and the roof-plane
slope/aspect overrides as numpy where-ops.

gdaldem already produces compass aspect (0 = N clockwise, 0 also marking flat via
-zero_for_flat), which is the convention the PV calculation consumes, so no aspect-convention
conversion is needed. Slope/aspect themselves stay on GDAL (gdaldem, produced by
generate_rasters); this only does the flat-roof default + override merge.
"""
from typing import Optional, Tuple

import numpy as np


def apply_correction(slope_deg: np.ndarray,
                     aspect_compass_deg: np.ndarray,
                     flat_roof_degrees: float,
                     flat_roof_threshold: float,
                     aspect_override_compass_deg: Optional[np.ndarray] = None,
                     slope_override_deg: Optional[np.ndarray] = None
                     ) -> Tuple[np.ndarray, np.ndarray]:
    """
    :param slope_deg: GDAL slope (degrees).
    :param aspect_compass_deg: GDAL compass aspect (degrees, 0 = N clockwise / flat).
    :param flat_roof_degrees: pitch assigned to flat roofs when there is no override.
    :param flat_roof_threshold: slope below which a roof counts as flat.
    :param aspect_override_compass_deg: per-pixel roof-plane aspect (compass degrees), NaN
        outside usable planes; None if no override raster (test path).
    :param slope_override_deg: per-pixel roof-plane slope (degrees), NaN outside usable
        planes; None if no override raster.
    :return: (slope_adjusted_deg, aspect_adjusted_compass_deg).
    """
    slope = np.asarray(slope_deg, dtype=np.float64)
    aspect = np.asarray(aspect_compass_deg, dtype=np.float64)

    if aspect_override_compass_deg is not None:
        override = np.asarray(aspect_override_compass_deg, dtype=np.float64)
        # override where a usable plane covers the pixel (non-NaN), else the base aspect:
        aspect_adjusted = np.where(np.isfinite(override), override, aspect)
    else:
        # no override: flat roofs default to South (compass 180):
        aspect_adjusted = np.where(slope < flat_roof_threshold, 180.0, aspect)

    if slope_override_deg is not None:
        so = np.asarray(slope_override_deg, dtype=np.float64)
        slope_adjusted = np.where(np.isfinite(so), so, slope)
    else:
        slope_adjusted = np.where(slope < flat_roof_threshold, flat_roof_degrees, slope)

    return slope_adjusted, aspect_adjusted
