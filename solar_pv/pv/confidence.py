# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
"""
Per-roof-plane confidence scoring: a [0,1] measure of how well we know the roof
surface.

The individual sub-scores are stored raw in the roof plane's `meta`, and combined
into the `confidence` column as a weighted geometric mean.
"""
import math
from typing import Dict

from solar_pv.constants import (
    ROOFDET_MAX_MAE,
    FLAT_ROOF_DEGREES_THRESHOLD,
    CONFIDENCE_FULL_FIT_MAE,
    CONFIDENCE_MAX_ASPECT_CIRC_SD,
    CONFIDENCE_ASPECT_FULL_SLOPE,
    CONFIDENCE_SHAPE_SPAN,
    CONFIDENCE_RESOLUTION_SCORES,
    CONFIDENCE_WEIGHTS,
    CONFIDENCE_SUB_SCORE_FLOOR,
)
from solar_pv.roof_detection.ransac import _min_thinness_ratio


def _clamp(v: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, v))


def _fit_score(mae: float) -> float:
    """RANSAC plane-fit residual (`mae`/`score`, metres): 1 at/below
    CONFIDENCE_FULL_FIT_MAE, falling on a log scale to 0 at the max acceptable MAE."""
    if mae <= CONFIDENCE_FULL_FIT_MAE:
        return 1.0
    return _clamp(1 - math.log(mae / CONFIDENCE_FULL_FIT_MAE)
                  / math.log(ROOFDET_MAX_MAE / CONFIDENCE_FULL_FIT_MAE))


def _aspect_score(aspect_circ_sd: float, slope: float, is_flat: bool) -> float:
    """Spread of inlier-pixel aspects (radians), with the penalty faded in by slope:
    aspect is meaningless for flat roofs and noisy for shallow ones. `is_flat` is
    needed as well as `slope` since flat roofs carry the panel tilt as their slope."""
    if is_flat:
        return 1.0
    raw = _clamp(1 - aspect_circ_sd / CONFIDENCE_MAX_ASPECT_CIRC_SD)
    strength = _clamp((slope - FLAT_ROOF_DEGREES_THRESHOLD)
                      / (CONFIDENCE_ASPECT_FULL_SLOPE - FLAT_ROOF_DEGREES_THRESHOLD))
    return 1 - (1 - raw) * strength


def _shape_score(thinness_ratio: float, n_pixels: float) -> float:
    """`thinness_ratio` (0..1, higher = less sliver-like) relative to the minimum
    roof detection accepts for a plane of this many pixels - larger planes are
    naturally less compact."""
    min_ratio = _min_thinness_ratio(n_pixels)
    return _clamp((thinness_ratio - min_ratio) / CONFIDENCE_SHAPE_SPAN)


def _resolution_score(resolution: float) -> float:
    """LiDAR working resolution (m) mapped to a score, by nearest known resolution."""
    if resolution in CONFIDENCE_RESOLUTION_SCORES:
        return CONFIDENCE_RESOLUTION_SCORES[resolution]
    nearest = min(CONFIDENCE_RESOLUTION_SCORES, key=lambda r: abs(r - resolution))
    return CONFIDENCE_RESOLUTION_SCORES[nearest]


def _geom_agreement_score(area_raw: float, area_grown: float) -> float:
    """Agreement between the tight (raw) polygon RANSAC fits and the grown polygon.
    The grown geometry is always >= the raw one; the closer they are, the less
    ambiguity there is about the roof's extent."""
    if area_grown <= 0:
        return 0.0
    return _clamp(area_raw / area_grown)


def confidence_sub_scores(meta: dict,
                          slope: float,
                          is_flat: bool,
                          resolution: float,
                          area_raw: float,
                          area_grown: float) -> Dict[str, float]:
    """The [0,1] sub-scores, keyed as in CONFIDENCE_WEIGHTS. `area_raw` is the
    planar area (m2) of the polygon RANSAC fitted."""
    return {
        "fit": _fit_score(meta["score"]),
        "aspect": _aspect_score(meta["aspect_circ_sd"], slope, is_flat),
        "shape": _shape_score(meta["thinness_ratio"], area_raw / resolution ** 2),
        "resolution": _resolution_score(resolution),
        "geom_agreement": _geom_agreement_score(area_raw, area_grown),
    }


def combine(sub_scores: Dict[str, float]) -> float:
    """Weighted geometric mean of the sub-scores (weights sum to 1), each floored at
    CONFIDENCE_SUB_SCORE_FLOOR."""
    product = 1.0
    for key, weight in CONFIDENCE_WEIGHTS.items():
        product *= max(sub_scores[key], CONFIDENCE_SUB_SCORE_FLOOR) ** weight
    return product


def roof_plane_confidence(meta: dict,
                          slope: float,
                          is_flat: bool,
                          resolution: float,
                          area_raw: float,
                          area_grown: float):
    """Return (combined [0,1] confidence, sub-scores dict)."""
    sub_scores = confidence_sub_scores(meta, slope, is_flat, resolution, area_raw, area_grown)
    return combine(sub_scores), sub_scores
