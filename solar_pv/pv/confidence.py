# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
"""
Per-roof-plane confidence scoring: a [0,1] measure of how well we know the roof
surface.

The individual sub-scores are stored raw in the roof plane's `meta`, and combined
into the `confidence` column as a weighted geometric mean.
"""
from typing import Dict

from solar_pv.constants import (
    ROOFDET_GOOD_SCORE,
    ROOFDET_MAX_MAE,
    CONFIDENCE_MAX_ASPECT_CIRC_SD,
    CONFIDENCE_RESOLUTION_SCORES,
    CONFIDENCE_WEIGHTS,
)


def _clamp(v: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, v))


def _fit_score(mae: float) -> float:
    """RANSAC plane-fit residual (`mae`/`score`, metres): 1 at/below the 'good'
    threshold, falling linearly to 0 at the max acceptable MAE."""
    return _clamp((ROOFDET_MAX_MAE - mae) / (ROOFDET_MAX_MAE - ROOFDET_GOOD_SCORE))


def _aspect_score(aspect_circ_sd: float, is_flat: bool) -> float:
    """Spread of inlier-pixel aspects (radians). Aspect is meaningless for flat
    roofs, so they get full marks."""
    if is_flat:
        return 1.0
    return _clamp(1 - aspect_circ_sd / CONFIDENCE_MAX_ASPECT_CIRC_SD)


def _shape_score(thinness_ratio: float) -> float:
    """`thinness_ratio` (0..1, higher = less sliver-like) used directly."""
    return _clamp(thinness_ratio)


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
                          is_flat: bool,
                          resolution: float,
                          area_raw: float,
                          area_grown: float) -> Dict[str, float]:
    """The [0,1] sub-scores, keyed as in CONFIDENCE_WEIGHTS."""
    return {
        "fit": _fit_score(meta["score"]),
        "aspect": _aspect_score(meta["aspect_circ_sd"], is_flat),
        "shape": _shape_score(meta["thinness_ratio"]),
        "resolution": _resolution_score(resolution),
        "geom_agreement": _geom_agreement_score(area_raw, area_grown),
    }


def combine(sub_scores: Dict[str, float]) -> float:
    """Weighted geometric mean of the sub-scores (weights sum to 1)."""
    product = 1.0
    for key, weight in CONFIDENCE_WEIGHTS.items():
        product *= sub_scores[key] ** weight
    return product


def roof_plane_confidence(meta: dict,
                          is_flat: bool,
                          resolution: float,
                          area_raw: float,
                          area_grown: float):
    """Return (combined [0,1] confidence, sub-scores dict)."""
    sub_scores = confidence_sub_scores(meta, is_flat, resolution, area_raw, area_grown)
    return combine(sub_scores), sub_scores
