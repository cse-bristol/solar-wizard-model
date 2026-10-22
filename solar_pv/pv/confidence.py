# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
"""
Per-roof-plane confidence scoring: a [0,1] measure of how well we know the roof
surface.

The individual sub-scores are stored raw in the roof plane's `meta`, and combined
into the `confidence` column as a weighted geometric mean.
"""
import math
from typing import Dict, Optional

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
    CONFIDENCE_MAX_FOOTPRINT_ASPECT_GAP,
    AZIMUTH_ALIGNMENT_THRESHOLD,
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


def _aspect_strength(slope: float) -> float:
    """How much the aspect sub-score counts: aspect is meaningless for flat roofs and noisy
    for shallow ones."""
    return _clamp((slope - FLAT_ROOF_DEGREES_THRESHOLD)
                  / (CONFIDENCE_ASPECT_FULL_SLOPE - FLAT_ROOF_DEGREES_THRESHOLD))


def _aspect_score(aspect_circ_sd: float, slope: float) -> float:
    """Spread of inlier-pixel aspects (radians), with the penalty faded in by slope."""
    raw = _clamp(1 - aspect_circ_sd / CONFIDENCE_MAX_ASPECT_CIRC_SD)
    return 1 - (1 - raw) * _aspect_strength(slope)


def _facing_score(facing_share: Optional[float], slope: float) -> Optional[float]:
    """
    The share of the plane's locally planar pixels whose own normals agree with it (see
    `detect_roofs._facing_share`), with the penalty faded in by slope as `_aspect_score`'s
    is. Segment roof detection records it in place of the spread of the pixels' aspects,
    which come from 3x3 windows that at 1m mostly straddle a ridge, eave or valley. None
    (not scored) if too few pixels were locally planar to say, unless it's flat.
    """
    strength = _aspect_strength(slope)
    if strength == 0:
        return 1.0
    if facing_share is None:
        return None
    return 1 - (1 - facing_share) * strength


def _footprint_score(footprint_aspect_gap: Optional[float], slope: float) -> float:
    """How far the plane's aspect is from the building's face azimuths (degrees, see
    `detect_roofs._set_footprint_aspect_gap`), faded in by slope as `_aspect_score` is.
    Roof detection keeps planes the footprint disagrees with if their pixels corroborate
    them, as footprints derived from LiDAR follow roof edges only roughly; this is what
    the disagreement costs. 1 if not known (flat, or from before it was recorded)."""
    if footprint_aspect_gap is None:
        return 1.0
    raw = _clamp(1 - (footprint_aspect_gap - AZIMUTH_ALIGNMENT_THRESHOLD)
                 / (CONFIDENCE_MAX_FOOTPRINT_ASPECT_GAP - AZIMUTH_ALIGNMENT_THRESHOLD))
    return 1 - (1 - raw) * _aspect_strength(slope)


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
                          resolution: float,
                          area_raw: float,
                          area_grown: float) -> Dict[str, Optional[float]]:
    """The [0,1] sub-scores, keyed as in CONFIDENCE_WEIGHTS, or None for one that isn't
    known. `area_raw` is the planar area (m2) of the polygon RANSAC fitted."""
    return {
        "fit": _fit_score(meta["score"]),
        "aspect": (_facing_score(meta["facing_share"], slope) if "facing_share" in meta
                   else _aspect_score(meta["aspect_circ_sd"], slope)),
        "shape": _shape_score(meta["thinness_ratio"], area_raw / resolution ** 2),
        "resolution": _resolution_score(resolution),
        "geom_agreement": _geom_agreement_score(area_raw, area_grown),
        "footprint": _footprint_score(meta.get("footprint_aspect_gap"), slope),
    }


def combine(sub_scores: Dict[str, Optional[float]]) -> float:
    """Weighted geometric mean of the known (not None) sub-scores, each floored at
    CONFIDENCE_SUB_SCORE_FLOOR: one that isn't known counts neither for nor against."""
    known = {key: weight for key, weight in CONFIDENCE_WEIGHTS.items() if sub_scores[key] is not None}
    total = sum(known.values())
    product = 1.0
    for key, weight in known.items():
        product *= max(sub_scores[key], CONFIDENCE_SUB_SCORE_FLOOR) ** (weight / total)
    return product


def roof_plane_confidence(meta: dict,
                          slope: float,
                          resolution: float,
                          area_raw: float,
                          area_grown: float):
    """Return (combined [0,1] confidence, sub-scores dict)."""
    sub_scores = confidence_sub_scores(meta, slope, resolution, area_raw, area_grown)
    return combine(sub_scores), sub_scores
