# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
"""
How far a roof plane's stored orientation is from the plane implied by the LiDAR pixels
its polygon covers.

The residuals (pixel z minus stored plane) are regressed on x and y: the slope terms are the
tilt error, the constant the height offset at the pixels' centroid, and what's left is
roughness. MAE mixes these up - a small plane can fit to within a few cm and still be several
degrees out, and a cluttered roof can have a high MAE and the right orientation - and only
orientation affects kWh/kWp.

The implied plane is a robust fit (a least trimmed squares start, then Tukey biweight), so it
is the plane most of the pixels lie on, not a compromise with pixels of a neighbouring face
or of clutter; the angle between it and a plain least-squares fit measures that contamination.
The start must fit at least 3/4 of the pixels, not the usual 1/2: on a plane only 3 or 4
pixels deep across its slope, a plane through half of them is just two rows, which it can fit
almost exactly and at the wrong tilt, rejecting the (mixed) ridge or eave rows. So it
tolerates up to 1/4 of the pixels being off the plane; where more are, the polygon straddles
two faces, there is no dominant plane, and the implied plane is an average that fits neither:
a high `roughness_m` and `contamination_deg` show this.

The tilt's standard error is a jackknife over bands of pixels, one pixel wide, across and along the slope:
how far the tilt moves when a band is left out. The residuals of a small plane aren't
independent noise - a ridge or eave row is offset as a whole - so a standard error that
assumes they are makes small planes' tilts look far more certain than they are.
"""
from typing import Optional, Tuple

import numpy as np

from solar_pv.geos import slope_deg, aspect_deg

MIN_PIXELS = 10
# Roughly LiDAR vertical noise: the robust scale can't go below this.
NOISE_FLOOR_M = 0.03
_TUKEY_C = 4.685
_START_QUANTILE = 0.75
_START_SAMPLES = 150
_START_MAX_PIXELS = 2000
# Fewer pixel-wide bands across the slope than this, and the tilt can't be pinned down by dropping one:
MIN_BANDS = 3
_MAX_JACKKNIFE_GROUPS = 10
_MIN_JACKKNIFE_PIXELS = 6


def plane_skew(xyz: np.ndarray,
               coef: Tuple[float, float, float],
               resolution: float) -> Optional[dict]:
    """
    Skew of the plane `coef` (x_coef, y_coef, intercept) against the pixels `xyz` (N, 3) its
    polygon covers, or None if there are too few pixels, or they don't span at least two
    pixels in x and in y, to determine a tilt.

    `tilt_err_deg` is the angle between the stored and implied planes' normals and `tilt_z`
    its size relative to its (jackknife) standard error - null if the pixels are fewer than
    `MIN_BANDS` pixels deep across the slope (`bands`). `offset_m` is the implied plane's
    height above the stored one at the pixels' centroid. `contamination_deg` is the angle between the
    implied plane and a least-squares fit to all the pixels, and `outlier_share` the share of
    pixels off the implied plane.
    """
    n = len(xyz)
    if n < MIN_PIXELS:
        return None
    x, y, z = xyz[:, 0], xyz[:, 1], xyz[:, 2]
    xc, yc, ic = coef
    residual = z - (x * xc + y * yc + ic)
    dx, dy = x - x.mean(), y - y.mean()
    if np.ptp(dx) < resolution or np.ptp(dy) < resolution:
        return None

    X = np.column_stack([dx, dy, np.ones(n)])
    ols = _wls(X, residual, np.ones(n))
    robust, sigma = _robust_fit(X, residual, _lts_start(X, residual))
    e = residual - X @ robust
    inliers = np.abs(e) <= 2.5 * sigma
    s2 = max(np.sum(e[inliers] ** 2) / max(inliers.sum() - 3, 1), NOISE_FLOOR_M ** 2)

    # bands across the slope run along its contours; a flat plane's slope direction is
    # arbitrary, so it uses x and y:
    if np.hypot(xc, yc) > 1e-3:
        down = -np.array([xc, yc]) / np.hypot(xc, yc)
    else:
        down = np.array([1.0, 0.0])
    across = dx * down[0] + dy * down[1]
    along = dx * -down[1] + dy * down[0]
    bands = int(len(np.unique(np.floor((across - across.min()) / resolution))))

    stored = _normal(xc, yc)
    implied_x, implied_y = xc + robust[0], yc + robust[1]
    implied = _normal(implied_x, implied_y)
    tilt_z = None
    if bands >= MIN_BANDS:
        se = max(_jackknife_se(X, residual, robust, across, resolution),
                 _jackknife_se(X, residual, robust, along, resolution))
        gradient = np.hypot(robust[0], robust[1])
        if se > 0 and np.isfinite(se):
            tilt_z = round(float(gradient / se), 2)

    return {
        "pixels": n,
        "bands": bands,
        "tilt_err_deg": round(_angle(stored, implied), 3),
        "tilt_z": tilt_z,
        "offset_m": round(float(robust[2]), 3),
        "roughness_m": round(float(np.sqrt(s2)), 3),
        "contamination_deg": round(_angle(implied, _normal(xc + ols[0], yc + ols[1])), 3),
        "outlier_share": round(float(1 - inliers.mean()), 3),
        "implied_slope": round(slope_deg(implied_x, implied_y), 2),
        "implied_aspect": round(aspect_deg(implied_x, implied_y), 2),
    }


def _jackknife_se(X: np.ndarray, r: np.ndarray, beta: np.ndarray, coord: np.ndarray, resolution: float) -> float:
    """
    Standard error of the residual plane's gradient in the direction of `beta`'s, from
    leaving out each of up to _MAX_JACKKNIFE_GROUPS contiguous groups of pixel-wide bands of `coord`
    in turn and refitting (from `beta`: these are perturbations of the same fit).
    """
    band = np.floor((coord - coord.min()) / resolution).astype(int)
    n_bands = band.max() + 1
    groups = np.minimum(band * min(n_bands, _MAX_JACKKNIFE_GROUPS) // n_bands, _MAX_JACKKNIFE_GROUPS - 1)
    ids = np.unique(groups)
    if len(ids) < MIN_BANDS:
        return 0.0
    gradients = []
    for g in ids:
        keep = groups != g
        if keep.sum() < _MIN_JACKKNIFE_PIXELS or np.ptp(X[keep, 0]) < resolution or np.ptp(X[keep, 1]) < resolution:
            return np.inf
        gradients.append(_robust_fit(X[keep], r[keep], beta)[0][:2])
    gradients = np.array(gradients)
    g = len(gradients)
    norm = np.hypot(*beta[:2])
    direction = beta[:2] / norm if norm > 1e-9 else np.array([1.0, 0.0])
    projected = gradients @ direction
    return float(np.sqrt((g - 1) / g * np.sum((projected - projected.mean()) ** 2)))


def _robust_fit(X: np.ndarray, r: np.ndarray, start: np.ndarray) -> Tuple[np.ndarray, float]:
    """Tukey biweight IRLS from `start`, and the final robust scale."""
    beta = start
    sigma = NOISE_FLOOR_M
    for _ in range(30):
        e = r - X @ beta
        sigma = max(1.4826 * np.median(np.abs(e - np.median(e))), NOISE_FLOOR_M)
        u = e / (_TUKEY_C * sigma)
        w = np.where(np.abs(u) < 1, (1 - u ** 2) ** 2, 0.0)
        if np.count_nonzero(w) < 3:
            break
        new = _wls(X, r, w)
        converged = np.allclose(new, beta, atol=1e-6)
        beta = new
        if converged:
            break
    return beta, sigma


def _lts_start(X: np.ndarray, r: np.ndarray) -> np.ndarray:
    """
    The plane through 3 pixels, out of random samples (or the stored plane itself), with the
    least `_START_QUANTILE` quantile of absolute residuals. A redescending refinement from
    here can't be dragged to a compromise between the dominant plane and pixels off it, as
    one from least squares can when the stored plane is a few degrees out: then the stored
    plane's residuals over a roof are as large as a step to a neighbouring surface.
    """
    rng = np.random.default_rng(0)
    if len(r) > _START_MAX_PIXELS:
        sub = rng.choice(len(r), _START_MAX_PIXELS, replace=False)
        X, r = X[sub], r[sub]
    best, best_q = np.zeros(3), np.quantile(np.abs(r), _START_QUANTILE)
    triples = np.array([rng.choice(len(r), 3, replace=False) for _ in range(_START_SAMPLES)])
    A = X[triples]
    ok = np.abs(np.linalg.det(A)) > 1e-6
    if ok.any():
        betas = np.linalg.solve(A[ok], r[triples[ok]][..., None])[..., 0]
        qs = np.quantile(np.abs(r[None, :] - betas @ X.T), _START_QUANTILE, axis=1)
        i = int(np.argmin(qs))
        if qs[i] < best_q:
            best = betas[i]
    return best


def _wls(X: np.ndarray, r: np.ndarray, w: np.ndarray) -> np.ndarray:
    sw = np.sqrt(w)
    beta, *_ = np.linalg.lstsq(X * sw[:, None], r * sw, rcond=None)
    return beta


def _normal(x_coef: float, y_coef: float) -> np.ndarray:
    n = np.array([-x_coef, -y_coef, 1.0])
    return n / np.linalg.norm(n)


def _angle(n1: np.ndarray, n2: np.ndarray) -> float:
    return float(np.degrees(np.arccos(np.clip(np.dot(n1, n2), -1.0, 1.0))))
