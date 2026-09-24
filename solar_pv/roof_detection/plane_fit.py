# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
"""
Lean stand-ins for the parts of sklearn's `LinearRegression` and regression metrics
that roof detection needs in its inner loops.

Roof detection fits and scores tens of thousands of candidate planes per building, and
sklearn's per-call input validation cost ~30x more than the maths itself. `PlaneFit` and
the metrics repeat sklearn 1.9's arithmetic in the same order, so results are
identical to sklearn's (`test_plane_fit.py` guards this). They assume what the
callers guarantee: dense, finite float64 inputs with X of shape (n, 2).

`PlaneSums` is an incremental fit, close to but not identical to `PlaneFit`.
"""
from typing import Optional, Tuple

import numpy as np
from scipy.linalg import lapack

# LinearRegression's default `tol`, passed to lstsq as `cond`:
_LSTSQ_COND = 1e-6

# The LAPACK driver behind scipy.linalg.lstsq, called directly as scipy's wrapper costs
# more than the solve for these small problems.
_gelsd, _gelsd_lwork = lapack.get_lapack_funcs(('gelsd', 'gelsd_lwork'), dtype=np.float64)


class PlaneFit:
    """z = coef_[0] * x + coef_[1] * y + intercept_, fit by least squares."""
    __slots__ = ("coef_", "intercept_")

    def fit(self, X: np.ndarray, y: np.ndarray) -> "PlaneFit":
        X = np.array(X, dtype=np.float64, copy=True)
        y = np.array(y, dtype=np.float64, copy=True)
        X_offset = X.mean(axis=0)
        X -= X_offset
        y_offset = y.mean(axis=0)
        y -= y_offset
        self.coef_ = _lstsq(X, y)
        self.intercept_ = y_offset - X_offset @ self.coef_
        return self

    def predict(self, X: np.ndarray) -> np.ndarray:
        return X @ self.coef_ + self.intercept_

    def score(self, X: np.ndarray, y: np.ndarray) -> float:
        return r2_score(y, self.predict(X))


class PlaneSums:
    """
    Running sums over a set of points, from which the least-squares plane through them
    can be solved in O(1) - and two sets' sums added to give their union, so the plane
    fit to a merge of two sets doesn't need a pass over their points.

    Solving from sums isn't exactly the same as `PlaneFit` (lstsq via SVD), and loses
    precision when the coordinates are large: use coordinates relative to a nearby
    origin, not raw 27700 ones.
    """
    __slots__ = ("sums",)

    # Below this ratio of the smallest to the largest eigenvalue of the points' xy
    # covariance, `solve` defers to `PlaneFit`. lstsq treats singular values below
    # 1e-6 of the largest as zero, i.e. an eigenvalue ratio of 1e-12; this is set
    # wider so that everything near that cutoff goes through lstsq.
    _MIN_EIGENVALUE_RATIO = 1e-9

    def __init__(self, sums: np.ndarray):
        self.sums = sums

    @classmethod
    def of(cls, xy: np.ndarray, z: np.ndarray) -> "PlaneSums":
        x = xy[:, 0]
        y = xy[:, 1]
        return cls(np.array([len(z), x.sum(), y.sum(), z.sum(), x @ x, x @ y, y @ y, x @ z, y @ z]))

    def __add__(self, other: "PlaneSums") -> "PlaneSums":
        return PlaneSums(self.sums + other.sums)

    def solve(self) -> Optional[Tuple[float, float, float]]:
        """(x_coef, y_coef, intercept), or None if the points are too near collinear
        (or too few) to solve reliably this way."""
        n, sx, sy, sz, sxx, sxy, syy, sxz, syz = self.sums
        mx, my, mz = sx / n, sy / n, sz / n
        cxx = sxx - sx * mx
        cxy = sxy - sx * my
        cyy = syy - sy * my
        cxz = sxz - sx * mz
        cyz = syz - sy * mz
        det = cxx * cyy - cxy * cxy
        trace = cxx + cyy
        # det / trace^2 approximates the smaller eigenvalue over the larger:
        if not det > self._MIN_EIGENVALUE_RATIO * trace * trace:
            return None
        a = (cxz * cyy - cyz * cxy) / det
        b = (cyz * cxx - cxz * cxy) / det
        return a, b, mz - a * mx - b * my


def _lstsq(X: np.ndarray, y: np.ndarray) -> np.ndarray:
    m, n = X.shape
    lwork, iwork, info = _gelsd_lwork(m, n, 1, _LSTSQ_COND)
    if info != 0:
        raise ValueError(f"gelsd workspace query failed: info {info}")
    b = y.reshape(-1, 1) if m >= n else np.concatenate([y, np.zeros(n - m)]).reshape(-1, 1)
    x, _, _, info = _gelsd(X, b, int(lwork), int(iwork), _LSTSQ_COND, False, False)
    if info != 0:
        raise np.linalg.LinAlgError(f"SVD did not converge in least squares: info {info}")
    return x[:n, 0]


def mean_absolute_error(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    return float(np.abs(y_pred - y_true).mean(axis=0))


def r2_score(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    if len(y_pred) < 2:
        return float("nan")
    numerator = np.sum((y_true - y_pred) ** 2, axis=0)
    denominator = np.sum((y_true - y_true.mean(axis=0)) ** 2, axis=0)
    if numerator == 0:
        return 1.0
    if denominator == 0:
        return 0.0
    return float(1 - (numerator / denominator))
