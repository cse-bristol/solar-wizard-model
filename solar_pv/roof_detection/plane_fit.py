# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
"""
Lean stand-ins for the parts of sklearn's `LinearRegression` and regression metrics
that roof detection needs in its inner loops.

Roof detection fits planes to pixels many times per building, and sklearn's per-call
input validation cost ~30x more than the maths itself. `PlaneFit` and
the metrics repeat sklearn 1.9's arithmetic in the same order, so results are
identical to sklearn's (`test_plane_fit.py` guards this). They assume what the
callers guarantee: dense, finite float64 inputs with X of shape (n, 2).
"""
from typing import Optional

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


def mean_squared_error(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    return float(((y_true - y_pred) ** 2).mean(axis=0))


def root_mean_squared_error(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    return float(np.sqrt(((y_true - y_pred) ** 2).mean(axis=0)))


def mean_squared_log_error(y_true: np.ndarray, y_pred: np.ndarray) -> Optional[float]:
    """None where sklearn's raises ValueError: if any value is -1 or less."""
    if np.any(y_true <= -1) or np.any(y_pred <= -1):
        return None
    return mean_squared_error(np.log1p(y_true), np.log1p(y_pred))


def mean_absolute_percentage_error(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    return float((np.abs(y_pred - y_true) / np.maximum(np.abs(y_true), np.finfo(np.float64).eps)).mean(axis=0))
