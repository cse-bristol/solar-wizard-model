# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import unittest

import numpy as np
from sklearn import metrics
from sklearn.linear_model import LinearRegression

from solar_pv.roof_detection.plane_fit import PlaneFit, mean_absolute_error, r2_score, \
    mean_squared_error, root_mean_squared_error, mean_squared_log_error, mean_absolute_percentage_error


def _roof_like_xyz(rng, n: int) -> np.ndarray:
    """Points with BNG-sized coords on a noisy plane, like roof detection's inputs."""
    x = 358000 + rng.integers(0, 60, n) + 0.5
    y = 172000 + rng.integers(0, 60, n) + 0.5
    z = 0.3 * (x - 358000) - 0.2 * (y - 172000) + 40 + rng.normal(0, 0.1, n)
    return np.column_stack([x, y, z])


class PlaneFitMatchesSklearnTest(unittest.TestCase):
    """PlaneFit and the metrics must be identical to sklearn, not just close:
    roof detection compares scores between candidates, so any drift can change which
    planes win."""

    def _cases(self):
        rng = np.random.default_rng(1)
        for n in [3, 4, 5, 17, 100, 1001, 20000]:
            for _ in range(20 if n < 1000 else 3):
                xyz = _roof_like_xyz(rng, n)
                # roof detection passes strided views of an (n, 3) array:
                yield xyz[:, :2], xyz[:, 2]
                yield np.ascontiguousarray(xyz[:, :2]), np.ascontiguousarray(xyz[:, 2])
        # degenerate: collinear and coincident points
        yield np.array([[1.5, 2.5], [2.5, 3.5], [3.5, 4.5]]), np.array([1.0, 2.0, 3.0])
        yield np.array([[1.5, 2.5], [1.5, 2.5], [1.5, 2.5]]), np.array([1.0, 1.0, 1.0])

    def test_fit_and_predict(self):
        for X, y in self._cases():
            sk = LinearRegression().fit(X, y)
            pf = PlaneFit().fit(X, y)
            np.testing.assert_array_equal(pf.coef_, sk.coef_)
            self.assertEqual(pf.intercept_, sk.intercept_)
            np.testing.assert_array_equal(pf.predict(X), sk.predict(X))
            self.assertEqual(pf.score(X, y), sk.score(X, y))

    def test_metrics(self):
        for X, y in self._cases():
            y_pred = LinearRegression().fit(X, y).predict(X)
            self.assertEqual(mean_absolute_error(y, y_pred), metrics.mean_absolute_error(y, y_pred))
            if len(y) >= 2:
                self.assertEqual(r2_score(y, y_pred), metrics.r2_score(y, y_pred))
            self.assertEqual(mean_squared_error(y, y_pred), metrics.mean_squared_error(y, y_pred))
            self.assertEqual(root_mean_squared_error(y, y_pred), metrics.root_mean_squared_error(y, y_pred))
            self.assertEqual(mean_squared_log_error(y, y_pred), metrics.mean_squared_log_error(y, y_pred))
            self.assertEqual(mean_absolute_percentage_error(y, y_pred),
                             metrics.mean_absolute_percentage_error(y, y_pred))
            # exact fit / constant target edge cases:
            self.assertEqual(r2_score(y, y.copy()), metrics.r2_score(y, y.copy()))
            const = np.full(y.shape, 3.0)
            self.assertEqual(r2_score(const, y_pred), metrics.r2_score(const, y_pred))
            # subsets of a prediction, as _evaluate_candidate scores them:
            idxs = np.arange(0, len(y), 2)
            self.assertEqual(mean_absolute_error(y[idxs], y_pred[idxs]),
                             metrics.mean_absolute_error(y[idxs], y_pred[idxs]))

    def test_msle_is_none_where_sklearn_raises(self):
        y = np.array([1.0, -1.0, 2.0])
        with self.assertRaises(ValueError):
            metrics.mean_squared_log_error(y, y + 0.5)
        self.assertIsNone(mean_squared_log_error(y, y + 0.5))

    def test_mape_near_zero_targets(self):
        y, y_pred = np.array([0.0, 1e-20, 2.0]), np.array([0.1, 0.0, 2.5])
        self.assertEqual(mean_absolute_percentage_error(y, y_pred), metrics.mean_absolute_percentage_error(y, y_pred))


if __name__ == '__main__':
    unittest.main()
