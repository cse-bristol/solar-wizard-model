# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
from collections import defaultdict
from typing import List, Set

import numpy as np
from shapely.geometry import Polygon
from sklearn.linear_model import LinearRegression

from solar_pv.constants import FLAT_ROOF_DEGREES_THRESHOLD
from solar_pv.geos import slope_deg, aspect_deg
from solar_pv.roof_detection.premade_planes import Plane
from solar_pv.roof_detection.ransac import _exclude_unconnected, _plane_metrics, \
    _evaluate_candidate, _FitContext, _Thresholds, _SCORE_GATE_REASONS


_NEVER_INLIER = 9999


class DETSACRegressorForLIDAR:

    def __init__(self, *,
                 residual_threshold,
                 flat_roof_residual_threshold,
                 min_points_per_plane=8,
                 min_points_per_plane_perc=0.008,
                 min_convex_hull_ratio=0.65,
                 max_aspect_circular_mean_degrees=80,
                 max_aspect_circular_sd=1.5,
                 resolution_metres=1):
        """
        Deterministic variant of RANSAC for fitting roof planes to LIDAR: instead of
        randomly sampling points it iterates over premade candidate planes (see
        premade_planes.py). Only extracts one plane per fit() call, so should be
        re-run until it can't find any more, with the points in the found plane
        removed from the next round's input.

        :param min_points_per_plane_perc: min points per plane as a percentage of total
        points that fall within the building bounds. Default 0.8% (0.008). This will
        only affect larger buildings and stops it finding lots of tiny little sections.
        """
        self.residual_threshold = residual_threshold
        self.flat_roof_residual_threshold = flat_roof_residual_threshold
        self.min_points_per_plane = min_points_per_plane
        self.min_points_per_plane_perc = min_points_per_plane_perc
        self.min_convex_hull_ratio = min_convex_hull_ratio
        self.max_aspect_circular_mean_degrees = max_aspect_circular_mean_degrees
        self.max_aspect_circular_sd = max_aspect_circular_sd
        self.resolution_metres = resolution_metres

        self.sd = None
        self.plane_properties = {}
        self.success = False
        self.finished = False

    def fit(self, X, y,
            polygon: Polygon,
            premade_planes: List[Plane],
            skip_planes: Set[str],
            aspect: np.ndarray,
            mask: np.ndarray,
            total_points_in_building: int,
            debug: bool = False):
        base_estimator = LinearRegression()

        residual_threshold = self.residual_threshold

        loss_function = lambda y_true, y_pred: np.abs(y_true - y_pred)

        # RANSAC for LIDAR additions:
        min_X = [np.amin(X[:, 0]), np.amin(X[:, 1])]

        sd_best = np.inf
        if debug:
            bad_sample_reasons = defaultdict(int)

        n_inliers_best = 1
        score_best = np.inf
        inlier_mask_best = None
        X_inlier_best = None
        y_inlier_best = None
        inlier_best_idxs_subset = None
        best_sample_idxs = None
        sample_residual_threshold_best = None
        plane_properties_best = {}

        # number of data samples
        n_samples = X.shape[0]
        sample_idxs = np.arange(n_samples)

        ctx = _FitContext(
            thresholds=_Thresholds.from_regressor(self), X=X, y=y, aspect=aspect,
            polygon=polygon, min_X=min_X, sample_idxs=sample_idxs,
            total_points_in_building=total_points_in_building,
            aspect_fallback_to_circ_mean=True)

        if len(premade_planes) == len(skip_planes):
            self.finished = True
            return self

        self.n_trials_ = 0
        for plane in premade_planes:
            self.n_trials_ += 1

            if plane.plane_id in skip_planes:
                continue

            # residuals of all data for current random sample model
            base_estimator = plane.fit()
            y_pred = base_estimator.predict(X)
            residuals_subset = loss_function(y, y_pred)

            # RANSAC for LiDAR addition: use a more restrictive threshold for flat
            # roofs, as they are more likely to be covered with obstacles, HVAC, pipes etc
            slope = slope_deg(base_estimator.coef_[0], base_estimator.coef_[1])
            if slope <= FLAT_ROOF_DEGREES_THRESHOLD:
                residual_threshold = self.flat_roof_residual_threshold

            # DETSAC change: allow the initial sample points to be further from the plane
            m1 = residuals_subset < plane.sample_residual_threshold
            m2 = np.zeros(residuals_subset.shape, dtype=int)
            m2[plane.idxs] = 1
            residuals_subset_copy = residuals_subset.copy()
            residuals_subset_copy[(m1 & m2) == 1] = 0
            # never allow plane to be fit to points already on a different plane:
            residuals_subset_copy[mask == 0] = _NEVER_INLIER

            # classify data into inliers and outliers
            inlier_mask_subset = residuals_subset_copy < residual_threshold

            reason, cand = _evaluate_candidate(
                ctx, y_pred, residuals_subset, inlier_mask_subset,
                base_estimator.coef_, slope, score_best, n_inliers_best)
            if reason is not None:
                if debug:
                    bad_sample_reasons[reason] += 1
                # A sample rejected on score is just skipped this iteration; every other
                # rejection bans the premade plane for the rest of the fit.
                if reason not in _SCORE_GATE_REASONS:
                    skip_planes.add(plane.plane_id)
                continue

            if debug:
                print(f"new best score plane found. MAE {score_best} -> {cand.score} . inliers {n_inliers_best} -> {cand.n_inliers} .  Current trial: {self.n_trials_}")

            # save current best sample
            n_inliers_best = cand.n_inliers
            best_sample_idxs = plane.idxs
            score_best = cand.score
            sd_best = cand.sd
            plane_properties_best = cand.plane_properties(plane.plane_type, plane.plane_id)
            inlier_mask_best = cand.inlier_mask
            X_inlier_best = cand.X_inlier
            y_inlier_best = cand.y_inlier
            inlier_best_idxs_subset = cand.inlier_idxs
            sample_residual_threshold_best = plane.sample_residual_threshold

            # RANSAC for LiDAR addition:
            # I've disabled the dynamic max_trials thing as it's based on proportion of
            # inliers to outliers, which isn't the metric we care about. We could potentially
            # have another version that uses SD to predict how close we are to having a good
            # plane - or just have a min threshold SD where we say we're automatically happy.
            #
            # max_trials = min(
            #     max_trials,
            #     _dynamic_max_trials(n_inliers_best, n_samples,
            #                         min_samples, self.stop_probability))

        if debug:
            print("DETSAC finished.")

            print("Planes were rejected for the following reasons:")
            total = 0
            for rejection_reason, count in bad_sample_reasons.items():
                print(f"{rejection_reason}: {count}")
                total += count
            print(f"total rejected: {total}.")

        # if none of the iterations met the required criteria
        if inlier_mask_best is None:
            self.success = False
            self.finished = True
            return self

        # estimate final model using all inliers
        base_estimator.fit(X_inlier_best, y_inlier_best)

        # RANSAC for LIDAR change:
        # Re-fit data to final model:
        y_pred = base_estimator.predict(X)
        residuals_subset = loss_function(y, y_pred)

        # allow the initial sample points to be further from the plane,
        # and never allow plane to be fit to points already on a different plane:
        m1 = residuals_subset < sample_residual_threshold_best
        m2 = np.zeros(residuals_subset.shape, dtype=int)
        m2[best_sample_idxs] = 1
        residuals_subset[(m1 & m2) == 1] = 0
        residuals_subset[mask == 0] = _NEVER_INLIER

        inlier_mask_best = residuals_subset < residual_threshold
        mask_without_excluded = _exclude_unconnected(X, min_X, inlier_mask_best, res=self.resolution_metres)

        if np.sum(mask_without_excluded) < self.min_points_per_plane:
            self.success = False
        else:
            self.success = True

            self.estimator_ = base_estimator
            self.inlier_mask_ = mask_without_excluded
            self.sd = sd_best
            self.plane_properties = plane_properties_best

            self.plane_properties.update(_plane_metrics(
                base_estimator, X, y, mask_without_excluded, sample_idxs))

        skip_planes.add(plane_properties_best["plane_id"])

        if debug:
            if self.success:
                a, b = self.estimator_.coef_
                print(f"plane found: slope {slope_deg(a, b)} aspect {aspect_deg(a, b)} sd {self.sd} inliers {np.sum(mask_without_excluded)}")
            else:
                print(f"plane found, but rejected")
            print("")
        return self
