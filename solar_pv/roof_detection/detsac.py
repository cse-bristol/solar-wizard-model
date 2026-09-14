# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
from collections import defaultdict
from typing import List, Set

import numpy as np
import math
from shapely.geometry import Polygon
from sklearn.linear_model import LinearRegression
from sklearn import metrics

from solar_pv.constants import ROOFDET_GOOD_SCORE, AZIMUTH_ALIGNMENT_THRESHOLD, \
    FLAT_ROOF_AZIMUTH_ALIGNMENT_THRESHOLD, FLAT_ROOF_DEGREES_THRESHOLD
from solar_pv.geos import slope_deg, aspect_deg
from solar_pv.roof_detection.premade_planes import Plane
from solar_pv.roof_detection.ransac import _exclude_unconnected, \
    _pixel_groups, _group_areas, _min_thinness_ratio, get_potential_aspects, \
    closest_azimuth, _convex_hull_ratio, _thinness_ratio, _aspect_stats, _plane_metrics


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
            n_inliers_subset = np.sum(inlier_mask_subset)

            # less inliers -> skip current random sample
            # if n_inliers_subset < n_inliers_best:
            #     bad_sample_reasons["LESS_INLIERS"] += 1
            #     self.n_skips_no_inliers_ += 1
            #     continue
            # RANSAC for LIDAR addition: don't optimise for number of points
            # fit to plane.
            # See Tarsha-Kurdi, 2007
            if n_inliers_subset < self.min_points_per_plane:
                if debug:
                    bad_sample_reasons["MIN_POINTS_PER_PLANE"] += 1
                skip_planes.add(plane.plane_id)
                continue

            # extract inlier data set
            inlier_idxs_subset = sample_idxs[inlier_mask_subset]

            # RANSAC for LIDAR addition: prep for following plane morphology checks
            groups, num_groups = _pixel_groups(X[inlier_idxs_subset], min_X, self.resolution_metres)
            group_areas = _group_areas(groups)

            # RANSAC for LIDAR addition: check that size of the largest continuous
            # group of pixels is also over the minimum number of points per plane:
            largest = max(group_areas, key=group_areas.get)
            roof_plane_area = group_areas[largest]
            if roof_plane_area < self.min_points_per_plane or roof_plane_area < (
                    total_points_in_building * self.min_points_per_plane_perc):
                if debug:
                    bad_sample_reasons["MIN_POINTS_PER_LARGEST_GROUP"] += 1
                skip_planes.add(plane.plane_id)
                continue

            # re-extract (connected) inlier data set
            inlier_mask_subset = _exclude_unconnected(X, min_X, inlier_mask_subset, res=self.resolution_metres)
            inlier_idxs_subset = sample_idxs[inlier_mask_subset]
            X_inlier_subset = X[inlier_idxs_subset]
            y_inlier_subset = y[inlier_idxs_subset]
            y_inlier_pred = y_pred[inlier_idxs_subset]

            # score of inlier data set
            score_subset = metrics.mean_absolute_error(y_inlier_subset, y_inlier_pred)
            # score_subset = base_estimator.score(X_inlier_subset, y_inlier_subset)

            sd = np.std(residuals_subset[inlier_mask_subset])

            if score_subset < ROOFDET_GOOD_SCORE and score_best < ROOFDET_GOOD_SCORE:
                if n_inliers_subset <= n_inliers_best or (n_inliers_subset == n_inliers_best and score_subset > score_best):
                    if debug:
                        bad_sample_reasons["LESS_INLIERS"] += 1
                    continue
            elif score_subset > score_best or (score_subset == score_best and n_inliers_subset <= n_inliers_best):
                if debug:
                    bad_sample_reasons["WORSE_SCORE"] += 1
                continue

            # same number of inliers but worse score -> skip current random
            # sample
            # if (n_inliers_subset == n_inliers_best
            #         and sd > sd_best):
            #     bad_sample_reasons["WORSE_SD"] += 1
            #     continue
            # RANSAC for LIDAR addition: use stddev of inlier distance to plane
            # as score instead
            # See Tarsha-Kurdi, 2007
            # if sd > sd_best or (sd == sd_best and n_inliers_subset <= n_inliers_best):
            #     if debug:
            #         bad_sample_reasons["WORSE_SD"] += 1
            #     continue

            # RANSAC for LIDAR addition:
            # if difference between circular mean of pixel aspects and slope aspect is too high:
            # if circular deviation of pixel aspects too high:
            aspect_circ_mean, aspect_circ_sd, aspect_diff = _aspect_stats(
                aspect, inlier_mask_subset, base_estimator.coef_[0], base_estimator.coef_[1])

            if slope > FLAT_ROOF_DEGREES_THRESHOLD:
                if aspect_diff > math.radians(self.max_aspect_circular_mean_degrees):
                    if debug:
                        bad_sample_reasons["CIRCULAR_MEAN"] += 1
                    skip_planes.add(plane.plane_id)
                    continue

                if aspect_circ_sd > self.max_aspect_circular_sd:
                    if debug:
                        bad_sample_reasons["CIRCULAR_SD"] += 1
                    skip_planes.add(plane.plane_id)
                    continue

            # RANSAC for LiDAR addition: check ratio of points area to ratio of convex
            # hull of points area.
            # If the convex hull's area is significantly larger, it's likely to be a
            # bad plane that cuts through the roof at an angle
            cv_hull_ratio, only_largest = _convex_hull_ratio(groups, largest, roof_plane_area)
            if cv_hull_ratio < self.min_convex_hull_ratio:
                if debug:
                    bad_sample_reasons["CONVEX_HULL_RATIO"] += 1
                skip_planes.add(plane.plane_id)
                continue

            # RANSAC for LiDAR addition: thinness ratio check
            thinness_ratio = _thinness_ratio(only_largest, roof_plane_area)
            if thinness_ratio < _min_thinness_ratio(roof_plane_area):
                if debug:
                    bad_sample_reasons["THINNESS_RATIO"] += 1
                skip_planes.add(plane.plane_id)
                continue

            azimuths = get_potential_aspects(X_inlier_subset, polygon)
            if len(azimuths) == 0:
                if debug:
                    bad_sample_reasons["NO_NEARBY_FACE"] += 1
                skip_planes.add(plane.plane_id)
                continue

            if slope > FLAT_ROOF_DEGREES_THRESHOLD:
                target_az = aspect_deg(base_estimator.coef_[0], base_estimator.coef_[1])
                az_diff_thresh = AZIMUTH_ALIGNMENT_THRESHOLD
                aspect_deg_ = closest_azimuth(azimuths, target_az, az_diff_thresh)
                if aspect_deg_ is None:
                    aspect_deg_ = closest_azimuth(azimuths, math.degrees(aspect_circ_mean), az_diff_thresh)
            else:
                target_az = 180
                az_diff_thresh = FLAT_ROOF_AZIMUTH_ALIGNMENT_THRESHOLD
                aspect_deg_ = closest_azimuth(azimuths, target_az, az_diff_thresh)

            if aspect_deg_ is None:
                if debug:
                    bad_sample_reasons["NO_CLOSE_ASPECT"] += 1
                skip_planes.add(plane.plane_id)
                continue

            if debug:
                # print(f"new best SD plane found. SD {sd}. Old SD {sd_best}. Current trial: {self.n_trials_}")
                print(f"new best score plane found. MAE {score_best} -> {score_subset} . inliers {n_inliers_best} -> {n_inliers_subset} .  Current trial: {self.n_trials_}")

            # save current random sample as best sample
            n_inliers_best = n_inliers_subset
            best_sample_idxs = plane.idxs
            score_best = score_subset
            sd_best = sd

            plane_properties_best = {
                "sd": sd_best,
                "score": score_best,
                "aspect_circ_mean": math.degrees(aspect_circ_mean) if aspect_circ_mean else None,
                "aspect_circ_sd": aspect_circ_sd,
                "thinness_ratio": thinness_ratio,
                "cv_hull_ratio": cv_hull_ratio,
                "plane_type": plane.plane_type,
                "plane_id": plane.plane_id,
                "aspect": aspect_deg_,
            }
            inlier_mask_best = inlier_mask_subset
            X_inlier_best = X_inlier_subset
            y_inlier_best = y_inlier_subset
            inlier_best_idxs_subset = inlier_idxs_subset
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
