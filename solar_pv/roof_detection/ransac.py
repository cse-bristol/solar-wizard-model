# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
from collections import defaultdict
from typing import Set, Tuple, List, Optional

import numpy as np
import warnings
import math
from shapely.geometry import Polygon, MultiPoint
from shapely.strtree import STRtree
from sklearn import metrics

from sklearn.linear_model import RANSACRegressor
from skimage import measure, morphology
from sklearn.base import clone
from sklearn.linear_model import LinearRegression
from sklearn.utils import check_random_state, check_consistent_length
from sklearn.utils.random import sample_without_replacement
from sklearn.utils.validation import _check_sample_weight
from sklearn.utils.validation import has_fit_parameter
from sklearn.exceptions import ConvergenceWarning
from skimage.measure import perimeter_crofton

from solar_pv.constants import AZIMUTH_ALIGNMENT_THRESHOLD, \
    FLAT_ROOF_AZIMUTH_ALIGNMENT_THRESHOLD, ROOFDET_GOOD_SCORE, \
    FLAT_ROOF_DEGREES_THRESHOLD
from solar_pv.geos import simplify_by_angle, polygon_line_segments, azimuth_deg, slope_deg, \
    aspect_deg, aspect_rad, circular_mean_rad, circular_sd_rad, rad_diff, deg_diff


_NEVER_INLIER = 9999


class RANSACRegressorForLIDAR(RANSACRegressor):

    def __init__(self, base_estimator=None, *,
                 min_samples=None,
                 residual_threshold=None,
                 flat_roof_residual_threshold=None,
                 is_data_valid=None,
                 is_model_valid=None,
                 max_trials=100,
                 max_skips=np.inf,
                 stop_n_inliers=np.inf,
                 stop_score=np.inf,
                 stop_probability=0.99,
                 loss='absolute_loss',
                 random_state=None,
                 # RANSAC for LIDAR additions:
                 resolution_metres=1,
                 min_points_per_plane=8,
                 min_points_per_plane_perc=0.008,
                 max_slope=None,
                 min_slope=None,
                 min_convex_hull_ratio=0.65,
                 max_num_groups=20,
                 max_group_area_ratio_to_largest=0.02,
                 max_aspect_circular_mean_degrees=90,
                 max_aspect_circular_sd=1.5):
        """
        :param min_points_per_plane_perc: min points per plane as a percentage of total
        points that fall within the building bounds. Default 0.8% (0.008). This will
        only affect larger buildings and stops it finding lots of tiny little sections.

        :param max_num_groups: Maximum number of contiguous groups the inliers are
        allowed to fall in to.

        :param max_group_area_ratio_to_largest: Maximum ratio of the area of each other
        group to the area of the largest.
        """
        super().__init__(base_estimator,
                         min_samples=min_samples,
                         residual_threshold=residual_threshold,
                         is_data_valid=is_data_valid,
                         is_model_valid=is_model_valid,
                         max_trials=max_trials,
                         max_skips=max_skips,
                         stop_n_inliers=stop_n_inliers,
                         stop_score=stop_score,
                         stop_probability=stop_probability,
                         loss=loss,
                         random_state=random_state)
        self.min_points_per_plane = min_points_per_plane
        self.min_points_per_plane_perc = min_points_per_plane_perc
        self.max_slope = max_slope
        self.min_slope = min_slope
        self.min_convex_hull_ratio = min_convex_hull_ratio
        self.max_num_groups = max_num_groups
        self.max_group_area_ratio_to_largest = max_group_area_ratio_to_largest
        self.max_aspect_circular_mean_degrees = max_aspect_circular_mean_degrees
        self.max_aspect_circular_sd = max_aspect_circular_sd
        self.flat_roof_residual_threshold = flat_roof_residual_threshold

        self.sd = None
        self.plane_properties = {}
        self.resolution_metres = resolution_metres
        self.success = False
        self.finished = False

    def fit(self, X, y,
            sample_weight=None,
            # These are all optional parameters just so that the method matches
            # the base class signature... They are actually required!
            polygon: Polygon = None,
            skip_planes: Set[Tuple[int]] = None,
            aspect: np.ndarray = None,
            mask: np.ndarray = None,
            total_points_in_building: int = None,
            debug: bool = False):
        """
        Extended implementation of RANSAC with additions for usage with LIDAR
        to detect roof planes.

        Changes made:
        * Tarsha-Kurdi, 2007 recommends rejecting planes where the (x,y) points in the
        plane do not form a single contiguous region of the LIDAR. This mostly helps
        but does exclude some valid planes where the correctly-fitted plane also happens
        to fit to other pixels in disconnected areas of the roof. I have modified it to
        allow planes where a small number of non-contiguous pixels fit, as long as
        the area ratio of those non-contiguous pixels to the area of the main mass of
        contiguous pixels is small.

        * Do not optimise for number of points within `residual_threshold` distance
        from plane, instead optimise for lowest SD of all points within `residual_threshold`
        distance from plane (Tarsha-Kurdi, 2007). In a normal regression trying to fit as
        many points as possible makes sense, but for roof plane fitting we know it is
        very likely that there will be multiple planes to fit in a given data set, so
        fitting more is not necessarily better.

        * Give the option of forbidding very steep or shallow slopes (not sourced from
        a paper) - since we don't care about walls and the LIDAR is cropped to the
        building bounds the steep ones are likely to be false positives. I don't
        currently use the 'no shallow slopes' rule as it doesn't seem necessary.

        * Reject planes where the area of the polygon formed by the inliers in the xy
        plane is significantly less than the area of the convex hull of that polygon.
        This is intended to reject planes which have cut across a roof and so have a
        u-shaped intersect with the actual points.

        * Reject planes where the `thinness ratio` is too low - i.e the shape of the
        polygon is very long and thin. The `thinness ratio` is defined as
        `4 * pi * area / perimeter^2`, and is a standard GIS approach to detecting
        sliver polygons. Even if these were accurately detected roofs, they're no good
        for PV panels so we can safely ignore them.

        This only extracts one plane at a time so should be re-run until it can't find
        any more, with the points in the found plane removed from the next round's input.
        """
        # Need to validate separately here.
        # We can't pass multi_ouput=True because that would allow y to be csr.
        check_X_params = dict(accept_sparse='csr')
        check_y_params = dict(ensure_2d=False)
        X, y = self._validate_data(X, y, validate_separately=(check_X_params,
                                                              check_y_params))
        check_consistent_length(X, y)

        if self.base_estimator is not None:
            base_estimator = clone(self.base_estimator)
        else:
            base_estimator = LinearRegression()

        if self.min_samples is None:
            # assume linear model by default
            min_samples = X.shape[1] + 1
        elif 0 < self.min_samples < 1:
            min_samples = np.ceil(self.min_samples * X.shape[0])
        elif self.min_samples >= 1:
            if self.min_samples % 1 != 0:
                raise ValueError("Absolute number of samples must be an "
                                 "integer value.")
            min_samples = self.min_samples
        else:
            raise ValueError("Value for `min_samples` must be scalar and "
                             "positive.")
        if min_samples > X.shape[0]:
            raise ValueError("`min_samples` may not be larger than number "
                             "of samples: n_samples = %d." % (X.shape[0]))

        if self.stop_probability < 0 or self.stop_probability > 1:
            raise ValueError("`stop_probability` must be in range [0, 1].")

        if self.residual_threshold is None:
            # MAD (median absolute deviation)
            residual_threshold = np.median(np.abs(y - np.median(y)))
        else:
            residual_threshold = self.residual_threshold

        if self.loss == "absolute_loss":
            if y.ndim == 1:
                loss_function = lambda y_true, y_pred: np.abs(y_true - y_pred)
            else:
                loss_function = lambda \
                    y_true, y_pred: np.sum(np.abs(y_true - y_pred), axis=1)

        elif self.loss == "squared_loss":
            if y.ndim == 1:
                loss_function = lambda y_true, y_pred: (y_true - y_pred) ** 2
            else:
                loss_function = lambda \
                    y_true, y_pred: np.sum((y_true - y_pred) ** 2, axis=1)

        elif callable(self.loss):
            loss_function = self.loss

        else:
            raise ValueError(
                "loss should be 'absolute_loss', 'squared_loss' or a callable."
                "Got %s. " % self.loss)

        random_state = check_random_state(self.random_state)

        try:  # Not all estimator accept a random_state
            base_estimator.set_params(random_state=random_state)
        except ValueError:
            pass

        estimator_fit_has_sample_weight = has_fit_parameter(base_estimator,
                                                            "sample_weight")
        estimator_name = type(base_estimator).__name__
        if (sample_weight is not None and not
                estimator_fit_has_sample_weight):
            raise ValueError("%s does not support sample_weight. Samples"
                             " weights are only used for the calibration"
                             " itself." % estimator_name)
        if sample_weight is not None:
            sample_weight = _check_sample_weight(sample_weight, X)

        # RANSAC for LIDAR additions:
        min_X = [np.amin(X[:, 0]), np.amin(X[:, 1])]

        sd_best = np.inf
        bad_samples = set()
        if debug:
            bad_sample_reasons = defaultdict(int)

        n_inliers_best = 1
        score_best = np.inf
        inlier_mask_best = None
        X_inlier_best = None
        y_inlier_best = None
        inlier_best_idxs_subset = None
        best_sample_idxs = None
        self.n_skips_no_inliers_ = 0
        self.n_skips_invalid_data_ = 0
        self.n_skips_invalid_model_ = 0

        # number of data samples
        n_samples = X.shape[0]
        sample_idxs = np.arange(n_samples)

        self.n_trials_ = 0
        max_trials = self.max_trials
        while self.n_trials_ < max_trials:
            self.n_trials_ += 1

            if (self.n_skips_no_inliers_ + self.n_skips_invalid_data_ +
                    self.n_skips_invalid_model_) > self.max_skips:
                break

            # choose random sample set
            subset_idxs = _sample(n_samples, min_samples, random_state=random_state, mask=mask)
            if subset_idxs is None:
                self.success = False
                self.finished = True
                return self

            # RANSAC for LIDAR addition:
            if tuple(subset_idxs) in bad_samples or tuple(subset_idxs) in skip_planes:
                if debug:
                    bad_sample_reasons["ALREADY_SAMPLED"] += 1
                continue

            X_subset = X[subset_idxs]
            y_subset = y[subset_idxs]

            # fit model for current random sample set
            base_estimator.fit(X_subset, y_subset)

            # RANSAC for LIDAR addition: if slope of fit plane is too steep ...
            slope = slope_deg(base_estimator.coef_[0], base_estimator.coef_[1])
            if self.max_slope and slope > self.max_slope:
                skip_planes.add(tuple(subset_idxs))
                if debug:
                    bad_sample_reasons["MAX_SLOPE"] += 1
                continue
            # RANSAC for LIDAR addition: if slope too shallow ...
            if self.min_slope and slope < self.min_slope:
                skip_planes.add(tuple(subset_idxs))
                if debug:
                    bad_sample_reasons["MIN_SLOPE"] += 1
                continue

            # RANSAC for LiDAR addition: use a more restrictive threshold for flat
            # roofs, as they are more likely to be covered with obstacles, HVAC, pipes etc
            if slope <= FLAT_ROOF_DEGREES_THRESHOLD:
                residual_threshold = self.flat_roof_residual_threshold

            # residuals of all data for current random sample model
            y_pred = base_estimator.predict(X)
            residuals_subset = loss_function(y, y_pred)
            # don't allow plane to be fit to points already on a different plane:
            residuals_subset[mask == 0] = _NEVER_INLIER

            # classify data into inliers and outliers
            inlier_mask_subset = residuals_subset < residual_threshold
            n_inliers_subset = np.sum(inlier_mask_subset)

            # less inliers -> skip current random sample
            # if n_inliers_subset < n_inliers_best:
            #     self.n_skips_no_inliers_ += 1
            #     continue
            # RANSAC for LIDAR addition: don't optimise for number of points
            # fit to plane.
            # See Tarsha-Kurdi, 2007
            if n_inliers_subset < self.min_points_per_plane:
                skip_planes.add(tuple(subset_idxs))
                self.n_skips_no_inliers_ += 1
                if debug:
                    bad_sample_reasons["MIN_POINTS_PER_PLANE"] += 1
                continue

            # extract inlier data set
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
                    # We don't add the sample to `skip_planes` here as it might still be
                    # the best sample in a subsequent run of RANSAC, but we still want to
                    # skip them within this run...
                    bad_samples.add(tuple(subset_idxs))
                    if debug:
                        bad_sample_reasons["LESS_INLIERS"] += 1
                    continue
            elif score_subset > score_best or (score_subset == score_best and n_inliers_subset <= n_inliers_best):
                # We don't add the sample to `skip_planes` here as it might still be
                # the best sample in a subsequent run of RANSAC, but we still want to
                # skip them within this run...
                bad_samples.add(tuple(subset_idxs))
                if debug:
                    bad_sample_reasons["WORSE_SCORE"] += 1
                continue

            # RANSAC for LIDAR addition: use stddev of inlier distance to plane
            # as score instead
            # See Tarsha-Kurdi, 2007
            # if sd > sd_best or (sd == sd_best and n_inliers_subset <= n_inliers_best):
            #     # We don't add the sample to `skip_planes` here as it might still be
            #     # the best sample in a subsequent run of RANSAC, but we still want to
            #     # skip them within this run...
            #     bad_samples.add(tuple(subset_idxs))
            #     if debug:
            #         bad_sample_reasons["WORSE_SD"] += 1
            #     continue

            # RANSAC for LIDAR addition:
            # if difference between circular mean of pixel aspects and slope aspect is too high:
            # if circular deviation of pixel aspects too high:
            if slope > FLAT_ROOF_DEGREES_THRESHOLD:
                aspect_inliers = np.radians(aspect[inlier_mask_subset])
                plane_aspect = aspect_rad(base_estimator.coef_[0], base_estimator.coef_[1])
                aspect_circ_mean = circular_mean_rad(aspect_inliers)
                aspect_diff = rad_diff(plane_aspect, aspect_circ_mean)
                if aspect_diff > math.radians(self.max_aspect_circular_mean_degrees):
                    skip_planes.add(tuple(subset_idxs))
                    if debug:
                        bad_sample_reasons["CIRCULAR_MEAN"] += 1
                    continue

                aspect_circ_sd = circular_sd_rad(aspect_inliers)
                if aspect_circ_sd > self.max_aspect_circular_sd:
                    skip_planes.add(tuple(subset_idxs))
                    if debug:
                        bad_sample_reasons["CIRCULAR_SD"] += 1
                    continue
            else:
                aspect_circ_sd = None
                aspect_circ_mean = None

            # TODO in DETSAC I moved all this to before the score/SD/inliers check, as
            #      we only care about those things (as well as the circ mean etc) for
            #      the connected inliers. That might be too expensive in standard RANSAC but it needs considering
            # RANSAC for LIDAR addition: prep for following plane morphology checks
            groups, num_groups = _pixel_groups(X_inlier_subset, min_X, self.resolution_metres)
            group_areas = _group_areas(groups)

            # RANSAC for LIDAR addition: check that size of the largest continuous
            # group of pixels is also over the minimum number of points per plane:
            largest = max(group_areas, key=group_areas.get)
            roof_plane_area = group_areas[largest]
            if roof_plane_area < self.min_points_per_plane or roof_plane_area < (
                    total_points_in_building * self.min_points_per_plane_perc):
                if debug:
                    bad_sample_reasons["MIN_POINTS_PER_LARGEST_GROUP"] += 1
                skip_planes.add(tuple(subset_idxs))
                continue

            # re-extract (connected) inlier data set
            inlier_mask_subset = _exclude_unconnected(X, min_X, inlier_mask_subset, res=self.resolution_metres)
            inlier_idxs_subset = sample_idxs[inlier_mask_subset]

            # RANSAC for LiDAR addition: check ratio of points area to ratio of convex
            # hull of points area.
            # If the convex hull's area is significantly larger, it's likely to be a
            # bad plane that cuts through the roof at an angle
            only_largest = groups == largest
            convex_hull = morphology.convex_hull_image(only_largest)
            convex_hull_area = np.count_nonzero(convex_hull)
            cv_hull_ratio = roof_plane_area / convex_hull_area
            if cv_hull_ratio < self.min_convex_hull_ratio:
                skip_planes.add(tuple(subset_idxs))
                if debug:
                    bad_sample_reasons["CONVEX_HULL_RATIO"] += 1
                continue

            # RANSAC for LiDAR addition: thinness ratio check
            perimeter = perimeter_crofton(only_largest, directions=4)
            thinness_ratio = (4 * np.pi * roof_plane_area) / (perimeter * perimeter)
            if thinness_ratio < _min_thinness_ratio(roof_plane_area):
                skip_planes.add(tuple(subset_idxs))
                if debug:
                    bad_sample_reasons["THINNESS_RATIO"] += 1
                continue

            azimuths = _get_potential_aspects(X_inlier_subset, polygon)
            if len(azimuths) == 0:
                if debug:
                    bad_sample_reasons["NO_NEARBY_FACE"] += 1
                skip_planes.add(tuple(subset_idxs))
                continue

            if slope > FLAT_ROOF_DEGREES_THRESHOLD:
                target_az = aspect_deg(base_estimator.coef_[0], base_estimator.coef_[1])
                az_diff_thresh = AZIMUTH_ALIGNMENT_THRESHOLD
            else:
                target_az = 180
                az_diff_thresh = FLAT_ROOF_AZIMUTH_ALIGNMENT_THRESHOLD

            az = min(azimuths, key=lambda az_: deg_diff(az_, target_az))
            if deg_diff(az, target_az) < az_diff_thresh:
                aspect_deg_ = az
            else:
                if debug:
                    bad_sample_reasons["NO_CLOSE_ASPECT"] += 1
                skip_planes.add(tuple(subset_idxs))
                continue

            if debug:
                print(f"new best score plane found. MAE {score_best} -> {score_subset} . inliers {n_inliers_best} -> {n_inliers_subset} .  Current trial: {self.n_trials_}")

            # save current random sample as best sample
            n_inliers_best = n_inliers_subset
            sd_best = sd
            score_best = score_subset

            plane_properties_best = {
                "sd": sd_best,
                "score": score_best,
                "aspect_circ_mean": math.degrees(aspect_circ_mean) if aspect_circ_mean else None,
                "aspect_circ_sd": aspect_circ_sd,
                "thinness_ratio": thinness_ratio,
                "cv_hull_ratio": cv_hull_ratio,
                "plane_type": "RANSAC",
                "plane_id": f"RANSAC_{tuple(sample_idxs)}",
                "aspect_adjusted": aspect_deg_,
            }

            inlier_mask_best = inlier_mask_subset
            X_inlier_best = X_inlier_subset
            y_inlier_best = y_inlier_subset
            inlier_best_idxs_subset = inlier_idxs_subset
            best_sample_idxs = sample_idxs

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

            # break if sufficient number of inliers
            if n_inliers_best >= self.stop_n_inliers:
                break

        if debug:
            print("RANSAC finished.")

            print("Planes were rejected for the following reasons:")
            total = 0
            for rejection_reason, count in bad_sample_reasons.items():
                print(f"{rejection_reason}: {count}")
                total += count
            print(f"total rejected: {total}. max trials: {max_trials}")

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
        # don't allow plane to be fit to points already on a different plane:
        residuals_subset[mask == 0] = _NEVER_INLIER
        inlier_mask_best = residuals_subset < residual_threshold
        mask_without_excluded = _exclude_unconnected(X, min_X, inlier_mask_best, res=self.resolution_metres)

        if np.sum(mask_without_excluded) < self.min_points_per_plane:
            self.success = False
            skip_planes.add(tuple(best_sample_idxs))
        else:
            self.success = True
            self.estimator_ = base_estimator
            self.inlier_mask_ = mask_without_excluded
            self.sd = sd_best
            self.plane_properties = plane_properties_best

            inlier_idxs_subset = sample_idxs[mask_without_excluded]
            y_true = y[inlier_idxs_subset]
            y_pred = self.estimator_.predict(X[inlier_idxs_subset])

            a, b = base_estimator.coef_
            d = base_estimator.intercept_
            self.plane_properties.update({
                "x_coef": a,
                "y_coef": b,
                "intercept": d,
                "slope": slope_deg(a, b),
                "aspect": aspect_deg(a, b),
                "inliers_xy": X[mask_without_excluded],
                "r2": metrics.r2_score(y_true, y_pred),
                "mae": metrics.mean_absolute_error(y_true, y_pred),
                "mse": metrics.mean_squared_error(y_true, y_pred),
                "rmse": metrics.mean_squared_error(y_true, y_pred, squared=False),
                "msle": metrics.mean_squared_log_error(y_true, y_pred),
                "mape": metrics.mean_absolute_percentage_error(y_true, y_pred),
            })

        if debug:
            if self.success:
                a, b = self.estimator_.coef_
                print(f"plane found: slope {slope_deg(a, b)} aspect {aspect_deg(a, b)} sd {self.sd} inliers {np.sum(mask_without_excluded)}")
            else:
                print(f"plane found, but rejected")
            print("")
        return self


def _exclude_unconnected(X, min_X, inlier_mask_best, res: float):
    """
    Create a new inlier mask which only sets as True those LIDAR pixels that
    form part of the largest contiguous group of pixels fitted to the plane.
    """

    normed = ((X - min_X) / res).astype(int)
    image = np.zeros((int(np.amax(normed[:, 0])) + 1,
                      int(np.amax(normed[:, 1])) + 1))
    idxs = np.zeros((int(np.amax(normed[:, 0])) + 1,
                     int(np.amax(normed[:, 1])) + 1), dtype=int)
    for i, pair in enumerate(normed):
        if inlier_mask_best[i]:
            image[pair[0]][pair[1]] = 1
        idxs[pair[0]][pair[1]] = i

    groups, num_groups = measure.label(image, connectivity=1, return_num=True)
    if num_groups == 0:
        return np.zeros(inlier_mask_best.shape, dtype=bool)

    group_areas = _group_areas(groups)
    largest_area_group = max(group_areas, key=group_areas.get)

    idx_subset = idxs[groups == largest_area_group]
    mask = np.zeros(inlier_mask_best.shape, dtype=bool)
    mask[idx_subset] = True
    return mask


def _sample(n_samples, min_samples, random_state, mask):
    sample_attempts = 0

    while sample_attempts < 1000:
        sample_attempts += 1
        sample = sample_without_replacement(n_samples, min_samples, random_state=random_state)
        masked = mask[sample]
        if np.all(masked):
            return sample

    return None


def _pixel_groups(X_inlier_subset, min_X, res: float):
    normed_inliers = ((X_inlier_subset - min_X) / res).astype(int)

    image = np.zeros((int(np.amax(normed_inliers[:, 0])) + 1,
                      int(np.amax(normed_inliers[:, 1])) + 1))
    image[normed_inliers[:, 0], normed_inliers[:, 1]] = 1

    return measure.label(image, connectivity=1, return_num=True)


def _group_areas(groups) -> dict:
    group_areas = dict(enumerate(np.bincount(np.ravel(groups))))
    if 0 in group_areas:
        del group_areas[0]
    return group_areas


def _min_thinness_ratio(area) -> float:
    """
    See dev_ransac.py thinness_ratio_experiments() for working out what numbers
    work for which areas.
    Could probably fit some kind of curve to these numbers but that sounds
    like more effort than it's worth.
    """
    if area <= 20:
        return 0.45
    elif area <= 30:
        return 0.45
    elif area <= 40:
        return 0.45
    elif area <= 50:
        return 0.4
    elif area <= 300:
        return 0.24
    elif area <= 500:
        return 0.2
    elif area <= 750:
        return 0.15
    elif area <= 1000:
        return 0.10
    elif area <= 2000:
        return 0.10
    elif area <= 3000:
        return 0.10
    else:
        return 0.07


def _get_potential_aspects(X_inlier_subset, polygon: Polygon) -> List[int]:
    polygon = simplify_by_angle(polygon, tolerance_degrees=2.0)
    line_segments = polygon_line_segments(polygon, min_length=1.0)
    mp = MultiPoint(X_inlier_subset)
    rp = mp.buffer(1.0)
    rtree = STRtree(line_segments)
    nearby = rtree.query(rp)
    if len(nearby) == 0:
        rp = mp.buffer(3.0)
        nearby = rtree.query(rp)
    if len(nearby) == 0:
        rp = mp.buffer(10.0)
        nearby = rtree.query(rp)
    if len(nearby) == 0:
        return []

    azimuths_base = [int(azimuth_deg(ls.coords[0], ls.coords[1])) for ls in line_segments]
    azimuths = set(azimuths_base)
    for az in azimuths_base:
        azimuths.add((az + 90) % 360)
        azimuths.add((az + 180) % 360)
        azimuths.add((az + 270) % 360)
    return list(azimuths)


def closest_azimuth(azimuths: List[float], aspect: float, thresh: float) -> Optional[float]:
    az = min(azimuths, key=lambda az_: deg_diff(az_, aspect))
    if deg_diff(az, aspect) < thresh:
        return az
    else:
        return None
