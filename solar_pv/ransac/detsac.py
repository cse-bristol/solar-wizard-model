# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import itertools
from collections import defaultdict
from dataclasses import dataclass
from typing import Tuple, List

import numpy as np
import warnings
import math
from scipy import ndimage
from shapely.geometry import LineString
from skimage.morphology import local_minima
from skimage.segmentation import watershed

from sklearn.linear_model import RANSACRegressor
from skimage import measure, morphology, segmentation, color, graph
from sklearn.base import clone
from sklearn.linear_model import LinearRegression
from sklearn.utils import check_random_state, check_consistent_length
from sklearn.utils.random import sample_without_replacement
from sklearn.utils.validation import _check_sample_weight
from sklearn.utils.validation import has_fit_parameter
from sklearn.exceptions import ConvergenceWarning
from skimage.measure import perimeter_crofton

from solar_pv.ransac.premade_planes import Plane, ArrayPlane
from solar_pv.ransac.ransac import _slope, _aspect, _aspect_rad, _circular_mean, \
    _circular_sd, _circular_variance, _rad_diff, _to_positive_angle, _exclude_unconnected, \
    _sample, _pixel_groups, _group_areas, _min_thinness_ratio, RANSACValueError
from solar_pv.ransac.ridge_test import ridges


class DETSACRegressorForLIDAR(RANSACRegressor):

    def __init__(self,
                 # DETSAC:
                 sample_residual_thresholds: List[float],
                 # base:
                 base_estimator=None, *,
                 min_samples=None,
                 residual_threshold=None,
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
                 flat_roof_residual_threshold=None,
                 resolution_metres=1,
                 min_points_per_plane=8,
                 min_points_per_plane_perc=0.008,
                 max_slope=None,
                 min_slope=None,
                 min_convex_hull_ratio=0.65,
                 max_num_groups=20,
                 max_group_area_ratio_to_largest=0.02,
                 flat_roof_threshold_degrees=5,
                 max_aspect_circular_mean_degrees=80,
                 max_aspect_circular_sd=1.5):
        """
        :param min_points_per_plane_perc: min points per plane as a percentage of total
        points that fall within the building bounds. Default 0.8% (0.008). This will
        only affect larger buildings and stops it finding lots of tiny little sections.

        :param max_num_groups: Maximum number of contiguous groups the inliers are
        allowed to fall in to.

        :param max_group_area_ratio_to_largest: Maximum ratio of the area of each other
        group to the area of the largest.

        :param sample_residual_thresholds: residual thresholds to use for points in the
        sample.
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
        self.sample_residual_thresholds = sample_residual_thresholds
        self.min_points_per_plane = min_points_per_plane
        self.min_points_per_plane_perc = min_points_per_plane_perc
        self.max_slope = max_slope
        self.min_slope = min_slope
        self.min_convex_hull_ratio = min_convex_hull_ratio
        self.max_num_groups = max_num_groups
        self.max_group_area_ratio_to_largest = max_group_area_ratio_to_largest
        self.flat_roof_threshold_degrees = flat_roof_threshold_degrees
        self.max_aspect_circular_mean_degrees = max_aspect_circular_mean_degrees
        self.max_aspect_circular_sd = max_aspect_circular_sd
        self.flat_roof_residual_threshold = flat_roof_residual_threshold

        self.sd = None
        self.plane_properties = {}
        self.resolution_metres = resolution_metres

    def fit(self, X, y,
            sample_weight=None,
            premade_planes: List[ArrayPlane] = None,
            aspect=None,
            mask=None,
            total_points_in_building: int = None,
            include_group_checks: bool = True,
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

        * Constrain the selection of the initial sample of 3 points to points whose
        detected aspect is close (not sourced from a paper) aspect can be detected
        using a tool like SAGA or GDAL.

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
                raise RANSACValueError("Absolute number of samples must be an "
                                       "integer value.")
            min_samples = self.min_samples
        else:
            raise RANSACValueError("Value for `min_samples` must be scalar and "
                             "positive.")
        if min_samples > X.shape[0]:
            raise RANSACValueError("`min_samples` may not be larger than number "
                                   "of samples: n_samples = %d." % (X.shape[0]))

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
            raise RANSACValueError(
                "loss should be 'absolute_loss', 'squared_loss' or a callable."
                "Got %s. " % self.loss)

        random_state = check_random_state(self.random_state)
        # commented out, seed is enormous:
        # if debug:
        #     print(f"random state: {random_state.get_state()}")

        try:  # Not all estimator accept a random_state
            base_estimator.set_params(random_state=random_state)
        except ValueError:
            pass

        estimator_fit_has_sample_weight = has_fit_parameter(base_estimator,
                                                            "sample_weight")
        estimator_name = type(base_estimator).__name__
        if (sample_weight is not None and not
                estimator_fit_has_sample_weight):
            raise RANSACValueError("%s does not support sample_weight. Samples"
                                   " weights are only used for the calibration"
                                   " itself." % estimator_name)
        if sample_weight is not None:
            sample_weight = _check_sample_weight(sample_weight, X)

        # RANSAC for LIDAR additions:
        min_X = [np.amin(X[:, 0]), np.amin(X[:, 1])]

        # TODO move somewhere else
        # _test_ridges(X, min_X, y, self.resolution_metres)
        # _test_watershed(X, min_X, y, self.resolution_metres)
        # _test_flow(X, min_X, y, self.resolution_metres)
        # _segment_aspect(X, min_X, aspect, self.resolution_metres)

        sd_best = np.inf
        bad_samples = set()
        if debug:
            bad_sample_reasons = defaultdict(int)

        n_inliers_best = 1
        score_best = -np.inf
        inlier_mask_best = None
        X_inlier_best = None
        y_inlier_best = None
        inlier_best_idxs_subset = None
        best_sample_idxs = None
        sample_residual_threshold_best = None
        plane_properties_best = {}
        self.n_skips_no_inliers_ = 0
        self.n_skips_invalid_data_ = 0
        self.n_skips_invalid_model_ = 0

        # number of data samples
        n_samples = X.shape[0]
        sample_idxs = np.arange(n_samples)

        self.n_trials_ = 0
        for plane, sample_residual_threshold in itertools.product(premade_planes, self.sample_residual_thresholds):
            self.n_trials_ += 1

            if (self.n_skips_no_inliers_ + self.n_skips_invalid_data_ +
                    self.n_skips_invalid_model_) > self.max_skips:
                break

            # residuals of all data for current random sample model
            base_estimator = plane.fit()
            y_pred = base_estimator.predict(X)
            residuals_subset = loss_function(y, y_pred)

            # RANSAC for LiDAR addition: use a more restrictive threshold for flat
            # roofs, as they are more likely to be covered with obstacles, HVAC, pipes etc
            slope = _slope(base_estimator.coef_[0], base_estimator.coef_[1])
            if slope <= self.flat_roof_threshold_degrees:
                residual_threshold = self.flat_roof_residual_threshold

            # DETSAC change: allow the initial sample points to be further from the plane,
            # and never allow plane to be fit to points already on a different plane:
            m1 = residuals_subset < sample_residual_threshold
            m2 = np.zeros(residuals_subset.shape, dtype=int)
            m2[plane.idxs] = 1
            residuals_subset_copy = residuals_subset.copy()
            residuals_subset_copy[(m1 & m2) == 1] = 0
            residuals_subset_copy[mask == 0] = 9999  # TODO constant

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
                self.n_skips_no_inliers_ += 1
                if debug:
                    bad_sample_reasons["MIN_POINTS_PER_PLANE"] += 1
                continue

            # extract inlier data set
            inlier_idxs_subset = sample_idxs[inlier_mask_subset]
            X_inlier_subset = X[inlier_idxs_subset]
            y_inlier_subset = y[inlier_idxs_subset]

            # score of inlier data set
            score_subset = base_estimator.score(X_inlier_subset,
                                                y_inlier_subset)

            if score_subset < score_best or (score_subset == score_best and n_inliers_subset <= n_inliers_best):
                if debug:
                    bad_sample_reasons["WORSE_SCORE"] += 1
                continue

            # RANSAC for LIDAR addition: use stddev of inlier distance to plane
            # for score
            sd = np.std(residuals_subset[inlier_mask_subset])

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

            # TODO can maybe remove?
            # RANSAC for LIDAR addition:
            # if difference between circular mean of pixel aspects and slope aspect is too high:
            # if circular deviation of pixel aspects too high:
            if slope > self.flat_roof_threshold_degrees:
                aspect_inliers = np.radians(aspect[inlier_mask_subset])
                plane_aspect = _aspect_rad(base_estimator.coef_[0], base_estimator.coef_[1])
                aspect_circ_mean = _circular_mean(aspect_inliers)
                aspect_diff = _rad_diff(plane_aspect, aspect_circ_mean)
                if aspect_diff > math.radians(self.max_aspect_circular_mean_degrees):
                    if debug:
                        bad_sample_reasons["CIRCULAR_MEAN"] += 1
                    continue

                aspect_circ_sd = _circular_sd(aspect_inliers)
                if aspect_circ_sd > self.max_aspect_circular_sd:
                    if debug:
                        bad_sample_reasons["CIRCULAR_SD"] += 1
                    continue
            else:
                aspect_circ_sd = None
                aspect_circ_mean = None

            # RANSAC for LIDAR addition: prep for following plane morphology checks
            groups, num_groups = _pixel_groups(X_inlier_subset, min_X, self.resolution_metres)
            group_areas = _group_areas(groups)

            # RANSAC for LIDAR addition: check that size of the largest continuous
            # group of pixels is also over the minimum number of points per plane:
            largest = max(group_areas, key=group_areas.get)
            roof_plane_area = group_areas[largest]
            if roof_plane_area < self.min_points_per_plane or roof_plane_area < (
                    total_points_in_building * self.min_points_per_plane_perc):
                # bad_samples.add(tuple(subset_idxs))
                if debug:
                    bad_sample_reasons["MIN_POINTS_PER_LARGEST_GROUP"] += 1
                continue

            # RANSAC for LIDAR addition: if inliers form multiple groups, reject
            # See Tarsha-Kurdi, 2007
            # Adapted from Tarsha-Kurdi to allow a few discontinuous pixels as long as
            # The `include_group_checks` flag allows disabling these 2 checks, as they
            # were causing issues for buildings with many unconnected roof sections
            # on the same plane:
            if num_groups > 1 and include_group_checks:
                # Allow a small amount of small outliers:
                if len(group_areas) > self.max_num_groups:
                    if debug:
                        bad_sample_reasons["TOO_MANY_GROUPS"] += 1
                    continue
                for groupid, area in group_areas.items():
                    if groupid != largest and area / roof_plane_area > self.max_group_area_ratio_to_largest:
                        if debug:
                            bad_sample_reasons["LARGEST_GROUP_TOO_SMALL"] += 1
                        continue

            # RANSAC for LiDAR addition: check ratio of points area to ratio of convex
            # hull of points area.
            # If the convex hull's area is significantly larger, it's likely to be a
            # bad plane that cuts through the roof at an angle
            only_largest = groups == largest
            convex_hull = morphology.convex_hull_image(only_largest)
            convex_hull_area = np.count_nonzero(convex_hull)
            cv_hull_ratio = roof_plane_area / convex_hull_area
            if cv_hull_ratio < self.min_convex_hull_ratio:
                if debug:
                    bad_sample_reasons["CONVEX_HULL_RATIO"] += 1
                continue

            # RANSAC for LiDAR addition: thinness ratio check
            perimeter = perimeter_crofton(only_largest, directions=4)
            thinness_ratio = (4 * np.pi * roof_plane_area) / (perimeter * perimeter)
            if thinness_ratio < _min_thinness_ratio(roof_plane_area):
                if debug:
                    bad_sample_reasons["THINNESS_RATIO"] += 1
                continue

            if debug:
                # print(f"new best SD plane found. SD {sd}. Old SD {sd_best}. Current trial: {self.n_trials_}")
                print(f"new best score plane found. score {score_subset}. Old score {score_best}. Current trial: {self.n_trials_}")

            # save current random sample as best sample
            n_inliers_best = n_inliers_subset
            best_sample_idxs = plane.idxs
            score_best = score_subset
            sd_best = sd

            plane_properties_best = {
                "aspect_circ_mean": math.degrees(aspect_circ_mean) if aspect_circ_mean else None,
                "aspect_circ_sd": aspect_circ_sd,
                "score": score_best,
                "thinness_ratio": thinness_ratio,
                "cv_hull_ratio": cv_hull_ratio,
                "plane_type": plane.plane_type,
            }
            inlier_mask_best = inlier_mask_subset
            X_inlier_best = X_inlier_subset
            y_inlier_best = y_inlier_subset
            inlier_best_idxs_subset = inlier_idxs_subset
            sample_residual_threshold_best = sample_residual_threshold

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
            print(f"total rejected: {total}.")

        # if none of the iterations met the required criteria
        if inlier_mask_best is None:
            if ((self.n_skips_no_inliers_ + self.n_skips_invalid_data_ +
                    self.n_skips_invalid_model_) > self.max_skips):
                raise RANSACValueError(
                    "RANSAC skipped more iterations than `max_skips` without"
                    " finding a valid consensus set. Iterations were skipped"
                    " because each randomly chosen sub-sample failed the"
                    " passing criteria. See estimator attributes for"
                    " diagnostics (n_skips*).")
            else:
                raise RANSACValueError(
                    "RANSAC could not find a valid consensus set. All"
                    " `max_trials` iterations were skipped because each"
                    " randomly chosen sub-sample failed the passing criteria."
                    " See estimator attributes for diagnostics (n_skips*).")
        else:
            if (self.n_skips_no_inliers_ + self.n_skips_invalid_data_ +
                    self.n_skips_invalid_model_) > self.max_skips:
                warnings.warn("RANSAC found a valid consensus set but exited"
                              " early due to skipping more iterations than"
                              " `max_skips`. See estimator attributes for"
                              " diagnostics (n_skips*).",
                              ConvergenceWarning)

        # estimate final model using all inliers
        if sample_weight is None:
            base_estimator.fit(X_inlier_best, y_inlier_best)
        else:
            base_estimator.fit(
                X_inlier_best,
                y_inlier_best,
                sample_weight=sample_weight[inlier_best_idxs_subset])

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
        residuals_subset[mask == 0] = 9999  # TODO constant

        inlier_mask_best = residuals_subset < residual_threshold
        mask_without_excluded = _exclude_unconnected(X, min_X, inlier_mask_best, res=self.resolution_metres)

        if np.sum(mask_without_excluded) < self.min_points_per_plane:
            raise RANSACValueError(f"Less than {self.min_points_per_plane} points within "
                                   f"{residual_threshold} of plane after final fit")

        self.estimator_ = base_estimator
        self.inlier_mask_ = mask_without_excluded
        self.sd = sd_best
        self.plane_properties = plane_properties_best

        if debug:
            a, b = self.estimator_.coef_
            print(f"plane found: slope {_slope(a, b)} aspect {_aspect(a, b)} sd {self.sd} inliers {np.sum(mask_without_excluded)}")
            print("")
        return self


def _test_ridges(X, min_X, y, res: float):

    normed = ((X - min_X) / res).astype(int)
    image = np.zeros((int(np.amax(normed[:, 0])) + 1,
                      int(np.amax(normed[:, 1])) + 1))
    for i, pair in enumerate(normed):
        image[pair[0]][pair[1]] = y[i]

    ridges(image)


def _test_watershed(X, min_X, y, res: float):
    normed = ((X - min_X) / res).astype(int)
    image = np.full((int(np.amax(normed[:, 1])) + 1,
                     int(np.amax(normed[:, 0])) + 1), 9999.0)
    for i, pair in enumerate(normed):
        image[pair[1]][pair[0]] = y[i]

    image = np.flip(image, axis=0)
    mask = image != 9999

    rounded = image.astype(int)
    markers_bool = local_minima(rounded, connectivity=2) * mask
    markers = ndimage.label(markers_bool)[0]

    # does not having a buffer around the building geom when selecting pixels make this worse? Think this is OK
    w = watershed(image, connectivity=2, mask=mask, compactness=1.0)
    return w


# def _test_flow(X, min_X, y, res: float):
#     normed = ((X - min_X) / res).astype(int)
#     image = np.full((int(np.amax(normed[:, 1])) + 1,
#                      int(np.amax(normed[:, 0])) + 1), 0.0)
#     for i, pair in enumerate(normed):
#         image[pair[1]][pair[0]] = y[i]
#
#     image = np.flip(image, axis=0)
#     mask = image != 0.0
#
#     from pysheds.view import Raster, ViewFinder
#     from pysheds.grid import Grid
#     raster = Raster(image, viewfinder=ViewFinder(shape=image.shape, mask=mask))
#     grid = Grid(raster.viewfinder)
#     grid.mask = mask
#     fdir = grid.flowdir(raster, apply_mask=True, nodata_in=np.nan)
#     fdir = fdir * mask
#     # fdir = np.degrees(fdir)
#     fdir = np.array(fdir)
#     print(fdir)
#     return fdir


def _segment_aspect(X, min_X, aspect, res: float):
    normed = ((X - min_X) / res).astype(int)
    image = np.full((int(np.amax(normed[:, 1])) + 1,
                     int(np.amax(normed[:, 0])) + 1), 0.0)
    for i, pair in enumerate(normed):
        image[pair[1]][pair[0]] = aspect[i]

    image = np.flip(image, axis=0)
    mask = image != 0.0

    labels1 = segmentation.slic(image, compactness=30, n_segments=100, start_label=1, mask=mask)
    # out1 = color.label2rgb(labels1, image, kind='avg', bg_label=0)

    from skimage.future.graph import rag_mean_color, cut_threshold
    g = rag_mean_color(image, labels1)
    labels2 = cut_threshold(labels1, g, 39)
    # out2 = color.label2rgb(labels2, image, kind='avg', bg_label=0)
    return labels2