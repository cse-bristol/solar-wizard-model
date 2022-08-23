from collections import defaultdict

import numpy as np
import warnings
import math

from sklearn.linear_model import RANSACRegressor
from skimage import measure, morphology
from sklearn.base import clone
from sklearn.linear_model import LinearRegression
from sklearn.linear_model._ransac import _dynamic_max_trials
from sklearn.utils import check_random_state, check_consistent_length
from sklearn.utils.random import sample_without_replacement
from sklearn.utils.validation import _check_sample_weight
from sklearn.utils.validation import has_fit_parameter
from sklearn.exceptions import ConvergenceWarning

from albion_models.solar_pv.ransac.perimeter import perimeter_crofton


class RANSACValueError(ValueError):
    """
    So we can treat when the RANSAC regressor has failed intentionally differently
    from unintentionally thrown ValueErrors due to bugs.

    Not sure I like this use of exceptions but it's inherited from sklearn.
    """
    pass


class RANSACRegressorForLIDAR(RANSACRegressor):

    def __init__(self, base_estimator=None, *,
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
                 resolution_metres=1,
                 min_points_per_plane=8,
                 min_points_per_plane_perc=0.008,
                 max_slope=None,
                 min_slope=None,
                 min_convex_hull_ratio=0.65,
                 min_thinness_ratio=0.55,
                 max_area_for_thinness_test=25,
                 max_num_groups=20,
                 max_group_area_ratio_to_largest=0.02,
                 flat_roof_threshold_degrees=5,
                 max_aspect_circular_mean_degrees=90,
                 max_aspect_circular_sd=1.5):
        """
        :param min_points_per_plane_perc: min points per plane as a percentage of total
        points that fall within the building bounds. Default 0.8% (0.008). This will
        only affect larger buildings and stops it finding lots of tiny little sections.

        :param max_area_for_thinness_test: The thinness test will not be applied to
        roofs larger than this - useful as above a certain size even well-formed
        rectangles start to count as too thin.

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
        self.min_thinness_ratio = min_thinness_ratio
        self.max_area_for_thinness_test = max_area_for_thinness_test
        self.max_num_groups = max_num_groups
        self.max_group_area_ratio_to_largest = max_group_area_ratio_to_largest
        self.flat_roof_threshold_degrees = flat_roof_threshold_degrees
        self.max_aspect_circular_mean_degrees = max_aspect_circular_mean_degrees
        self.max_aspect_circular_sd = max_aspect_circular_sd

        self.sd = None
        self.plane_properties = {}
        self.resolution_metres = resolution_metres

    def fit(self, X, y,
            sample_weight=None,
            aspect=None,
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
                raise RANSACValueError("Absolute number of samples must be an "
                                 "integer value.")
            min_samples = self.min_samples
        else:
            raise RANSACValueError("Value for `min_samples` must be scalar and "
                             "positive.")
        if min_samples > X.shape[0]:
            raise RANSACValueError("`min_samples` may not be larger than number "
                             "of samples: n_samples = %d." % (X.shape[0]))

        if self.stop_probability < 0 or self.stop_probability > 1:
            raise RANSACValueError("`stop_probability` must be in range [0, 1].")

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
            subset_idxs = _sample(n_samples, min_samples, random_state=random_state, aspect=aspect)

            # RANSAC for LIDAR addition:
            if tuple(subset_idxs) in bad_samples:
                if debug:
                    bad_sample_reasons["ALREADY_SAMPLED"] += 1
                continue

            X_subset = X[subset_idxs]
            y_subset = y[subset_idxs]

            # check if random sample set is valid
            if (self.is_data_valid is not None
                    and not self.is_data_valid(X_subset, y_subset)):
                self.n_skips_invalid_data_ += 1
                if debug:
                    bad_sample_reasons["INVALID"] += 1
                continue

            # fit model for current random sample set
            if sample_weight is None:
                base_estimator.fit(X_subset, y_subset)
            else:
                base_estimator.fit(X_subset, y_subset,
                                   sample_weight=sample_weight[subset_idxs])

            # RANSAC for LIDAR addition: if slope of fit plane is too steep ...
            slope = _slope(base_estimator.coef_[0], base_estimator.coef_[1])
            if self.max_slope and slope > self.max_slope:
                bad_samples.add(tuple(subset_idxs))
                if debug:
                    bad_sample_reasons["MAX_SLOPE"] += 1
                continue
            # RANSAC for LIDAR addition: if slope too shallow ...
            if self.min_slope and slope < self.min_slope:
                bad_samples.add(tuple(subset_idxs))
                if debug:
                    bad_sample_reasons["MIN_SLOPE"] += 1
                continue

            # check if estimated model is valid
            if (self.is_model_valid is not None and not
                    self.is_model_valid(base_estimator, X_subset, y_subset)):
                self.n_skips_invalid_model_ += 1
                if debug:
                    bad_sample_reasons["MODEL_INVALID"] += 1
                continue

            # residuals of all data for current random sample model
            y_pred = base_estimator.predict(X)
            residuals_subset = loss_function(y, y_pred)

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
                bad_samples.add(tuple(subset_idxs))
                self.n_skips_no_inliers_ += 1
                if debug:
                    bad_sample_reasons["MIN_POINTS_PER_PLANE"] += 1
                continue

            # extract inlier data set
            inlier_idxs_subset = sample_idxs[inlier_mask_subset]
            X_inlier_subset = X[inlier_idxs_subset]
            y_inlier_subset = y[inlier_idxs_subset]

            # score of inlier data set
            # score_subset = base_estimator.score(X_inlier_subset,
            #                                     y_inlier_subset)
            # RANSAC for LIDAR addition: use stddev of inlier distance to plane
            # for score
            sd = np.std(residuals_subset[inlier_mask_subset])

            # RANSAC for LIDAR addition:
            # if difference between circular mean of pixel aspects and slope aspect is too high:
            # if circular deviation of pixel aspects too high:
            if slope > self.flat_roof_threshold_degrees:
                aspect_inliers = np.radians(aspect[inlier_mask_subset])
                plane_aspect = _aspect_rad(base_estimator.coef_[0], base_estimator.coef_[1])
                aspect_circ_mean = _circular_mean(aspect_inliers)
                aspect_diff = _rad_diff(plane_aspect, aspect_circ_mean)
                if aspect_diff > math.radians(self.max_aspect_circular_mean_degrees):
                    bad_samples.add(tuple(subset_idxs))
                    if debug:
                        bad_sample_reasons["CIRCULAR_MEAN"] += 1
                    continue

                aspect_circ_sd = _circular_sd(aspect_inliers)
                if aspect_circ_sd > self.max_aspect_circular_sd:
                    bad_samples.add(tuple(subset_idxs))
                    if debug:
                        bad_sample_reasons["CIRCULAR_SD"] += 1
                    continue
                # sd = aspect_circ_sd
            else:
                aspect_circ_sd = None
                aspect_circ_mean = None

            # same number of inliers but worse score -> skip current random
            # sample
            # if (n_inliers_subset == n_inliers_best
            #         and score_subset < score_best):
            #     continue
            # RANSAC for LIDAR addition: use stddev of inlier distance to plane
            # as score instead
            # See Tarsha-Kurdi, 2007
            if sd > sd_best or (sd == sd_best and n_inliers_subset <= n_inliers_best):
                bad_samples.add(tuple(subset_idxs))
                if debug:
                    bad_sample_reasons["WORSE_SD"] += 1
                continue

            # RANSAC for LIDAR addition: if inliers form multiple groups, reject
            # See Tarsha-Kurdi, 2007
            # Also check ratio of points area to ratio of convex hull of points area.
            # If the convex hull's area is significantly larger, it's likely to be a
            # bad plane that cuts through the roof at an angle
            if not _plane_morphology_ok(X_inlier_subset, min_X,
                                        min_convex_hull_ratio=self.min_convex_hull_ratio,
                                        min_thinness_ratio=self.min_thinness_ratio,
                                        max_area_for_thinness_test=self.max_area_for_thinness_test,
                                        min_points_per_plane=self.min_points_per_plane,
                                        min_points_per_plane_perc=self.min_points_per_plane_perc,
                                        total_points_in_building=total_points_in_building,
                                        max_num_groups=self.max_num_groups,
                                        max_group_area_ratio_to_largest=self.max_group_area_ratio_to_largest,
                                        include_group_checks=include_group_checks,
                                        res=self.resolution_metres):
                bad_samples.add(tuple(subset_idxs))
                if debug:
                    bad_sample_reasons["PLANE_MORPHOLOGY"] += 1
                continue

            # save current random sample as best sample
            n_inliers_best = n_inliers_subset
            sd_best = sd
            plane_properties_best = {
                "aspect_circ_mean": math.degrees(aspect_circ_mean) if aspect_circ_mean else None,
                "aspect_circ_sd": aspect_circ_sd,
            }
            inlier_mask_best = inlier_mask_subset
            X_inlier_best = X_inlier_subset
            y_inlier_best = y_inlier_subset
            inlier_best_idxs_subset = inlier_idxs_subset

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
                print(f"new best SD plane found. SD {sd_best}. Current trial: {self.n_trials_}")

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
        inlier_mask_best = residuals_subset < residual_threshold
        mask_without_excluded = _exclude_unconnected(X, min_X, inlier_mask_best, res=self.resolution_metres)

        self.estimator_ = base_estimator
        self.inlier_mask_ = mask_without_excluded
        self.sd = sd_best
        self.plane_properties = plane_properties_best

        if debug:
            a, b = self.estimator_.coef_
            print(f"plane found: slope {_slope(a, b)} aspect {_aspect(a, b)} sd {self.sd}")
            print("")
        return self


def _slope(a: float, b: float) -> float:
    """
    Return the slope of a plane defined by the X coefficient a and the Y coefficient b,
    in degrees from flat.
    """
    return abs(math.degrees(math.atan(math.sqrt(a**2 + b**2))))


def _aspect(a: float, b: float) -> float:
    """
    Return the aspect of a plane defined by the X coefficient a  and the Y coefficient b,
    in degrees from North.
    """
    return _to_positive_angle(math.degrees(math.atan2(b, -a) + (math.pi / 2)))


def _aspect_rad(a: float, b: float) -> float:
    """
    Return the aspect of a plane defined by the X coefficient a  and the Y coefficient b,
    in radians between 0 and 2pi
    """
    a = math.atan2(b, -a) + (math.pi / 2)
    return a if a >= 0 else a + (2 * math.pi)


def _circular_mean(pop):
    """
    Circular mean of a population of radians.
    Assumes radians between 0 and 2pi (might work with other ranges, not tested)
    Returns a value between 0 and 2pi.
    """
    cm = math.atan2(np.mean(np.sin(pop)), np.mean(np.cos(pop)))
    return cm if cm >= 0 else cm + (2 * math.pi)


def _circular_sd(pop):
    """
    Circular standard deviation of a population of radians.
    Assumes radians between 0 and 2pi (might work with other ranges, not tested).

    See https://en.wikipedia.org/wiki/Directional_statistics#Measures_of_location_and_spread
    """
    return math.sqrt(-2 * math.log(
        math.sqrt(sum(np.sin(pop)) ** 2 +
                  sum(np.cos(pop)) ** 2) /
        len(pop)))


def _circular_variance(pop):
    """
    Circular variance of a population of radians.
    Assumes radians between 0 and 2pi (might work with other ranges, not tested).

    See https://en.wikipedia.org/wiki/Directional_statistics#Measures_of_location_and_spread
    """
    return 1 - (math.sqrt(sum(np.sin(pop)) ** 2 +
                          sum(np.cos(pop)) ** 2) /
                len(pop))


def _rad_diff(r1, r2):
    """
    Smallest difference between radians.
    Assumes radians between 0 and 2pi. Will return a positive number.
    """
    return min(abs(r1 - r2), (2 * math.pi) - abs(r1 - r2))


def _to_positive_angle(angle):
    angle = angle % 360
    return angle + 360 if angle < 0 else angle


def _plane_morphology_ok(X_inlier_subset, min_X,
                         min_convex_hull_ratio: float,
                         min_thinness_ratio: float,
                         max_area_for_thinness_test: int,
                         min_points_per_plane: int,
                         min_points_per_plane_perc: float,
                         total_points_in_building: int,
                         max_num_groups: int,
                         max_group_area_ratio_to_largest: float,
                         include_group_checks: bool,
                         res: float) -> int:
    normed_inliers = ((X_inlier_subset - min_X) / res).astype(int)

    image = np.zeros((int(np.amax(normed_inliers[:, 0])) + 1,
                      int(np.amax(normed_inliers[:, 1])) + 1))
    image[normed_inliers[:, 0], normed_inliers[:, 1]] = 1

    groups, num_groups = measure.label(image, connectivity=1, return_num=True)
    areas = _group_areas(groups)
    largest = max(areas, key=areas.get)
    roof_plane_area = areas[largest]
    if roof_plane_area < min_points_per_plane or roof_plane_area < (total_points_in_building * min_points_per_plane_perc):
        return False
    only_largest = groups == largest

    # The `include_group_checks` flag allows disabling these 2 checks, as they
    # were causing issues for buildings with many unconnected roof sections
    # on the same plane:
    if num_groups > 1 and include_group_checks:
        # Allow a small amount of small outliers:
        group_areas = _group_areas(groups)
        if len(group_areas) > max_num_groups:
            return False
        for groupid, area in group_areas.items():
            if groupid != largest and area / roof_plane_area > max_group_area_ratio_to_largest:
                return False

    convex_hull = morphology.convex_hull_image(only_largest)
    convex_hull_area = np.count_nonzero(convex_hull)
    cv_hull_ratio = roof_plane_area / convex_hull_area
    if cv_hull_ratio < min_convex_hull_ratio:
        return False

    if roof_plane_area <= max_area_for_thinness_test:
        perimeter = perimeter_crofton(only_largest, directions=4)
        thinness_ratio = (4 * np.pi * roof_plane_area) / (perimeter * perimeter)

        if thinness_ratio < min_thinness_ratio:
            return False

    return True


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

    group_areas = _group_areas(groups)
    largest_area_group = max(group_areas, key=group_areas.get)

    idx_subset = idxs[groups == largest_area_group]
    mask = np.zeros(inlier_mask_best.shape, dtype=bool)
    mask[idx_subset] = True
    return mask


def _sample(n_samples, min_samples, random_state, aspect):
    max_aspect_range = 5
    sample_attempts = 0

    while sample_attempts < 1000:
        sample_attempts += 1
        initial_sample = sample_without_replacement(n_samples, 1, random_state=random_state)[0]
        initial_aspect = aspect[initial_sample]

        aspect_diff = np.minimum((aspect - initial_aspect) % 180, (initial_aspect - aspect) % 180)

        choose_from = np.asarray(aspect_diff < max_aspect_range).nonzero()[0]
        choose_from = choose_from[choose_from != initial_sample]
        if len(choose_from) < min_samples - 1:
            continue
        if (sample_attempts + 1) % 100 == 0:
            max_aspect_range += 5
        chosen = np.random.choice(choose_from, min_samples - 1)
        return np.append([initial_sample], chosen)

    raise RANSACValueError("Cannot find initial sample with aspect similarity")


def _group_areas(groups) -> dict:
    u, c = np.unique(groups, return_counts=True)
    group_areas = dict(zip(u, c))
    if 0 in group_areas:
        del group_areas[0]
    return group_areas
