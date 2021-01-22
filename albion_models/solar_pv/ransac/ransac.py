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
                 min_points_per_plane=8,
                 max_slope=None,
                 min_slope=None,
                 min_convex_hull_ratio=0.6,
                 min_thinness_ratio=0.55):
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
        self.max_slope = max_slope
        self.min_slope = min_slope
        self.min_convex_hull_ratio = min_convex_hull_ratio
        self.min_thinness_ratio = min_thinness_ratio
        self.sd = None

    def fit(self, X, y, sample_weight=None, aspect=None):
        """
        Extended implementation of RANSAC with additions for usage with LIDAR
        to detect roof planes.

        Changes made:
        * Reject planes where the (x,y) points in the plane do not form a single
        contiguous region of the LIDAR (Tarsha-Kurdi, 2007) - these are unlikely to
        be roofs.

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
        This is intended to reject planes

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
                continue

            X_subset = X[subset_idxs]
            y_subset = y[subset_idxs]

            # check if random sample set is valid
            if (self.is_data_valid is not None
                    and not self.is_data_valid(X_subset, y_subset)):
                self.n_skips_invalid_data_ += 1
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
                continue
            # RANSAC for LIDAR addition: if slope too shallow ...
            if self.min_slope and slope < self.min_slope:
                bad_samples.add(tuple(subset_idxs))
                continue

            # check if estimated model is valid
            if (self.is_model_valid is not None and not
                    self.is_model_valid(base_estimator, X_subset, y_subset)):
                self.n_skips_invalid_model_ += 1
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
                continue

            # RANSAC for LIDAR addition: if inliers form multiple groups, reject
            # See Tarsha-Kurdi, 2007
            # Also check ratio of points area to ratio of convex hull of points area.
            # If the convex hull's area is significantly larger, it's likely to be a
            # bad plane that cuts through the roof at an angle
            if not _plane_morphology_ok(X_inlier_subset, min_X,
                                        self.min_convex_hull_ratio,
                                        self.min_thinness_ratio):
                bad_samples.add(tuple(subset_idxs))
                continue

            # save current random sample as best sample
            n_inliers_best = n_inliers_subset
            sd_best = sd
            inlier_mask_best = inlier_mask_subset
            X_inlier_best = X_inlier_subset
            y_inlier_best = y_inlier_subset
            inlier_best_idxs_subset = inlier_idxs_subset

            max_trials = min(
                max_trials,
                _dynamic_max_trials(n_inliers_best, n_samples,
                                    min_samples, self.stop_probability))

            # break if sufficient number of inliers
            if n_inliers_best >= self.stop_n_inliers:
                break

        # if none of the iterations met the required criteria
        if inlier_mask_best is None:
            if ((self.n_skips_no_inliers_ + self.n_skips_invalid_data_ +
                    self.n_skips_invalid_model_) > self.max_skips):
                raise ValueError(
                    "RANSAC skipped more iterations than `max_skips` without"
                    " finding a valid consensus set. Iterations were skipped"
                    " because each randomly chosen sub-sample failed the"
                    " passing criteria. See estimator attributes for"
                    " diagnostics (n_skips*).")
            else:
                raise ValueError(
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

        self.estimator_ = base_estimator
        self.inlier_mask_ = inlier_mask_best
        self.sd = sd_best
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


def _to_positive_angle(angle):
    angle = angle % 360
    return angle + 360 if angle < 0 else angle


def _smallest_angle_between(x, y):
    a = (x - y) % 180
    b = (y - x) % 180
    return a if a < b else b


def _plane_morphology_ok(X_inlier_subset, min_X,
                         min_convex_hull_ratio: float,
                         min_thinness_ratio: float) -> int:
    normed_inliers = np.array([np.array([pair[0] - min_X[0],
                                         pair[1] - min_X[1]])
                               for pair in X_inlier_subset]).astype('int')

    image = np.zeros((int(np.amax(normed_inliers[:, 0])) + 1,
                      int(np.amax(normed_inliers[:, 1])) + 1))
    for pair in normed_inliers:
        image[pair[0]][pair[1]] = 1

    # groups = measure.euler_number(image, connectivity=1)
    groups, num_groups = measure.label(image, connectivity=1, return_num=True)
    if num_groups > 1:
        return False

    if min_convex_hull_ratio is None:
        return True

    roof_plane_area = np.count_nonzero(groups)
    convex_hull = morphology.convex_hull_image(groups)
    convex_hull_area = np.count_nonzero(convex_hull)
    cv_hull_ratio = roof_plane_area / convex_hull_area
    if cv_hull_ratio < min_convex_hull_ratio:
        return False

    if min_thinness_ratio is None:
        return True

    perimeter = measure.perimeter_crofton(groups, directions=4)
    thinness_ratio = (4 * np.pi * roof_plane_area) / (perimeter * perimeter)

    if thinness_ratio < min_thinness_ratio:
        return False

    return True


def _sample(n_samples, min_samples, random_state, aspect):
    max_aspect_range = 5
    sample_attempts = 0

    while sample_attempts < 1000:
        sample_attempts += 1
        initial_sample = sample_without_replacement(n_samples, 1, random_state=random_state)[0]
        initial_aspect = aspect[initial_sample]

        mask = np.array([_smallest_angle_between(aspect[i], initial_aspect)
                         for i in range(0, n_samples)
                         if i != initial_sample])
        choose_from = np.where(mask < max_aspect_range)[0]
        if len(choose_from) < 2:
            continue
        if (sample_attempts + 1) % 100 == 0:
            max_aspect_range += 5
        chosen = np.random.choice(choose_from, min_samples-1)
        return np.append([initial_sample], chosen)

    raise ValueError("Cannot find initial sample with aspect similarity")
