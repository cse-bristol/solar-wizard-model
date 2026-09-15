# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
from collections import defaultdict
from dataclasses import dataclass
from typing import Set, Tuple, List, Optional

import numpy as np
import math
from shapely.geometry import Polygon, MultiPoint
from shapely.strtree import STRtree
from sklearn import metrics

from skimage import measure, morphology
from sklearn.linear_model import LinearRegression
from sklearn.utils import check_array, check_random_state, check_consistent_length
from sklearn.utils.random import sample_without_replacement
from skimage.measure import perimeter_crofton

from solar_pv.constants import AZIMUTH_ALIGNMENT_THRESHOLD, \
    FLAT_ROOF_AZIMUTH_ALIGNMENT_THRESHOLD, ROOFDET_GOOD_SCORE, \
    FLAT_ROOF_DEGREES_THRESHOLD
from solar_pv.geos import simplify_by_angle, polygon_line_segments, azimuth_deg, slope_deg, \
    aspect_deg, aspect_rad, circular_mean_rad, circular_sd_rad, rad_diff, deg_diff


_NEVER_INLIER = 9999

_SCORE_GATE_REASONS = ("LESS_INLIERS", "WORSE_SCORE")


class RANSACRegressorForLIDAR:

    def __init__(self, *,
                 residual_threshold,
                 flat_roof_residual_threshold,
                 max_trials=100,
                 max_slope=None,
                 min_slope=None,
                 min_points_per_plane=8,
                 min_points_per_plane_perc=0.008,
                 min_convex_hull_ratio=0.65,
                 max_aspect_circular_mean_degrees=90,
                 max_aspect_circular_sd=1.5,
                 resolution_metres=1,
                 random_state=None):
        """
        RANSAC adapted for fitting roof planes to LIDAR (see fit() for the changes
        made to the standard algorithm). Only extracts one plane per fit() call, so
        should be re-run until it can't find any more, with the points in the found
        plane removed from the next round's input.

        :param min_points_per_plane_perc: min points per plane as a percentage of total
        points that fall within the building bounds. Default 0.8% (0.008). This will
        only affect larger buildings and stops it finding lots of tiny little sections.
        """
        self.residual_threshold = residual_threshold
        self.flat_roof_residual_threshold = flat_roof_residual_threshold
        self.max_trials = max_trials
        self.max_slope = max_slope
        self.min_slope = min_slope
        self.min_points_per_plane = min_points_per_plane
        self.min_points_per_plane_perc = min_points_per_plane_perc
        self.min_convex_hull_ratio = min_convex_hull_ratio
        self.max_aspect_circular_mean_degrees = max_aspect_circular_mean_degrees
        self.max_aspect_circular_sd = max_aspect_circular_sd
        self.resolution_metres = resolution_metres
        self.random_state = random_state

        self.sd = None
        self.plane_properties = {}
        self.success = False
        self.finished = False

    def fit(self, X, y,
            polygon: Polygon,
            skip_planes: Set[Tuple[int]],
            aspect: np.ndarray,
            mask: np.ndarray,
            total_points_in_building: int,
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
        X = check_array(X, accept_sparse='csr')
        y = check_array(y, ensure_2d=False)
        check_consistent_length(X, y)

        base_estimator = LinearRegression()

        # assume linear model by default:
        min_samples = X.shape[1] + 1
        if min_samples > X.shape[0]:
            raise ValueError("`min_samples` may not be larger than number "
                             "of samples: n_samples = %d." % (X.shape[0]))

        residual_threshold = self.residual_threshold

        loss_function = lambda y_true, y_pred: np.abs(y_true - y_pred)

        random_state = check_random_state(self.random_state)

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
        best_subset_idxs = None

        # number of data samples
        n_samples = X.shape[0]
        sample_idxs = np.arange(n_samples)

        ctx = _FitContext(
            thresholds=_Thresholds.from_regressor(self), X=X, y=y, aspect=aspect,
            polygon=polygon, min_X=min_X, sample_idxs=sample_idxs,
            total_points_in_building=total_points_in_building,
            aspect_fallback_to_circ_mean=False)

        self.n_trials_ = 0
        max_trials = self.max_trials
        while self.n_trials_ < max_trials:
            self.n_trials_ += 1

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

            reason, cand = _evaluate_candidate(
                ctx, y_pred, residuals_subset, inlier_mask_subset,
                base_estimator.coef_, slope, score_best, n_inliers_best)
            if reason is not None:
                if debug:
                    bad_sample_reasons[reason] += 1
                # A sample rejected on score might still be the best in a later run of
                # RANSAC, so it's only skipped within this run (bad_samples), not banned
                # across runs (skip_planes).
                if reason in _SCORE_GATE_REASONS:
                    bad_samples.add(tuple(subset_idxs))
                else:
                    skip_planes.add(tuple(subset_idxs))
                continue

            if debug:
                print(f"new best score plane found. MAE {score_best} -> {cand.score} . inliers {n_inliers_best} -> {cand.n_inliers} .  Current trial: {self.n_trials_}")

            # save current random sample as best sample
            n_inliers_best = cand.n_inliers
            sd_best = cand.sd
            score_best = cand.score
            plane_properties_best = cand.plane_properties("RANSAC", f"RANSAC_{tuple(subset_idxs)}")
            inlier_mask_best = cand.inlier_mask
            X_inlier_best = cand.X_inlier
            y_inlier_best = cand.y_inlier
            inlier_best_idxs_subset = cand.inlier_idxs
            best_subset_idxs = subset_idxs

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
            skip_planes.add(tuple(best_subset_idxs))
        else:
            self.success = True
            self.estimator_ = base_estimator
            self.inlier_mask_ = mask_without_excluded
            self.sd = sd_best
            self.plane_properties = plane_properties_best

            self.plane_properties.update(_plane_metrics(
                base_estimator, X, y, mask_without_excluded, sample_idxs))

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
    inlier_mask_best = np.asarray(inlier_mask_best, dtype=bool)
    normed = ((X - min_X) / res).astype(int)
    rows, cols = normed[:, 0], normed[:, 1]
    shape = (int(np.amax(rows)) + 1, int(np.amax(cols)) + 1)

    image = np.zeros(shape)
    image[rows[inlier_mask_best], cols[inlier_mask_best]] = 1

    # Map each pixel back to a point index. Where several points share a pixel the
    # highest-indexed one wins (sequential assignment, matching the original loop).
    idxs = np.zeros(shape, dtype=int)
    idxs[rows, cols] = np.arange(len(normed))

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


def _convex_hull_ratio(groups, largest, roof_plane_area: int):
    """
    Ratio of the largest contiguous group's pixel area to the area of that group's
    convex hull. A low ratio indicates a plane that has cut across a roof at an angle,
    leaving a concave (e.g. u-shaped) intersection with the actual points.

    Returns the ratio and the boolean image of the largest group (which the
    thinness-ratio check reuses).
    """
    only_largest = groups == largest
    convex_hull = morphology.convex_hull_image(only_largest)
    convex_hull_area = np.count_nonzero(convex_hull)
    return roof_plane_area / convex_hull_area, only_largest


def _thinness_ratio(only_largest, roof_plane_area: int) -> float:
    """
    `thinness ratio` (4 * pi * area / perimeter^2) of the largest contiguous group -
    a standard GIS measure for detecting sliver polygons. A low value means a long,
    thin shape, which is no good for PV panels even if accurately detected.
    """
    perimeter = perimeter_crofton(only_largest, directions=4)
    return (4 * np.pi * roof_plane_area) / (perimeter * perimeter)


def _aspect_stats(aspect: np.ndarray, inlier_mask, x_coef: float, y_coef: float):
    """
    Circular statistics comparing the aspects of the inlier LIDAR pixels with the
    aspect of the fitted plane.

    Returns (circular mean of the pixel aspects, circular sd of the pixel aspects,
    difference between the plane's aspect and that circular mean) - all in radians.
    """
    aspect_inliers = np.radians(aspect[inlier_mask])
    plane_aspect = aspect_rad(x_coef, y_coef)
    aspect_circ_mean = circular_mean_rad(aspect_inliers)
    aspect_diff = rad_diff(plane_aspect, aspect_circ_mean)
    aspect_circ_sd = circular_sd_rad(aspect_inliers)
    return aspect_circ_mean, aspect_circ_sd, aspect_diff


def _plane_metrics(estimator, X, y, mask_without_excluded, sample_idxs) -> dict:
    """
    Coefficients, derived slope/aspect, and goodness-of-fit metrics for the final
    fitted plane, computed over its connected inliers. Returned as a dict to merge
    into the plane's properties.
    """
    inlier_idxs = sample_idxs[mask_without_excluded]
    y_true = y[inlier_idxs]
    y_pred = estimator.predict(X[inlier_idxs])

    a, b = estimator.coef_
    d = estimator.intercept_
    slope = slope_deg(a, b)
    try:
        msle = metrics.mean_squared_log_error(y_true, y_pred)
    except ValueError:
        msle = None
    return {
        "x_coef": a,
        "y_coef": b,
        "intercept": d,
        "slope": slope,
        "is_flat": slope <= FLAT_ROOF_DEGREES_THRESHOLD,
        "aspect_raw": aspect_deg(a, b),
        "inliers_xy": X[mask_without_excluded],
        "r2": metrics.r2_score(y_true, y_pred),
        "mae": metrics.mean_absolute_error(y_true, y_pred),
        "mse": metrics.mean_squared_error(y_true, y_pred),
        "rmse": metrics.root_mean_squared_error(y_true, y_pred),
        "msle": msle,
        "mape": metrics.mean_absolute_percentage_error(y_true, y_pred),
    }


@dataclass
class _Candidate:
    """A viable roof plane found by `_evaluate_candidate`, before it's compared to
    the best-so-far and turned into a full result."""
    n_inliers: int
    sd: float
    score: float
    aspect_circ_mean: float
    aspect_circ_sd: float
    thinness_ratio: float
    cv_hull_ratio: float
    aspect: float
    inlier_mask: np.ndarray
    X_inlier: np.ndarray
    y_inlier: np.ndarray
    inlier_idxs: np.ndarray

    def plane_properties(self, plane_type: str, plane_id: str) -> dict:
        return {
            "sd": self.sd,
            "score": self.score,
            "aspect_circ_mean": math.degrees(self.aspect_circ_mean) if self.aspect_circ_mean else None,
            "aspect_circ_sd": self.aspect_circ_sd,
            "thinness_ratio": self.thinness_ratio,
            "cv_hull_ratio": self.cv_hull_ratio,
            "plane_type": plane_type,
            "plane_id": plane_id,
            "aspect": self.aspect,
        }


@dataclass
class _Thresholds:
    """The plane-acceptance thresholds `_evaluate_candidate` checks against. RANSAC and
    DETSAC both carry these as attributes; `from_regressor` lifts them into one value."""
    min_points_per_plane: int
    min_points_per_plane_perc: float
    min_convex_hull_ratio: float
    max_aspect_circular_mean_degrees: float
    max_aspect_circular_sd: float
    resolution_metres: float

    @classmethod
    def from_regressor(cls, reg) -> "_Thresholds":
        return cls(
            min_points_per_plane=reg.min_points_per_plane,
            min_points_per_plane_perc=reg.min_points_per_plane_perc,
            min_convex_hull_ratio=reg.min_convex_hull_ratio,
            max_aspect_circular_mean_degrees=reg.max_aspect_circular_mean_degrees,
            max_aspect_circular_sd=reg.max_aspect_circular_sd,
            resolution_metres=reg.resolution_metres)


@dataclass
class _FitContext:
    """Invariants shared by every candidate evaluation within a single fit() call,
    built once before the trial loop. The per-candidate values (residuals, mask,
    fitted coefficients, running best) are passed to `_evaluate_candidate` alongside."""
    thresholds: _Thresholds
    X: np.ndarray
    y: np.ndarray
    aspect: np.ndarray
    polygon: Polygon
    min_X: list
    sample_idxs: np.ndarray
    total_points_in_building: int
    # DETSAC falls back to the circular-mean pixel aspect when the plane aspect has no
    # nearby building face to align to; RANSAC doesn't.
    aspect_fallback_to_circ_mean: bool


def _evaluate_candidate(ctx: _FitContext, y_pred, residuals_subset, inlier_mask_subset,
                        coef, slope, score_best, n_inliers_best):
    """
    Run the per-candidate roof-plane pipeline shared by RANSAC and DETSAC, in order:
    min-points -> connectivity -> score gate -> circular aspect stats -> convex-hull
    ratio -> thinness ratio -> aspect alignment. Scoring and the aspect stats are
    computed on the connected inliers.

    Returns `(reason, None)` if the candidate is rejected - `reason` being one of the
    bad_sample_reasons strings - or `(None, _Candidate)` if it's viable. The caller
    owns the reject bookkeeping (skip_planes vs bad_samples), which differs between
    the two regressors.
    """
    t = ctx.thresholds
    n_inliers_subset = np.sum(inlier_mask_subset)
    if n_inliers_subset < t.min_points_per_plane:
        return "MIN_POINTS_PER_PLANE", None

    inlier_idxs_subset = ctx.sample_idxs[inlier_mask_subset]

    # prep for the plane morphology checks (before scoring, so score/SD/aspect stats
    # are computed on the connected inliers - the points we actually care about):
    groups, num_groups = _pixel_groups(ctx.X[inlier_idxs_subset], ctx.min_X, t.resolution_metres)
    group_areas = _group_areas(groups)

    # check that the largest continuous group of pixels is also over the minimum:
    largest = max(group_areas, key=group_areas.get)
    roof_plane_area = group_areas[largest]
    if roof_plane_area < t.min_points_per_plane or roof_plane_area < (
            ctx.total_points_in_building * t.min_points_per_plane_perc):
        return "MIN_POINTS_PER_LARGEST_GROUP", None

    # re-extract (connected) inlier data set
    inlier_mask_subset = _exclude_unconnected(ctx.X, ctx.min_X, inlier_mask_subset, res=t.resolution_metres)
    inlier_idxs_subset = ctx.sample_idxs[inlier_mask_subset]
    X_inlier_subset = ctx.X[inlier_idxs_subset]
    y_inlier_subset = ctx.y[inlier_idxs_subset]
    y_inlier_pred = y_pred[inlier_idxs_subset]

    score_subset = metrics.mean_absolute_error(y_inlier_subset, y_inlier_pred)
    sd = np.std(residuals_subset[inlier_mask_subset])

    # don't optimise for number of points fit to plane (Tarsha-Kurdi, 2007):
    if score_subset < ROOFDET_GOOD_SCORE and score_best < ROOFDET_GOOD_SCORE:
        if n_inliers_subset <= n_inliers_best or (n_inliers_subset == n_inliers_best and score_subset > score_best):
            return "LESS_INLIERS", None
    elif score_subset > score_best or (score_subset == score_best and n_inliers_subset <= n_inliers_best):
        return "WORSE_SCORE", None

    # reject if the pixel aspects disagree with the plane aspect (non-flat roofs):
    aspect_circ_mean, aspect_circ_sd, aspect_diff = _aspect_stats(
        ctx.aspect, inlier_mask_subset, coef[0], coef[1])
    if slope > FLAT_ROOF_DEGREES_THRESHOLD:
        if aspect_diff > math.radians(t.max_aspect_circular_mean_degrees):
            return "CIRCULAR_MEAN", None
        if aspect_circ_sd > t.max_aspect_circular_sd:
            return "CIRCULAR_SD", None

    cv_hull_ratio, only_largest = _convex_hull_ratio(groups, largest, roof_plane_area)
    if cv_hull_ratio < t.min_convex_hull_ratio:
        return "CONVEX_HULL_RATIO", None

    thinness_ratio = _thinness_ratio(only_largest, roof_plane_area)
    if thinness_ratio < _min_thinness_ratio(roof_plane_area):
        return "THINNESS_RATIO", None

    azimuths = get_potential_aspects(X_inlier_subset, ctx.polygon)
    if len(azimuths) == 0:
        return "NO_NEARBY_FACE", None

    if slope > FLAT_ROOF_DEGREES_THRESHOLD:
        aspect_deg_ = closest_azimuth(azimuths, aspect_deg(coef[0], coef[1]), AZIMUTH_ALIGNMENT_THRESHOLD)
        if aspect_deg_ is None and ctx.aspect_fallback_to_circ_mean:
            aspect_deg_ = closest_azimuth(azimuths, math.degrees(aspect_circ_mean), AZIMUTH_ALIGNMENT_THRESHOLD)
    else:
        aspect_deg_ = closest_azimuth(azimuths, 180, FLAT_ROOF_AZIMUTH_ALIGNMENT_THRESHOLD)

    if aspect_deg_ is None:
        return "NO_CLOSE_ASPECT", None

    return None, _Candidate(
        n_inliers=n_inliers_subset,
        sd=sd,
        score=score_subset,
        aspect_circ_mean=aspect_circ_mean,
        aspect_circ_sd=aspect_circ_sd,
        thinness_ratio=thinness_ratio,
        cv_hull_ratio=cv_hull_ratio,
        aspect=aspect_deg_,
        inlier_mask=inlier_mask_subset,
        X_inlier=X_inlier_subset,
        y_inlier=y_inlier_subset,
        inlier_idxs=inlier_idxs_subset,
    )


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


def get_potential_aspects(X_inlier_subset, polygon: Polygon) -> List[int]:
    polygon = simplify_by_angle(polygon, tolerance_degrees=2.0)
    line_segments = polygon_line_segments(polygon, min_length=1.0)
    mp = MultiPoint(X_inlier_subset)
    rp = mp.buffer(1.0)
    rtree = STRtree(line_segments)
    nearby = rtree.query(rp, predicate='intersects')
    if len(nearby) == 0:
        rp = mp.buffer(3.0)
        nearby = rtree.query(rp, predicate='intersects')
    if len(nearby) == 0:
        rp = mp.buffer(10.0)
        nearby = rtree.query(rp, predicate='intersects')
    if len(nearby) == 0:
        return []

    azimuths_base = [int(azimuth_deg(line_segments[idx].coords[0], line_segments[idx].coords[1])) for idx in nearby]
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
