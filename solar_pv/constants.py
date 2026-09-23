# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.

# A roof is considered to be flat if it's slope is less than this. Not to be confused
# with the model parameter `flat_roof_degrees`, which is the slope at which panels
# are mounted on flat roofs.
# Source: first-user partners
FLAT_ROOF_DEGREES_THRESHOLD = 4.9

# If a roof plane has an aspect which is closer than this value to the azimuth of
# one of the facings of a building, re-align the roof plane to that azimuth.
AZIMUTH_ALIGNMENT_THRESHOLD = 15.5

# Same as above, but for flat roofs:
FLAT_ROOF_AZIMUTH_ALIGNMENT_THRESHOLD = 46

# PVGIS recommend this factor is applied to cover losses due to cabling, inverter, and
# degradation due to age.
# See section 5.2.5 here:
# https://joint-research-centre.ec.europa.eu/pvgis-photovoltaic-geographical-information-system/getting-started-pvgis/pvgis-data-sources-calculation-methods_en#ref-5-calculation-of-pv-power-output
SYSTEM_LOSS = 0.14

# Area in m2 of a building to consider large for RANSAC purposes
# (which has the effect of allowing planes that cover multiple discontinuous groups
# of pixels, as large buildings often have separate roof areas that are on the
# same plane):
RANSAC_LARGE_BUILDING = 1000
# Area in m2 of a building to consider small for RANSAC purposes
# (which has the effect of increasing `max_trials`, as it is harder to fit a
# good plane to a smaller set of points):
RANSAC_SMALL_BUILDING = 100

RANSAC_LARGE_MAX_TRIALS = 500
RANSAC_MEDIUM_MAX_TRIALS = 500
RANSAC_SMALL_MAX_TRIALS = 1000

# If a roof plane's score is lower than this, stop optimising for score
# and start optimising for number of points on the plane:
ROOFDET_GOOD_SCORE = 0.125

ROOFDET_MAX_MAE = 1.0

# GDAL default tile geotiff tilesize:
POSTGIS_TILESIZE = 256

# Don't use more than this many CPUs for roof plane detection (otherwise it uses 3/4
# of what's available)
ROOFDET_MAX_CPUS = 100

# Max area of buildings to run roof plane detection on
ROOFDET_MAX_AREA = 50000

##
# PV output uncertainty constants:
##

# One-sided normal 90th-percentile z-score:
Z_P90 = 1.282
# Coefficient of variation of UK annual global horizontal irradiation between years:
INTERANNUAL_GHI_COV = 0.05

##
# Roof-plane confidence scoring constants:
##

# Plane-fit MAE (metres) at/below which the fit sub-score is 1. It falls on a log
# scale from here to 0 at ROOFDET_MAX_MAE, as residuals span orders of magnitude:
CONFIDENCE_FULL_FIT_MAE = 0.05

# aspect_circ_sd (radians) at/above which the aspect sub-score is 0. Matches the
# roof-detection rejection threshold (Thresholds.max_aspect_circular_sd):
CONFIDENCE_MAX_ASPECT_CIRC_SD = 1.5

# Pixel aspects get noisier as slope approaches flat, so the aspect penalty fades in
# linearly from FLAT_ROOF_DEGREES_THRESHOLD, reaching full strength at this slope:
CONFIDENCE_ASPECT_FULL_SLOPE = 20.0

# The shape sub-score rises from 0 at roof detection's area-dependent minimum
# thinness ratio to 1 at that minimum plus this:
CONFIDENCE_SHAPE_SPAN = 0.4

# LiDAR working resolution (m) -> sub-score. Finer LiDAR resolves roof detail better:
CONFIDENCE_RESOLUTION_SCORES = {0.5: 1.0, 1.0: 0.8, 2.0: 0.2}

# Sub-scores are floored at this when combined, so a single zero sub-score ranks a
# roof bottom without discarding what the others say about it:
CONFIDENCE_SUB_SCORE_FLOOR = 0.05

# Weights for the geometric mean; must sum to 1:
CONFIDENCE_WEIGHTS = {
    "fit": 0.30,
    "resolution": 0.25,
    "geom_agreement": 0.20,
    "aspect": 0.15,
    "shape": 0.10,
}
