# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.

# A roof is considered to be flat if it's slope is less than this. Not to be confused
# with the model parameter `flat_roof_degrees`, which is the slope at which panels
# are mounted on flat roofs.
# Source: first-user partners
FLAT_ROOF_DEGREES_THRESHOLD = 5.0

# If a roof plane has an aspect which is closer than this value to the azimuth of
# one of the facings of a building, re-align the roof plane to that azimuth.
AZIMUTH_ALIGNMENT_THRESHOLD = 15

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

RANSAC_LARGE_MAX_TRIALS = 2000
RANSAC_MEDIUM_MAX_TRIALS = 2000
RANSAC_SMALL_MAX_TRIALS = 3000

# GDAL default tile geotiff tilesize:
POSTGIS_TILESIZE = 256

# These tend to use a lot of memory. This will only slow down the horizon profiling,
# which only takes a few minutes anyway.
MAX_PVMAPS_PROCESSES = 6
