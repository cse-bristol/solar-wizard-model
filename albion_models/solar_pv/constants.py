
# A roof is considered to be flat if it's slope is less than this. Not to be confused
# with the model parameter `flat_roof_degrees`, which is the slope at which panels
# are mounted on flat roofs.
# Source: clause 5.8.11
# https://mcssco.wpenginepowered.com/wp-content/uploads/2021/10/MIS-3002_Solar-PV-Systems-V4.0.pdf#page=19
FLAT_ROOF_DEGREES_THRESHOLD = 10.0

# If a roof plane has an aspect which is closer than this value to the azimuth of
# one of the facings of a building, re-align the roof plane to that azimuth.
AZIMUTH_ALIGNMENT_THRESHOLD = 15

# Same as above, but for flat roofs:
FLAT_ROOF_AZIMUTH_ALIGNMENT_THRESHOLD = 45

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

RANSAC_BASE_MAX_TRIALS = 1000
# Don't go over this number of trials, whatever size the building is:
RANSAC_ABS_MAX_TRIALS = 30000

# LiDAR will be downscaled to this resolution if it is high-res (lower number):
# 50cm LiDAR is too slow unfortunately, particularly at the stages of loading raster
# pixel data into the database and running RANSAC
LIDAR_DOWNSCALE_TO = 1.0
