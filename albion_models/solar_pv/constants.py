
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
