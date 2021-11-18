"""Database schema and table names"""

PIXEL_HORIZON_TABLE = 'pixel_horizons'
"""Table to store the per-pixel horizon data from 320-albion-saga-gis"""

ROOF_HORIZON_TABLE = 'roof_horizons'
"""Table to store the horizon data joined with the polygons which represent planar areas of roof"""

PANEL_HORIZON_TABLE = 'panel_horizons'
"""Horizon data joined with polygons representing arrays of PV panels"""

SOLAR_PV_TABLE = 'solar_pv'
"""Table to store the raw solar PV data received from PV-GIS"""

BOUNDS_TABLE = 'bounds_4326'
"""Table with the job bounds transformed to SRID 4326"""

BUILDINGS_TABLE = 'buildings'
"""All the buildings inside the job bounds, transformed to 27700"""

ROOF_PLANE_TABLE = 'roof_planes'
"""All the roof planes detected using RANSAC"""

BUILDING_EXCLUSION_REASONS_TABLE = 'building_exclusion_reasons'
"""A lookup from toid to the reason why the building has no PV 
(or null if the building has PV)"""

ALL_BUILDINGS_TABLE = 'all_buildings'
"""A single multipolygon of the union of all buildings in the job"""


def schema(job_id: int) -> str:
    """Get the solar PV schema given a job_id."""
    return f"solar_pv_job_{int(job_id)}"
