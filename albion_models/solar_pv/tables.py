"""Database schema and table names"""

PIXEL_HORIZON_TABLE = 'pixel_horizons'
"""Table to store the per-pixel horizon data from 320-albion-saga-gis"""

ROOF_POLYGON_TABLE = 'roof_polygons'
"""Table to store the polygons which represent planar areas of roof"""

ROOF_HORIZON_TABLE = 'roof_horizons'
"""Table to store the horizon data joined with the polygons which represent planar areas of roof"""

SOLAR_PV_TABLE = 'solar_pv'
"""Table to store the raw solar PV data received from PV-GIS"""

BOUNDS_TABLE = 'bounds_4326'
"""Table with the job bounds transformed to SRID 4326"""


def schema(job_id: int) -> str:
    """Get the solar PV schema given a job_id."""
    return f"solar_pv_job_{int(job_id)}"
