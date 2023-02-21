# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
"""Database schema and table names"""

LIDAR_PIXEL_TABLE = 'lidar_pixels'
"""Table to store the per-pixel data"""

ROOF_POLYGON_TABLE = 'roof_polygons'
"""Table to store the polygons which represent planar areas of roof"""

PANEL_POLYGON_TABLE = 'panel_polygons'
"""pixel data joined with polygons representing arrays of PV panels"""

PIXEL_KWH_TABLE = 'pixel_kwh'
"""Table to store the raw per-pixel solar PV data received from PV-GIS"""

BOUNDS_TABLE = 'bounds_4326'
"""Table with the job bounds transformed to SRID 4326"""

BUILDINGS_TABLE = 'buildings'
"""All the buildings inside the job bounds, transformed to 27700"""

SIMPLIFIED_BUILDING_GEOM_TABLE = 'simple_building_geoms'
"""Building geometries simplified for export for solar wizard frontend"""

ELEVATION = 'elevation'
"""Elevation raster for job"""

ASPECT = 'aspect'
"""Aspect raster for job"""

MASK = 'mask'
"""Mask raster for job"""

MASKED_ELEVATION = 'masked_elevation'
"""Masked elevation raster for job"""

INVERSE_MASKED_ELEVATION = 'inverse_masked_elevation'
"""Inverse masked elevation raster for job"""


def schema(job_id: int) -> str:
    """Get the solar PV schema given a job_id."""
    return f"solar_pv_job_{int(job_id)}"
