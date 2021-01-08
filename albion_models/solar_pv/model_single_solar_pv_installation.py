import logging
import os
from os.path import join
from typing import List

import albion_models.solar_pv.mask as mask
from albion_models.db_funcs import process_pg_uri
from albion_models.solar_pv.polygonize import aggregate_user_submitted_polygon_horizons
from albion_models.solar_pv.saga_gis.horizons import get_horizons, load_horizons_to_db
from albion_models.solar_pv import gdal_helpers
from albion_models.solar_pv.model_solar_pv import _init_schema, _pv_gis, _write_results_to_db


def model_single_solar_pv_installation(pg_uri: str,
                                       root_solar_dir: str,
                                       job_id: int,
                                       lidar_paths: List[str],
                                       horizon_search_radius: int,
                                       horizon_slices: int,
                                       roof_area_percent_usable: int,
                                       flat_roof_degrees: int,
                                       peak_power_per_m2: float,
                                       pv_tech: str,
                                       aggregate_fn: str = 'avg'):
    """
    Model solar PV.

    The `bounds` multipolygon column in the job definition
    is taken to represent the bounds of individual solar PV installations, rather than
    the area to search for viable buildings in.
    """
    pg_uri = process_pg_uri(pg_uri)
    _validate_params(
        lidar_paths,
        horizon_search_radius,
        horizon_slices,
        roof_area_percent_usable,
        flat_roof_degrees,
        peak_power_per_m2)

    solar_dir = join(root_solar_dir, f"job_{job_id}")
    os.makedirs(solar_dir, exist_ok=True)

    vrt_file = join(solar_dir, 'tiles.vrt')
    gdal_helpers.create_vrt(lidar_paths, vrt_file)

    logging.info("Initialising postGIS schema...")
    _init_schema(pg_uri, job_id)

    logging.info("Creating raster mask from job bounds polygons...")
    mask_file = mask.create_bounds_mask(job_id, solar_dir, pg_uri, resolution_metres=1)

    logging.info("Cropping lidar to mask dimensions...")
    cropped_lidar = join(solar_dir, 'cropped_lidar.tif')
    gdal_helpers.crop_or_expand(mask_file, vrt_file, mask_file, adjust_resolution=False)
    gdal_helpers.crop_or_expand(vrt_file, mask_file, cropped_lidar, adjust_resolution=True)

    logging.info("Using 320-albion-saga-gis to find horizons...")
    horizons_csv = join(solar_dir, 'horizons.csv')
    get_horizons(cropped_lidar, solar_dir, mask_file, horizons_csv, horizon_search_radius, horizon_slices)
    load_horizons_to_db(pg_uri, job_id, horizons_csv, horizon_slices)

    logging.info("Aggregating horizon data by user-submitted polygons and filtering...")
    aggregate_user_submitted_polygon_horizons(pg_uri, job_id, horizon_slices, flat_roof_degrees, aggregate_fn)

    logging.info("Sending requests to PV-GIS...")
    solar_pv_csv = _pv_gis(pg_uri, job_id, peak_power_per_m2, pv_tech, roof_area_percent_usable, solar_dir)

    logging.info("Loading PV data into albion...")
    _write_results_to_db(pg_uri, job_id, solar_pv_csv)


def _validate_params(lidar_paths: List[str],
                     horizon_search_radius: int,
                     horizon_slices: int,
                     roof_area_percent_usable: int,
                     flat_roof_degrees: int,
                     peak_power_per_m2: float):
    if not lidar_paths or len(lidar_paths) == 0:
        raise ValueError(f"No LIDAR tiles available, cannot run solar PV modelling.")
    if horizon_search_radius < 0 or horizon_search_radius > 10000:
        raise ValueError(
            f"horizon search radius must be between 0 and 10000, was {horizon_search_radius}")
    if horizon_slices > 64 or horizon_slices < 8:
        raise ValueError(
            f"horizon slices must be between 8 and 64, was {horizon_slices}")
    if roof_area_percent_usable < 0 or roof_area_percent_usable > 100:
        raise ValueError(
            f"roof_area_percent_usable must be between 0 and 100, was {roof_area_percent_usable}")
    if flat_roof_degrees < 0 or flat_roof_degrees > 90:
        raise ValueError(
            f"flat_roof_degrees must be between 0 and 90, was {flat_roof_degrees}")
    if peak_power_per_m2 < 0:
        raise ValueError(
            f"peak_power_per_m2 must be greater than or equal to 0, was {peak_power_per_m2}")
