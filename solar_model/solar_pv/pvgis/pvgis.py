# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import logging
import shutil
from os.path import join
from typing import List, Optional

import os


import solar_model.solar_pv.tables as tables
from solar_model import paths, gdal_helpers
from solar_model.db_funcs import connection
from solar_model.lidar.lidar import LIDAR_NODATA
from solar_model.postgis import rasters_to_postgis, create_raster_table
from solar_model.solar_pv.constants import FLAT_ROOF_DEGREES_THRESHOLD, SYSTEM_LOSS, \
    POSTGIS_TILESIZE
from solar_model.solar_pv.pvgis import pvmaps
from solar_model.solar_pv.pvgis.aggregate_pixel_results import aggregate_pixel_results
from solar_model.solar_pv.rasters import create_elevation_override_raster, generate_flat_roof_aspect_raster
from solar_model.util import get_cpu_count


def _pvmaps_cpu_count():
    """Use 3/4s of available CPUs for PVMAPS"""
    return int(get_cpu_count() * 0.75)


def pvgis(pg_uri: str,
          job_id: int,
          solar_dir: str,
          job_lidar_dir: str,
          resolution_metres: float,
          pv_tech: str,
          horizon_search_radius: int,
          horizon_slices: int,
          peak_power_per_m2: float,
          flat_roof_degrees: int,
          elevation_raster: str,
          mask_raster: str,
          slope_raster: str,
          aspect_raster: str,
          debug_mode: bool):

    if pv_tech == "crystSi":
        panel_type = pvmaps.CSI
    elif pv_tech == "CdTe":
        panel_type = pvmaps.CDTE
    else:
        raise ValueError(f"Unsupported panel type '{pv_tech}' for PVMAPS")

    # GRASS needs the user to have a home directory that exists
    # (not always the case for prod deployments in containers):
    home_dir = os.environ.get("HOME")
    os.makedirs(home_dir, exist_ok=True)

    # In theory for r.horizon and r.pv this value can be a float,
    # but I can't get that to work. r.horizon seems to truncate it to int
    # in the filename, though the code doesn't read like it should
    horizon_step_degrees = 360 // horizon_slices
    if 360 % horizon_slices != 0:
        logging.warning(f"Using f{horizon_step_degrees} for horizon step, "
                        f"truncated from {360 / horizon_slices}. To avoid this, use"
                        f"a horizon_slices value that is a factor of 360.")

    logging.info("Getting building height elevation override raster...")
    elevation_override_raster: Optional[str] = create_elevation_override_raster(
        pg_uri=pg_uri,
        job_id=job_id,
        solar_dir=solar_dir,
        elevation_raster_27700_filename=elevation_raster)

    logging.info("Generating flat roof raster")
    flat_roof_aspect_raster: Optional[str] = generate_flat_roof_aspect_raster(
        pg_uri=pg_uri,
        job_id=job_id,
        solar_dir=solar_dir,
        mask_raster_27700_filename=mask_raster)

    pvm = pvmaps.PVMaps(
        grass_dbase_dir=os.environ.get("PVGIS_GRASS_DBASE_DIR", None),
        input_dir=solar_dir,
        output_dir=job_lidar_dir,
        pvgis_data_tar_file=join(os.environ.get("PVGIS_DATA_TAR_FILE_DIR", None), "pvgis_data.tar"),
        pv_model_coeff_file_dir=paths.RESOURCES_DIR,
        keep_temp_mapset=debug_mode,
        num_processes=_pvmaps_cpu_count(),
        output_direct_diffuse=False,
        horizon_step_degrees=horizon_step_degrees,
        horizon_search_distance=horizon_search_radius,
        flat_roof_degrees=flat_roof_degrees,
        flat_roof_degrees_threshold=FLAT_ROOF_DEGREES_THRESHOLD,
        panel_type=panel_type,
        job_id=job_id
    )
    pvm.create_pvmap(
        elevation_filename=os.path.basename(elevation_raster),
        mask_filename=os.path.basename(mask_raster),
        flat_roof_aspect_filename=os.path.basename(flat_roof_aspect_raster) if flat_roof_aspect_raster else None,
        elevation_override_filename=os.path.basename(elevation_override_raster) if elevation_override_raster else None,
        forced_slope_filename=slope_raster,             # Use values from GDAL for slope and aspect as aspects differ by
        forced_aspect_filename_compass=aspect_raster    # up to approx 3 degrees after switch to 27700
    )

    yearly_kwh_raster = pvm.yearly_kwh_raster
    monthly_wh_rasters = pvm.monthly_wh_rasters
    # horizons are CCW from East
    horizon_rasters = pvm.horizons

    logging.info("Finished PVMAPS, loading into db...")

    _write_results_to_db(
        pg_uri=pg_uri,
        job_id=job_id,
        solar_dir=solar_dir,
        resolution_metres=resolution_metres,
        peak_power_per_m2=peak_power_per_m2,
        yearly_kwh_raster=yearly_kwh_raster,
        monthly_wh_rasters=monthly_wh_rasters,
        horizon_rasters=horizon_rasters)


def _write_results_to_db(pg_uri: str,
                         job_id: int,
                         solar_dir: str,
                         resolution_metres: float,
                         peak_power_per_m2: float,
                         yearly_kwh_raster: str,
                         monthly_wh_rasters: List[str],
                         horizon_rasters: List[str]):
    if len(monthly_wh_rasters) != 12:
        raise ValueError(f"Expected 12 monthly rasters - got {len(monthly_wh_rasters)}")

    schema = tables.schema(job_id)

    with connection(pg_uri) as pg_conn:
        raster_tables = []
        raster_table = f"{schema}.kwh_year"

        create_raster_table(pg_conn, raster_table, drop=True)
        rasters_to_postgis(pg_conn, [yearly_kwh_raster], raster_table, solar_dir, POSTGIS_TILESIZE, nodata_val=LIDAR_NODATA, srid=27700)
        raster_tables.append(raster_table)

        for i, raster in enumerate(monthly_wh_rasters):
            raster_table = f"{schema}.month_{str(i + 1).zfill(2)}_wh"
            create_raster_table(pg_conn, raster_table, drop=True)
            rasters_to_postgis(pg_conn, [raster], raster_table, solar_dir, POSTGIS_TILESIZE, nodata_val=LIDAR_NODATA, srid=27700)
            raster_tables.append(raster_table)

        for i, raster in enumerate(horizon_rasters):
            raster_table = f"{schema}.horizon_{str(i).zfill(2)}"
            create_raster_table(pg_conn, raster_table, drop=True)
            rasters_to_postgis(pg_conn, [raster], raster_table, solar_dir, POSTGIS_TILESIZE, nodata_val=LIDAR_NODATA, srid=27700)
            raster_tables.append(raster_table)

    aggregate_pixel_results(pg_uri=pg_uri,
                            job_id=job_id,
                            raster_tables=raster_tables,
                            resolution=resolution_metres,
                            peak_power_per_m2=peak_power_per_m2,
                            system_loss=SYSTEM_LOSS)
