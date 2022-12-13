import logging
from calendar import mdays
from os.path import join
from typing import List, Optional

import os

from psycopg2.sql import Identifier, Literal

import albion_models.solar_pv.tables as tables
from albion_models import paths, gdal_helpers
from albion_models.db_funcs import sql_script, connection, \
    sql_command
from albion_models.solar_pv.constants import FLAT_ROOF_DEGREES_THRESHOLD, SYSTEM_LOSS
from albion_models.solar_pv.pvgis import pvmaps
from albion_models.solar_pv.rasters import copy_rasters, \
    create_elevation_override_raster, generate_flat_roof_aspect_raster
from albion_models.util import get_cpu_count


def _pvmaps_cpu_count():
    """Use 3/4s of available CPUs for PVMAPS"""
    return int(get_cpu_count() * 0.75)


def pvgis(pg_uri: str,
          job_id: int,
          solar_dir: str,
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

    pvmaps_dir = join(solar_dir, "pvmaps")
    os.makedirs(pvmaps_dir, exist_ok=True)

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
        output_dir=pvmaps_dir,
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
        forced_slope_filename=slope_raster,     # Use values from GDAL for slope and aspect as aspects differ by
        forced_aspect_filename=aspect_raster    # up to approx 3 degrees after switch to 27700
    )

    yearly_kwh_raster = pvm.yearly_kwh_raster
    monthly_wh_rasters = pvm.monthly_wh_rasters
    # horizons are CCW from East
    horizon_rasters = pvm.horizons

    yearly_kwh_27700, mask_27700, monthly_wh_27700, horizon_27700 = _generate_equal_sized_rasters(
        pvmaps_dir, yearly_kwh_raster, mask_raster, monthly_wh_rasters, horizon_rasters)

    logging.info("Finished PVMAPS, loading into db...")

    with connection(pg_uri) as pg_conn:
        _write_results_to_db(
            pg_conn=pg_conn,
            job_id=job_id,
            solar_dir=solar_dir,
            resolution_metres=resolution_metres,
            peak_power_per_m2=peak_power_per_m2,
            yearly_kwh_raster=yearly_kwh_27700,
            monthly_wh_rasters=monthly_wh_27700,
            horizon_rasters=horizon_27700,
            mask_raster=mask_27700,
            debug_mode=debug_mode)


def _generate_equal_sized_rasters(pvmaps_dir: str,
                                  yearly_kwh_raster: str,
                                  mask_raster: str,
                                  monthly_wh_rasters: List[str],
                                  horizon_rasters: List[str]):
    """Generate rasters the same size as mask_raster
    """
    if len(monthly_wh_rasters) != 12:
        raise ValueError(f"Expected 12 monthly rasters - got {len(monthly_wh_rasters)}")

    yearly_kwh_27700 = join(pvmaps_dir, "kwh_year_27700.tif")

    gdal_helpers.crop_or_expand(yearly_kwh_raster, mask_raster, yearly_kwh_27700, True)

    monthly_wh_27700 = [join(pvmaps_dir, f"wh_m{str(i).zfill(2)}_27700.tif") for i in range(1, 13)]
    for r_in, r_out in zip(monthly_wh_rasters, monthly_wh_27700):
        gdal_helpers.crop_or_expand(r_in, mask_raster, r_out, True)

    horizon_27700 = [join(pvmaps_dir, f"horizon_{str(i).zfill(2)}_27700.tif") for i, _ in enumerate(horizon_rasters)]
    for r_in, r_out in zip(horizon_rasters, horizon_27700):
        gdal_helpers.crop_or_expand(r_in, mask_raster, r_out, True)

    return yearly_kwh_27700, mask_raster, monthly_wh_27700, horizon_27700


def _raster_value_transformer(data: List[float]) -> str:
    kwh_year = data[0]
    wh_month = data[1:13]
    horizons = data[13:]

    output_data = [f"{kwh_year:.2f}"]
    for i, wh_monthday in enumerate(wh_month):
        kwh_month = wh_monthday * 0.001 * mdays[i + 1]
        output_data.append(f"{kwh_month:.2f}")

    horizon_array = '"{' + ','.join([f"{val:.2f}" for val in horizons]) + '}"'
    output_data.append(horizon_array)

    return ','.join(output_data)


def _write_results_to_db(pg_conn,
                         job_id: int,
                         solar_dir: str,
                         resolution_metres: float,
                         peak_power_per_m2: float,
                         yearly_kwh_raster: str,
                         monthly_wh_rasters: List[str],
                         horizon_rasters: List[str],
                         mask_raster: str,
                         debug_mode: bool):
    if len(monthly_wh_rasters) != 12:
        raise ValueError(f"Expected 12 monthly rasters - got {len(monthly_wh_rasters)}")

    schema = tables.schema(job_id)
    sql_command(
        pg_conn,
        """
        CREATE TABLE {pixel_kwh} (
            x double precision,
            y double precision,
            kwh real,
            kwh_m01 real,
            kwh_m02 real,
            kwh_m03 real,
            kwh_m04 real,
            kwh_m05 real,
            kwh_m06 real,
            kwh_m07 real,
            kwh_m08 real,
            kwh_m09 real,
            kwh_m10 real,
            kwh_m11 real,
            kwh_m12 real,
            horizon real[],
            PRIMARY KEY (x, y)
        );
        """,
        pixel_kwh=Identifier(schema, tables.PIXEL_KWH_TABLE))

    copy_rasters(
        pg_conn, solar_dir,
        [yearly_kwh_raster] + monthly_wh_rasters + horizon_rasters,
        f"{schema}.{tables.PIXEL_KWH_TABLE}",
        mask_raster,
        value_transformer=_raster_value_transformer,
        debug_mode=debug_mode)

    sql_script(
        pg_conn,
        'pv/post-load.solar-pv.sql',
        {"job_id": job_id,
         "peak_power_per_m2": peak_power_per_m2},
        pixel_kwh=Identifier(schema, tables.PIXEL_KWH_TABLE),
        panel_kwh=Identifier(schema, "panel_kwh"),
        pixels_in_panels=Identifier(schema, "pixels_in_panels"),
        panel_polygons=Identifier(schema, tables.PANEL_POLYGON_TABLE),
        pixels_in_roofs=Identifier(schema, "pixels_in_roofs"),
        roof_horizons=Identifier(schema, "roof_horizons"),
        roof_polygons=Identifier(schema, tables.ROOF_POLYGON_TABLE),
        buildings=Identifier(schema, tables.BUILDINGS_TABLE),
        job_view=Identifier(f"solar_pv_job_{job_id}"),
        res=Literal(resolution_metres),
        system_loss=Literal(SYSTEM_LOSS))
