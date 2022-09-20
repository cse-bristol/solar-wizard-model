import logging
from os.path import join
from typing import List

import os

from psycopg2.sql import Identifier, Literal

import albion_models.solar_pv.tables as tables
from albion_models import paths, gdal_helpers
from albion_models.db_funcs import sql_script, sql_script_with_bindings, connection, \
    sql_command
from albion_models.solar_pv.pvgis import pvmaps
from albion_models.solar_pv.rasters import copy_raster
from albion_models.transformations import OSTN15_TO_27700, OSTN15_TO_4326, OSTN02_PROJ4, \
    _7_PARAM_SHIFT
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
          debug_mode: bool):
    """
    TODO:
     * load rasters into db and combine
     * usual check to see if stage of model has already happened
     * 14% loss param
    """
    if pv_tech == "crystSi":
        panel_type = pvmaps.CSI
    elif pv_tech == "CdTe":
        panel_type = pvmaps.CDTE
    else:
        raise ValueError(f"Unsupported panel type '{pv_tech}' for PVMAPS")

    pvmaps_dir = join(solar_dir, "pvmaps")
    os.makedirs(pvmaps_dir, exist_ok=True)

    horizon_step_degrees = 360 // horizon_slices

    # pvm = pvmaps.PVMaps(
    #     grass_dbase_dir=os.environ.get("PVGIS_GRASS_DBASE", None),
    #     input_dir=solar_dir,
    #     output_dir=pvmaps_dir,
    #     pvgis_data_tar_file=join(os.environ.get("PVGIS_DATA_DIR", None), "pvgis_data.tar"),
    #     pv_model_coeff_file_dir=paths.RESOURCES_DIR,
    #     keep_temp_mapset=debug_mode,
    #     num_processes=_pvmaps_cpu_count(),
    #     output_direct_diffuse=False,
    #     horizon_step_degrees=horizon_step_degrees,
    #     horizon_search_distance=horizon_search_radius,
    #     flat_roof_degrees=flat_roof_degrees,
    #     flat_roof_degrees_threshold=5.0,
    #     panel_type=panel_type,
    # )
    # yearly_kwh_raster, monthly_wh_rasters = pvm.create_pvmap(
    #     elevation_filename=os.path.basename(elevation_raster),
    #     mask_filename=os.path.basename(mask_raster))

    yearly_kwh_raster = "/home/neil/data/albion-models/solar/job_1194/pvmaps/hpv_wind_spectral_year.tif"
    monthly_wh_rasters = [
        "/home/neil/data/albion-models/solar/job_1194/pvmaps/hpv_wind_spectral_17_1.tif",
        "/home/neil/data/albion-models/solar/job_1194/pvmaps/hpv_wind_spectral_46_2.tif",
        "/home/neil/data/albion-models/solar/job_1194/pvmaps/hpv_wind_spectral_75_3.tif",
        "/home/neil/data/albion-models/solar/job_1194/pvmaps/hpv_wind_spectral_103_4.tif",
        "/home/neil/data/albion-models/solar/job_1194/pvmaps/hpv_wind_spectral_135_5.tif",
        "/home/neil/data/albion-models/solar/job_1194/pvmaps/hpv_wind_spectral_162_6.tif",
        "/home/neil/data/albion-models/solar/job_1194/pvmaps/hpv_wind_spectral_198_7.tif",
        "/home/neil/data/albion-models/solar/job_1194/pvmaps/hpv_wind_spectral_228_8.tif",
        "/home/neil/data/albion-models/solar/job_1194/pvmaps/hpv_wind_spectral_259_9.tif",
        "/home/neil/data/albion-models/solar/job_1194/pvmaps/hpv_wind_spectral_289_10.tif",
        "/home/neil/data/albion-models/solar/job_1194/pvmaps/hpv_wind_spectral_319_11.tif",
        "/home/neil/data/albion-models/solar/job_1194/pvmaps/hpv_wind_spectral_345_12.tif",
    ]

    # yearly_kwh_27700, yearly_kwh_mask_27700, monthly_wh_27700 = _generate_27700_rasters(
    #     pvmaps_dir, yearly_kwh_raster, mask_raster, monthly_wh_rasters)

    logging.info("Finished PVMAPS, loading into db...")

    with connection(pg_uri) as pg_conn:
        _write_results_to_db(
            pg_conn=pg_conn,
            job_id=job_id,
            solar_dir=solar_dir,
            resolution_metres=resolution_metres,
            peak_power_per_m2=peak_power_per_m2,
            yearly_kwh_raster=yearly_kwh_raster,
            monthly_wh_rasters=monthly_wh_rasters,
            debug_mode=debug_mode)


def _generate_27700_rasters(pvmaps_dir: str,
                            yearly_kwh_raster: str,
                            mask_raster: str,
                            monthly_wh_rasters: List[str]):
    if len(monthly_wh_rasters) != 12:
        raise ValueError(f"Expected 12 monthly rasters - got {len(monthly_wh_rasters)}")

    yearly_kwh_27700 = join(pvmaps_dir, "kwh_year_27700.tif")
    yearly_kwh_mask_27700 = join(pvmaps_dir, "kwh_year_mask_27700.tif")
    gdal_helpers.reproject(yearly_kwh_raster, yearly_kwh_27700, src_srs="EPSG:4326", dst_srs=_7_PARAM_SHIFT)
    gdal_helpers.reproject(mask_raster, yearly_kwh_mask_27700, src_srs="EPSG:4326", dst_srs=_7_PARAM_SHIFT)

    gdal_helpers.crop_or_expand(yearly_kwh_27700, yearly_kwh_mask_27700, yearly_kwh_27700, False)

    monthly_wh_27700 = [join(pvmaps_dir, f"wh_m{str(i).zfill(2)}_27700.tif") for i in range(1, 13)]
    for r_in, r_out in zip(monthly_wh_rasters, monthly_wh_27700):
        gdal_helpers.reproject(r_in, r_out, src_srs="EPSG:4326", dst_srs=_7_PARAM_SHIFT)
        gdal_helpers.crop_or_expand(r_out, yearly_kwh_mask_27700, r_out, False)

    return yearly_kwh_27700, yearly_kwh_mask_27700, monthly_wh_27700


def _write_results_to_db(pg_conn,
                         job_id: int,
                         solar_dir: str,
                         resolution_metres: float,
                         peak_power_per_m2: float,
                         yearly_kwh_raster: str,
                         monthly_wh_rasters: List[str],
                         debug_mode: bool):
    if len(monthly_wh_rasters) != 12:
        raise ValueError(f"Expected 12 monthly rasters - got {len(monthly_wh_rasters)}")

    schema = tables.schema(job_id)
    raster_tables = []
    for i, raster in enumerate(monthly_wh_rasters):
        raster_tables.append((raster, f'{tables.SOLAR_PV_TABLE}_m{str(i + 1).zfill(2)}'))
    raster_tables.append((yearly_kwh_raster, tables.SOLAR_PV_TABLE))

    for raster, table in raster_tables:
        sql_command(
            pg_conn,
            """
            DROP TABLE IF EXISTS {table};
            CREATE TABLE {table} (
                x double precision,
                y double precision,
                val real,
                PRIMARY KEY (x, y)
            );
            """,
            table=Identifier(schema, table))
        copy_raster(
            pg_conn, solar_dir, raster, f"{schema}.{table}",
            None, include_nans=False, debug_mode=debug_mode)
        logging.info(f"Loaded {raster} into {table}")

    sql_script_with_bindings(
        pg_conn,
        'pv/post-load.solar-pv.sql',
        {"job_id": job_id,
         "peak_power_per_m2": peak_power_per_m2},
        toid_kwh=Identifier(schema, "toid_kwh"),
        solar_pv=Identifier(schema, tables.SOLAR_PV_TABLE),
        m01=Identifier(schema, raster_tables[0][1]),
        m02=Identifier(schema, raster_tables[1][1]),
        m03=Identifier(schema, raster_tables[2][1]),
        m04=Identifier(schema, raster_tables[3][1]),
        m05=Identifier(schema, raster_tables[4][1]),
        m06=Identifier(schema, raster_tables[5][1]),
        m07=Identifier(schema, raster_tables[6][1]),
        m08=Identifier(schema, raster_tables[7][1]),
        m09=Identifier(schema, raster_tables[8][1]),
        m10=Identifier(schema, raster_tables[9][1]),
        m11=Identifier(schema, raster_tables[10][1]),
        m12=Identifier(schema, raster_tables[11][1]),
        panel_kwh=Identifier(schema, "panel_kwh"),
        panel_polygons=Identifier(schema, tables.PANEL_POLYGON_TABLE),
        building_exclusion_reasons=Identifier(schema, tables.BUILDING_EXCLUSION_REASONS_TABLE),
        job_view=Identifier(f"solar_pv_job_{job_id}"),
        res=Literal(resolution_metres))
