from os.path import join
from typing import List

import os

from psycopg2.sql import Identifier, Literal

import albion_models.solar_pv.tables as tables
from albion_models import paths
from albion_models.db_funcs import sql_script, sql_script_with_bindings, connection, \
    sql_command
from albion_models.solar_pv.pvgis import pvmaps
from albion_models.solar_pv.rasters import copy_raster
from albion_models.util import get_cpu_count


def _pvmaps_cpu_count():
    """Use 3/4s of available CPUs for PVMAPS"""
    return int(get_cpu_count() * 0.75)


def pvgis(pg_uri: str,
          job_id: int,
          solar_dir: str,
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
     * apply peak_power_per_m2 adjustment
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

    pvm = pvmaps.PVMaps(
        grass_dbase_dir=os.environ.get("PVGIS_GRASS_DBASE", None),
        input_dir=pvmaps_dir,
        output_dir=pvmaps_dir,
        pvgis_data_tar_file=join(os.environ.get("PVGIS_DATA_DIR", None), "pvgis_data.tar"),
        pv_model_coeff_file_dir=paths.RESOURCES_DIR,
        keep_temp_mapset=debug_mode,
        num_processes=_pvmaps_cpu_count(),
        output_direct_diffuse=False,
        horizon_step_degrees=horizon_step_degrees,
        horizon_search_distance=horizon_search_radius,
        flat_roof_degrees=flat_roof_degrees,
        flat_roof_degrees_threshold=5.0,
        panel_type=panel_type,
    )
    yearly_kwh_raster, monthly_kwh_rasters = pvm.create_pvmap(os.path.basename(elevation_raster), os.path.basename(mask_raster))
    with connection(pg_uri) as pg_conn:
        _write_results_to_db(
            pg_conn, job_id, solar_dir, peak_power_per_m2, yearly_kwh_raster,
            monthly_kwh_rasters, mask_raster, debug_mode)


def _write_results_to_db(pg_conn,
                         job_id: int,
                         solar_dir: str,
                         peak_power_per_m2: float,
                         yearly_kwh_raster: str,
                         monthly_kwh_rasters: List[str],
                         mask_raster: str,
                         debug_mode: bool):
    if len(monthly_kwh_rasters) != 12:
        raise ValueError(f"Expected 12 monthly rasters - got {len(monthly_kwh_rasters)}")

    schema = tables.schema(job_id)
    raster_tables = []
    for i, raster in enumerate(monthly_kwh_rasters):
        raster_tables.append((raster, f'{tables.SOLAR_PV_TABLE}_m{str(i + 1).zfill(2)}'))
    raster_tables.append((yearly_kwh_raster, tables.SOLAR_PV_TABLE))

    for raster, table in raster_tables:
        sql_command(
            pg_conn,
            """
            DROP TABLE IF EXISTS {table};
            CREATE TABLE {table} (
                lon double precision,
                lat double precision,
                kwh real,
                PRIMARY KEY (lon, lat)
            );
            """,
            table=Identifier(schema, table))
        copy_raster(pg_conn, solar_dir, raster, f"{schema}.{table}", mask_raster, debug_mode)

    sql_script_with_bindings(
        pg_conn,
        'pv/post-load.solar-pv.sql',
        {"job_id": job_id,
         "peak_power_per_m2": peak_power_per_m2},
        toid_kwh=Identifier(schema, "toid_kwh"),
        solar_pv=Identifier(schema, tables.SOLAR_PV_TABLE),
        panel_horizons=Identifier(schema, tables.PANEL_POLYGON_TABLE),
        building_exclusion_reasons=Identifier(schema, tables.BUILDING_EXCLUSION_REASONS_TABLE),
        job_view=Identifier(f"solar_pv_job_{job_id}"),
        srid=Literal(4326))
