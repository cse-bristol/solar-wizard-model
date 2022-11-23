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
from albion_models.solar_pv.rasters import copy_raster
from albion_models.transformations import _7_PARAM_SHIFT
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
          elevation_override_raster: Optional[str],
          mask_raster: str,
          flat_roof_aspect_raster: Optional[str],
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
        elevation_override_filename=os.path.basename(elevation_override_raster) if elevation_override_raster else None
    )

    yearly_kwh_raster = pvm.yearly_kwh_raster
    monthly_wh_rasters = pvm.monthly_wh_rasters
    # horizons are CCW from East
    horizon_rasters = pvm.horizons

    yearly_kwh_27700, mask_27700, monthly_wh_27700, horizon_27700 = _generate_27700_rasters(
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


def _generate_27700_rasters(pvmaps_dir: str,
                            yearly_kwh_raster: str,
                            mask_raster: str,
                            monthly_wh_rasters: List[str],
                            horizon_rasters: List[str]):
    if len(monthly_wh_rasters) != 12:
        raise ValueError(f"Expected 12 monthly rasters - got {len(monthly_wh_rasters)}")

    yearly_kwh_27700 = join(pvmaps_dir, "kwh_year_27700.tif")
    mask_27700 = join(pvmaps_dir, "mask_27700.tif")
    gdal_helpers.reproject(yearly_kwh_raster, yearly_kwh_27700, src_srs="EPSG:4326", dst_srs=_7_PARAM_SHIFT)
    gdal_helpers.reproject(mask_raster, mask_27700, src_srs="EPSG:4326", dst_srs=_7_PARAM_SHIFT)

    gdal_helpers.crop_or_expand(yearly_kwh_27700, mask_27700, yearly_kwh_27700, True)

    monthly_wh_27700 = [join(pvmaps_dir, f"wh_m{str(i).zfill(2)}_27700.tif") for i in range(1, 13)]
    for r_in, r_out in zip(monthly_wh_rasters, monthly_wh_27700):
        gdal_helpers.reproject(r_in, r_out, src_srs="EPSG:4326", dst_srs=_7_PARAM_SHIFT)
        gdal_helpers.crop_or_expand(r_out, mask_27700, r_out, True)

    horizon_27700 = [join(pvmaps_dir, f"horizon_{str(i).zfill(2)}_27700.tif") for i, _ in enumerate(horizon_rasters)]
    for r_in, r_out in zip(horizon_rasters, horizon_27700):
        gdal_helpers.reproject(r_in, r_out, src_srs="EPSG:4326", dst_srs=_7_PARAM_SHIFT)
        gdal_helpers.crop_or_expand(r_out, mask_27700, r_out, True)

    return yearly_kwh_27700, mask_27700, monthly_wh_27700, horizon_27700


def _combine_horizons(pg_conn,
                      job_id: int,
                      horizon_tables: List[str]):
    schema = tables.schema(job_id)
    sql_command(
        pg_conn,
        "ALTER TABLE {pixel_kwh} ADD COLUMN horizon real[]",
        pixel_kwh=Identifier(schema, tables.PIXEL_KWH_TABLE),
    )
    for htable in horizon_tables:
        sql_command(
            pg_conn,
            """UPDATE {pixel_kwh} sp 
               SET horizon = horizon || h.val 
               FROM {htable} h 
               WHERE sp.x = h.x AND sp.y = h.y;
               
               DROP TABLE {htable};""",
            pixel_kwh=Identifier(schema, tables.PIXEL_KWH_TABLE),
            htable=Identifier(schema, htable))
        logging.info(f"combined horizon slice from {htable}")


def _combine_monthly_whs(pg_conn,
                         job_id: int,
                         monthly_wh_tables: List[str]):
    schema = tables.schema(job_id)
    for i, mtable in enumerate(monthly_wh_tables, start=1):
        sql_command(
            pg_conn,
            """ALTER TABLE {pixel_kwh} ADD COLUMN {col} real;
            
               UPDATE {pixel_kwh} sp 
               SET {col} = m.val * 0.001 * {mdays} 
               FROM {month} m 
               WHERE sp.x = m.x AND sp.y = m.y;
               
               DROP TABLE {month};""",
            pixel_kwh=Identifier(schema, tables.PIXEL_KWH_TABLE),
            col=Identifier(f"kwh_m{str(i).zfill(2)}"),
            mdays=Literal(mdays[i]),
            month=Identifier(schema, mtable))
        logging.info(f"combined monthly Wh data from {mtable}")


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
    raster_tables = []
    horizon_tables = []
    monthly_tables = []

    for i, raster in enumerate(monthly_wh_rasters):
        m_id = "m" + str(i + 1).zfill(2)
        m_table = f'{tables.PIXEL_KWH_TABLE}_{m_id}'
        raster_tables.append((raster, m_table))
        monthly_tables.append(m_table)

    for i, raster in enumerate(horizon_rasters):
        h_id = "s" + str(i).zfill(2)
        h_table = f'horizon_{h_id}'
        raster_tables.append((raster, h_table))
        horizon_tables.append(h_table)

    raster_tables.append((yearly_kwh_raster, tables.PIXEL_KWH_TABLE))

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
            pg_conn, solar_dir, raster, f"{schema}.{table}", mask_raster,
            include_nans=False, debug_mode=debug_mode)
        logging.info(f"Loaded {raster} into {table}")

    _combine_monthly_whs(pg_conn, job_id, monthly_tables)
    _combine_horizons(pg_conn, job_id, horizon_tables)

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

    if not debug_mode:
        sql_command(
            pg_conn,
            """
            DROP TABLE {panel_kwh};
            DROP TABLE {pixel_kwh};
            DROP TABLE {pixels_in_panels};
            DROP TABLE {roof_horizons};
            DROP TABLE {pixels_in_roofs};
            """)
