from psycopg2.sql import Identifier

import albion_models.solar_pv.tables as tables
from albion_models.db_funcs import sql_script, sql_script_with_bindings
from albion_models.solar_pv.rasters import copy_raster


def pvgis(pg_uri: str,
          job_id: int,
          solar_dir: str,
          pv_tech: str,
          horizon_search_radius: int,
          horizon_slices: int,
          peak_power_per_m2: float,
          elevation_raster: str,
          mask_raster: str,
          debug_mode: bool):
    """
    TODO:
     * run r.mask, r.horizon and r.pv
       and maybe some other commands too - see section 4 of pvmaps PDF documentation
     * once all the rasters have been made, load them into the database
     * the creation and post-load SQL scripts will need to be extended - see below
    """


def _write_results_to_db(pg_conn,
                         job_id: int,
                         solar_dir: str,
                         peak_power_per_m2: float,
                         kwh_raster: str,
                         mask_raster: str,
                         debug_mode: bool):
    # TODO:
    #  I don't know how many separate raster outputs we'll get - they'll all need combining
    #  and the SQL scripts below will need editing (they are from the old approach)
    #  See albion_models.solar_pv.rasters._load_rasters_to_db() and the post-load SQL it calls
    #  for the basic approach to loading/combining rasters
    #  On the various fields that we currently get as output from PVGIS:
    #  * we don't need the monthly kWh values, just the yearly (total_avg_energy_prod_kwh_per_year)
    #  * we don't use the various loss values (aoi_loss, spectral_loss, temp_irr_loss, total_loss)
    #    but it would be good to know they're still being applied to the total - see section 4 of pvmaps pdf
    #    The HTTP API takes a loss parameter too which we set to the recommended value of 14, this probably needs applying manually
    sql_script(
        pg_conn,
        'pv/create.solar-pv.sql',
        solar_pv=Identifier(tables.schema(job_id), tables.SOLAR_PV_TABLE))

    copy_raster(pg_conn, solar_dir, kwh_raster, f'{tables.schema(job_id)}.{tables.SOLAR_PV_TABLE}', mask_raster, debug_mode)

    sql_script_with_bindings(
        pg_conn,
        'pv/post-load.solar-pv.sql',
        {"job_id": job_id,
         "peak_power_per_m2": peak_power_per_m2},
        toid_kwh=Identifier(tables.schema(job_id), "toid_kwh"),
        solar_pv=Identifier(tables.schema(job_id), tables.SOLAR_PV_TABLE),
        panel_horizons=Identifier(tables.schema(job_id), tables.PANEL_POLYGON_TABLE),
        building_exclusion_reasons=Identifier(tables.schema(job_id), tables.BUILDING_EXCLUSION_REASONS_TABLE),
        job_view=Identifier(f"solar_pv_job_{job_id}"))
