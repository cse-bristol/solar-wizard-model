import logging

from psycopg2.sql import Literal

from albion_models.db_funcs import command_to_gpkg
from albion_models.ogr_helpers import get_layer_names

_PANELS = "panels"
_INSTALLATIONS = "installations"


def export(pg_conn, pg_uri: str, gpkg_fname: str, os_run_id: int, job_id: int, regenerate: bool):
    """
    Export data needed for PanelArray in
    https://github.com/cse-bristol/710-open-solar-webapp/blob/master/opensolar/backend/models.py
    :param pg_conn:
    :param pg_uri:
    :param gpkg_fname: file name of gpkg file to add data to (or create if doesn't exist yet)
    :param os_run_id: Run to export from (no check is done that that job_id is from this run, just used in the o/p)
    :param job_id: Job to export from
    """
    layer_names = get_layer_names(gpkg_fname)
    if regenerate or _INSTALLATIONS not in layer_names:
        if command_to_gpkg(
            pg_conn, pg_uri, gpkg_fname, _INSTALLATIONS,
            src_srs=4326, dst_srs=4326,
            overwrite=True,
            command=f"""
            WITH kwh_per_kwp AS (
                SELECT 
                    roof_plane_id,
                    CASE WHEN SUM(kwp) = 0 THEN 0 ELSE SUM(kwh_year) / SUM(kwp) END AS kwh_per_kwp
                FROM models.pv_panel
                WHERE job_id = {job_id} 
                GROUP BY roof_plane_id
            )
            SELECT 
                {os_run_id} AS run_id,
                rp.job_id AS job_id,
                rp.toid AS toid,
                rp.roof_plane_id AS roof_plane_id,
                rp.horizon AS horizon,
                rp.slope AS slope,
                rp.aspect AS aspect,
                rp.x_coef AS x_coef,
                rp.y_coef AS y_coef,
                rp.intercept AS intercept,
                rp.is_flat AS is_flat,
                kwh_per_kwp.kwh_per_kwp AS kwh_per_kwp
            FROM models.pv_roof_plane rp 
            LEFT JOIN kwh_per_kwp 
            ON rp.roof_plane_id = kwh_per_kwp.roof_plane_id 
            WHERE job_id = {job_id}
            """,
            os_run_id=Literal(os_run_id),
            job_id=Literal(job_id)
        ) is not None:
            raise RuntimeError("Error running ogr2ogr")

    if regenerate or _PANELS not in layer_names:
        if command_to_gpkg(
            pg_conn, pg_uri, gpkg_fname, _PANELS,
            src_srs=4326, dst_srs=4326,
            overwrite=True,
            command=f"""
            SELECT 
                {os_run_id} AS run_id,
                job_id AS job_id,
                toid AS toid,
                roof_plane_id AS roof_plane_id,
                panel_id AS panel_id,
                panel_geom_4326 AS panel_geom_4326,
                kwh_jan AS jan_avg_energy_prod_kwh_per_month,
                kwh_feb AS feb_avg_energy_prod_kwh_per_month,
                kwh_mar AS mar_avg_energy_prod_kwh_per_month,
                kwh_apr AS apr_avg_energy_prod_kwh_per_month,
                kwh_may AS may_avg_energy_prod_kwh_per_month,
                kwh_jun AS jun_avg_energy_prod_kwh_per_month,
                kwh_jul AS jul_avg_energy_prod_kwh_per_month,
                kwh_aug AS aug_avg_energy_prod_kwh_per_month,
                kwh_sep AS sep_avg_energy_prod_kwh_per_month,
                kwh_oct AS oct_avg_energy_prod_kwh_per_month,
                kwh_nov AS nov_avg_energy_prod_kwh_per_month,
                kwh_dec AS dec_avg_energy_prod_kwh_per_month,
                kwh_year AS total_avg_energy_prod_kwh_per_year,
                kwp AS peak_power,
                horizon AS horizon,
                area AS area,
                footprint AS footprint,
                ST_AsGeoJSON(panel_geom_4326) AS geom_str,
                CASE WHEN kwp = 0 THEN 0 ELSE kwh_year / kwp END AS kwh_per_kwp     
            FROM models.pv_panel
            WHERE job_id = {job_id}
            """,
            os_run_id=Literal(os_run_id),
            job_id=Literal(job_id)
        ) is not None:
            raise RuntimeError("Error running ogr2ogr")
    else:
        logging.info(f"Not regenerating existing {gpkg_fname}")
