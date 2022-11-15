from psycopg2.sql import Literal

from albion_models.db_funcs import command_to_gpkg


def export(pg_conn, pg_uri: str, gpkg: str, os_run_id: int, job_id: int):
    """
    Export data needed for PanelArray in
    https://github.com/cse-bristol/710-open-solar-webapp/blob/master/opensolar/backend/models.py
    :param pg_conn:
    :param pg_uri:
    :param gpkg: file name of gpkg file to add data to (or create if doesn't exist yet)
    :param os_run_id: Run to export from (no check is done that that job_id is from this run, just used in the o/p)
    :param job_id: Job to export from
    """
    command_to_gpkg(
        pg_conn, pg_uri, gpkg, "panels",
        src_srs=4326, dst_srs=4326,
        overwrite=True,
        command=f"""
        SELECT 
            {os_run_id} AS run_id,
            pv.job_id AS job_id,
            pv.toid AS toid,
            pv.roof_plane_id AS roof_plane_id,
            pv.roof_geom_4326 AS roof_geom_4326,
            pv.kwh_jan AS jan_avg_energy_prod_kwh_per_month,
            pv.kwh_feb AS feb_avg_energy_prod_kwh_per_month,
            pv.kwh_mar AS mar_avg_energy_prod_kwh_per_month,
            pv.kwh_apr AS apr_avg_energy_prod_kwh_per_month,
            pv.kwh_may AS may_avg_energy_prod_kwh_per_month,
            pv.kwh_jun AS jun_avg_energy_prod_kwh_per_month,
            pv.kwh_jul AS jul_avg_energy_prod_kwh_per_month,
            pv.kwh_aug AS aug_avg_energy_prod_kwh_per_month,
            pv.kwh_sep AS sep_avg_energy_prod_kwh_per_month,
            pv.kwh_oct AS oct_avg_energy_prod_kwh_per_month,
            pv.kwh_nov AS nov_avg_energy_prod_kwh_per_month,
            pv.kwh_dec AS dec_avg_energy_prod_kwh_per_month,
            pv.kwh_year AS total_avg_energy_prod_kwh_per_year,
            pv.kwp AS peak_power,
            pv.horizon AS horizon,
            pv.slope AS slope,
            pv.aspect AS aspect,
            pv.area AS area,
            pv.footprint AS footprint,
            pv.x_coef AS x_coef,
            pv.y_coef AS y_coef,
            pv.intercept AS intercept,
            pv.is_flat AS is_flat,
            ST_AsGeoJSON(pv.roof_geom_4326) AS geom_str        
        FROM models.solar_pv pv
        WHERE pv.job_id = {job_id}
        AND pv.kwh_year <> double precision 'NaN'
        AND pv.kwh_year IS NOT NULL
        """,
        os_run_id=Literal(os_run_id),
        job_id=Literal(job_id)
        )
