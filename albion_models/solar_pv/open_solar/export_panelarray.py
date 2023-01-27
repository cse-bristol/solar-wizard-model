from psycopg2.sql import Literal

from albion_models.db_funcs import command_to_gpkg

L_PANELS = "panels"
L_INSTALLATIONS = "installations"


def export(pg_conn, pg_uri: str, gpkg_fname: str, os_run_id: int, job_id: int):
    """
    Export data needed for PanelArray in
    https://github.com/cse-bristol/710-open-solar-webapp/blob/master/opensolar/backend/models.py
    :param pg_conn:
    :param pg_uri:
    :param gpkg_fname: file name of gpkg file to add data to (or create if doesn't exist yet)
    :param os_run_id: Run to export from (no check is done that that job_id is from this run, just used in the o/p)
    :param job_id: Job to export from
    """
    # The "installation_id" column below is needed as django doesn't support multi-column foreign keys or joins
    if command_to_gpkg(
        pg_conn, pg_uri, gpkg_fname, L_INSTALLATIONS,
        src_srs=4326, dst_srs=4326,
        overwrite=True,
        command=f"""
        WITH panels AS (
            SELECT 
                roof_plane_id,
                round(SUM(kwh_jan)::numeric, 2) AS kwh_jan,
                round(SUM(kwh_feb)::numeric, 2) AS kwh_feb,
                round(SUM(kwh_mar)::numeric, 2) AS kwh_mar,
                round(SUM(kwh_apr)::numeric, 2) AS kwh_apr,
                round(SUM(kwh_may)::numeric, 2) AS kwh_may,
                round(SUM(kwh_jun)::numeric, 2) AS kwh_jun,
                round(SUM(kwh_jul)::numeric, 2) AS kwh_jul,
                round(SUM(kwh_aug)::numeric, 2) AS kwh_aug,
                round(SUM(kwh_sep)::numeric, 2) AS kwh_sep,
                round(SUM(kwh_oct)::numeric, 2) AS kwh_oct,
                round(SUM(kwh_nov)::numeric, 2) AS kwh_nov,
                round(SUM(kwh_dec)::numeric, 2) AS kwh_dec,
                round(SUM(kwh_year)::numeric, 2) AS kwh_year,
                round(SUM(kwp)::numeric, 2) AS kwp, 
                round(SUM(area)::numeric, 2) AS area,
                round(SUM(footprint)::numeric, 2) AS footprint,
                round(CASE WHEN SUM(kwp) IS NULL THEN 0::numeric ELSE (SUM(kwh_year) / SUM(kwp))::numeric END, 2) AS kwh_per_kwp,
                COUNT(*) AS panels
            FROM models.pv_panel
            WHERE job_id = {job_id} 
            GROUP BY roof_plane_id
        )
        SELECT
            rp.toid || '_' || rp.roof_plane_id AS installation_id,
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
            panels.kwh_jan AS jan_avg_energy_prod_kwh_per_month,
            panels.kwh_feb AS feb_avg_energy_prod_kwh_per_month,
            panels.kwh_mar AS mar_avg_energy_prod_kwh_per_month,
            panels.kwh_apr AS apr_avg_energy_prod_kwh_per_month,
            panels.kwh_may AS may_avg_energy_prod_kwh_per_month,
            panels.kwh_jun AS jun_avg_energy_prod_kwh_per_month,
            panels.kwh_jul AS jul_avg_energy_prod_kwh_per_month,
            panels.kwh_aug AS aug_avg_energy_prod_kwh_per_month,
            panels.kwh_sep AS sep_avg_energy_prod_kwh_per_month,
            panels.kwh_oct AS oct_avg_energy_prod_kwh_per_month,
            panels.kwh_nov AS nov_avg_energy_prod_kwh_per_month,
            panels.kwh_dec AS dec_avg_energy_prod_kwh_per_month,
            panels.kwh_year AS total_avg_energy_prod_kwh_per_year,
            panels.kwp AS peak_power,
            panels.kwh_per_kwp AS kwh_per_kwp,
            panels.area AS area,
            panels.footprint AS footprint,
            panels.panels AS panels
        FROM models.pv_roof_plane rp 
        INNER JOIN panels 
        ON rp.roof_plane_id = panels.roof_plane_id 
        WHERE job_id = {job_id}
        """,  # using inner join above so that roof planes with no panels are not included (they shouldn't be in
              # pv_roof_plane, but they are)
        os_run_id=Literal(os_run_id),
        job_id=Literal(job_id)
    ) is not None:
        raise RuntimeError("Error running ogr2ogr")

    # The "installation_id" column below is needed as django doesn't support multi-column foreign keys or joins
    if command_to_gpkg(
        pg_conn, pg_uri, gpkg_fname, L_PANELS,
        src_srs=4326, dst_srs=4326,
        overwrite=True,
        command=f"""
        SELECT 
            toid || '_' || roof_plane_id AS installation_id,
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
