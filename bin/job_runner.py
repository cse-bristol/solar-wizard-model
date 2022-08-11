import logging
import os
import textwrap

import time
from typing import Optional, List

import psycopg2
import psycopg2.extras

from albion_models import gdal_helpers
from albion_models.lidar.bulk_lidar_client import load_from_bulk
from albion_models.lidar.defra_lidar_api_client import get_all_lidar
from albion_models.lidar.lidar_coverage import calculate_lidar_coverage
from albion_models.hard_soft_dig.model_hard_soft_dig import model_hard_soft_dig
from albion_models.heat_demand.model_heat_demand import model_heat_demand, \
    model_insulation_measure_costs
from albion_models.solar_pv.model_solar_pv import model_solar_pv
from albion_models.solar_pv.cost_benefit.model_cost_benefit import model_cost_benefit
from albion_models.db_funcs import process_pg_uri


def main_loop():
    """
    Main loop - every 60 seconds, poll the job_queue table in the database for
    new jobs.

    systemd handles restarting in case of error.
    """
    logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')
    pg_uri = os.environ.get("PG_URI")
    pg_conn = _connect(pg_uri)
    while True:
        job = _get_next_job(pg_conn)
        if job is not None:
            try:
                success = _handle_job(pg_conn, job)
                _set_job_status(pg_conn, job['job_id'], 'COMPLETE' if success else 'FAILED')
            except Exception as e:
                pg_conn.rollback()
                err_message = "Job failed: {0}. Arguments:\n{1!r}".format(type(e).__name__, e.args)
                _set_job_status(pg_conn, job['job_id'], 'FAILED', err_message)
                _send_failure_email(job['email'], job['job_id'], job['project'], err_message)
                raise
        time.sleep(60)


def _connect(pg_uri):
    return psycopg2.connect(pg_uri, cursor_factory=psycopg2.extras.DictCursor)


def _get_next_job(pg_conn) -> Optional[dict]:
    """
    Get the earliest NOT_STARTED job row which isn't locked, and set it's status
    to IN_PROGRESS.
    """
    with pg_conn.cursor() as cursor:
        cursor.execute("""
            WITH next AS (
                SELECT job_id FROM models.job_queue
                WHERE status = 'NOT_STARTED'
                ORDER BY created_at ASC
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            )
            UPDATE models.job_queue q 
            SET status = 'IN_PROGRESS', started_at = NOW() 
            FROM next
            WHERE next.job_id = q.job_id
            RETURNING
                q.job_id,
                q.project,
                q.created_at,
                ST_AsText(ST_Transform(q.bounds, 4326)) AS bounds,
                q.solar_pv,
                q.heat_demand,
                q.soft_dig,
                q.lidar,
                q.solar_pv_cost_benefit,
                q.status,
                q.email,
                q.params
        """)
        pg_conn.commit()
        return cursor.fetchone()


def _set_job_status(pg_conn, job_id: int, status: str, error: str = None):
    with pg_conn.cursor() as cursor:
        cursor.execute("UPDATE models.job_queue "
                       "SET status = %s, error = %s, finished_at = NOW() WHERE job_id = %s",
                       (status, error, job_id))
        pg_conn.commit()


def _handle_job(pg_conn, job: dict) -> bool:
    job_id: int = job['job_id']
    project: str = job['project']
    bounds: str = job['bounds']
    params: dict = job['params']
    pg_uri = process_pg_uri(os.environ.get("PG_URI"))
    logging.info(f"Handling job {job_id}, project {project}")

    if job['soft_dig']:
        soft_ground_buffer_metres = params['soft_ground_buffer_metres']
        model_hard_soft_dig(
            pg_conn, job_id, bounds, soft_ground_buffer_metres
        )

    if job['heat_demand'] or job['solar_pv'] or job['lidar']:
        lidar_dir = os.environ.get("LIDAR_DIR")
        bulk_lidar_dir = os.environ.get("BULK_LIDAR_DIR", None)
        if not bulk_lidar_dir:
            logging.info("BULK_LIDAR_DIR not set, falling back to getting LiDAR from DEFRA API")
            get_all_lidar(pg_conn, job_id, os.path.join(lidar_dir, "defra"))
        else:
            load_from_bulk(pg_conn, job_id, lidar_dir, bulk_lidar_dir)

        if job['lidar']:
            calculate_lidar_coverage(job_id, pg_uri)

        if job['heat_demand']:
            heat_degree_days = params['heat_degree_days']
            model_heat_demand(
                pg_conn,
                job_id,
                bounds,
                os.environ.get("HEAT_DEMAND_DIR"),
                heat_degree_days)

            include_cwi = params['include_cwi']
            include_swi = params['include_swi']
            include_loft_ins = params['include_loft_ins']
            include_roof_ins = params['include_roof_ins']
            include_floor_ins = params['include_floor_ins']
            include_glazing = params['include_glazing']

            cwi_max_pct_area = params['cwi_max_pct_area']
            swi_max_pct_area = params['swi_max_pct_area']
            loft_ins_max_pct_area = params['loft_ins_max_pct_area']
            roof_ins_max_pct_area = params['roof_ins_max_pct_area']
            floor_ins_max_pct_area = params['floor_ins_max_pct_area']
            glazing_max_pct_area = params['glazing_max_pct_area']

            cwi_per_m2_cost = params['cwi_per_m2_cost']
            swi_per_m2_cost = params['swi_per_m2_cost']
            loft_ins_per_m2_cost = params['loft_ins_per_m2_cost']
            roof_ins_per_m2_cost = params['roof_ins_per_m2_cost']
            floor_ins_per_m2_cost = params['floor_ins_per_m2_cost']
            glazing_per_m2_cost = params['glazing_per_m2_cost']

            cwi_fixed_cost = params['cwi_fixed_cost']
            swi_fixed_cost = params['swi_fixed_cost']
            loft_ins_fixed_cost = params['loft_ins_fixed_cost']
            roof_ins_fixed_cost = params['roof_ins_fixed_cost']
            floor_ins_fixed_cost = params['floor_ins_fixed_cost']
            glazing_fixed_cost = params['glazing_fixed_cost']

            cwi_pct_demand_reduction = params['cwi_pct_demand_reduction']
            swi_pct_demand_reduction = params['swi_pct_demand_reduction']
            loft_ins_pct_demand_reduction = params['loft_ins_pct_demand_reduction']
            roof_ins_pct_demand_reduction = params['roof_ins_pct_demand_reduction']
            floor_ins_pct_demand_reduction = params['floor_ins_pct_demand_reduction']
            glazing_pct_demand_reduction = params['glazing_pct_demand_reduction']

            model_insulation_measure_costs(
                pg_conn,
                job_id=job_id,
                include_cwi=include_cwi,
                include_swi=include_swi,
                include_loft_ins=include_loft_ins,
                include_roof_ins=include_roof_ins,
                include_floor_ins=include_floor_ins,
                include_glazing=include_glazing,

                cwi_max_pct_area=cwi_max_pct_area,
                swi_max_pct_area=swi_max_pct_area,
                loft_ins_max_pct_area=loft_ins_max_pct_area,
                roof_ins_max_pct_area=roof_ins_max_pct_area,
                floor_ins_max_pct_area=floor_ins_max_pct_area,
                glazing_max_pct_area=glazing_max_pct_area,

                cwi_per_m2_cost=cwi_per_m2_cost,
                swi_per_m2_cost=swi_per_m2_cost,
                loft_ins_per_m2_cost=loft_ins_per_m2_cost,
                roof_ins_per_m2_cost=roof_ins_per_m2_cost,
                floor_ins_per_m2_cost=floor_ins_per_m2_cost,
                glazing_per_m2_cost=glazing_per_m2_cost,

                cwi_fixed_cost=cwi_fixed_cost,
                swi_fixed_cost=swi_fixed_cost,
                loft_ins_fixed_cost=loft_ins_fixed_cost,
                roof_ins_fixed_cost=roof_ins_fixed_cost,
                floor_ins_fixed_cost=floor_ins_fixed_cost,
                glazing_fixed_cost=glazing_fixed_cost,

                cwi_pct_demand_reduction=cwi_pct_demand_reduction,
                swi_pct_demand_reduction=swi_pct_demand_reduction,
                loft_ins_pct_demand_reduction=loft_ins_pct_demand_reduction,
                roof_ins_pct_demand_reduction=roof_ins_pct_demand_reduction,
                floor_ins_pct_demand_reduction=floor_ins_pct_demand_reduction,
                glazing_pct_demand_reduction=glazing_pct_demand_reduction)

        if job['solar_pv']:
            horizon_search_radius = params['horizon_search_radius']
            horizon_slices = params['horizon_slices']
            max_roof_slope_degrees = params['max_roof_slope_degrees']
            min_roof_area_m = params['min_roof_area_m']
            min_roof_degrees_from_north = params['min_roof_degrees_from_north']
            flat_roof_degrees = params['flat_roof_degrees']
            peak_power_per_m2 = params['peak_power_per_m2']
            pv_tech = params['pv_tech']
            panel_width_m = params['panel_width_m']
            panel_height_m = params['panel_height_m']
            panel_spacing_m = params['panel_spacing_m']
            large_building_threshold = params['large_building_threshold']
            min_dist_to_edge_m = params['min_dist_to_edge_m']
            min_dist_to_edge_large_m = params['min_dist_to_edge_large_m']
            debug_mode = params.get('debug_mode', False)
            model_solar_pv(
                pg_uri=pg_uri,
                root_solar_dir=os.environ.get("SOLAR_DIR"),
                job_id=job_id,
                horizon_search_radius=horizon_search_radius,
                horizon_slices=horizon_slices,
                max_roof_slope_degrees=max_roof_slope_degrees,
                min_roof_area_m=min_roof_area_m,
                min_roof_degrees_from_north=min_roof_degrees_from_north,
                flat_roof_degrees=flat_roof_degrees,
                peak_power_per_m2=peak_power_per_m2,
                pv_tech=pv_tech,
                panel_width_m=panel_width_m,
                panel_height_m=panel_height_m,
                panel_spacing_m=panel_spacing_m,
                large_building_threshold=large_building_threshold,
                min_dist_to_edge_m=min_dist_to_edge_m,
                min_dist_to_edge_large_m=min_dist_to_edge_large_m,
                debug_mode=debug_mode)

    if job['solar_pv_cost_benefit']:
        solar_pv_job_id = params["solar_pv_job_id"]
        period_years = params["period_years"]
        discount_rate = params["discount_rate"]
        electricity_kwh_costs = params["electricity_kwh_costs"]

        small_inst_cost_per_kwp = params["small_inst_cost_per_kwp"]
        med_inst_cost_per_kwp = params["med_inst_cost_per_kwp"]
        large_inst_cost_per_kwp = params["large_inst_cost_per_kwp"]
        small_inst_fixed_cost = params["small_inst_fixed_cost"]
        med_inst_fixed_cost = params["med_inst_fixed_cost"]
        large_inst_fixed_cost = params["large_inst_fixed_cost"]
        small_inst_vat = params["small_inst_vat"]
        med_inst_vat = params["med_inst_vat"]
        large_inst_vat = params["large_inst_vat"]

        exclude_already_have_pv = params["exclude_already_have_pv"]
        exclude_listed = params["exclude_listed"]

        model_cost_benefit(
            pg_uri=pg_uri,
            job_id=job_id,
            solar_pv_job_id=solar_pv_job_id,
            period_years=period_years,
            discount_rate=discount_rate,
            electricity_kwh_costs=electricity_kwh_costs,
            small_inst_cost_per_kwp=small_inst_cost_per_kwp,
            med_inst_cost_per_kwp=med_inst_cost_per_kwp,
            large_inst_cost_per_kwp=large_inst_cost_per_kwp,
            small_inst_fixed_cost=small_inst_fixed_cost,
            med_inst_fixed_cost=med_inst_fixed_cost,
            large_inst_fixed_cost=large_inst_fixed_cost,
            small_inst_vat=small_inst_vat,
            med_inst_vat=med_inst_vat,
            large_inst_vat=large_inst_vat,
            exclude_already_have_pv=exclude_already_have_pv,
            exclude_listed=exclude_listed)

    logging.info(f"Completed job {job_id}, project {project}")
    _send_success_email(job['email'], job_id, project)
    return True


def _send_failure_email(to_email: str, job_id: int, project: str, error: str):
    all_recipients = []
    notify_on_failure = os.environ.get("EMAIL_TO_NOTIFY_ON_FAILURE")
    if to_email:
        all_recipients.append(to_email)
    if notify_on_failure:
        all_recipients.append(notify_on_failure)
    if len(all_recipients) == 0:
        return

    _send_email(
        from_email=os.environ.get("SMTP_FROM"),
        to_email=all_recipients,
        password=os.environ.get("SMTP_PASS"),
        subject=f"Albion modelling job '{project}' failed",
        body=textwrap.dedent(f"""
            Hello,
            
            Unfortunately your Albion job '{project}', ID {job_id} has failed.
            
            Error: {error}
            """),
    )


def _send_success_email(to_email: str, job_id: int, project: str):
    if not to_email:
        return

    _send_email(
        from_email=os.environ.get("SMTP_FROM"),
        to_email=[to_email],
        password=os.environ.get("SMTP_PASS"),
        subject=f"Albion job '{project}' complete",
        body=textwrap.dedent(f"""
            Hello,
            
            Your Albion job '{project}', ID {job_id} has completed and can be viewed here:
            
            http://albion.r.cse.org.uk/completed-jobs
            
            You can now extract the results here: http://albion.r.cse.org.uk/extract-results
            """),
    )


def _send_email(from_email: str, to_email: List[str], password: str, subject: str, body: str):
    import smtplib
    from email.message import EmailMessage
    msg = EmailMessage()
    msg['Subject'] = subject
    msg['From'] = from_email
    msg['To'] = ', '.join(to_email)
    msg.set_content(body)

    try:
        with smtplib.SMTP('smtp.office365.com', 587) as mailserver:
            mailserver.ehlo()
            mailserver.starttls()
            mailserver.login(from_email, password)
            mailserver.send_message(msg)
    except smtplib.SMTPException:
        logging.exception("Failed to send email")


if __name__ == "__main__":
    main_loop()
