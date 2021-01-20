import logging
import os
import textwrap

import time
from typing import Optional, List

import psycopg2
import psycopg2.extras

from albion_models.lidar.get_lidar import get_all_lidar
from albion_models.hard_soft_dig.model_hard_soft_dig import model_hard_soft_dig
from albion_models.heat_demand.model_heat_demand import model_heat_demand
from albion_models.solar_pv.model_solar_pv import model_solar_pv


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
            UPDATE models.job_queue q SET status = 'IN_PROGRESS' FROM next
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
                q.status,
                q.email,
                q.params
        """)
        pg_conn.commit()
        return cursor.fetchone()


def _set_job_status(pg_conn, job_id: int, status: str, error: str = None):
    with pg_conn.cursor() as cursor:
        cursor.execute("UPDATE models.job_queue "
                       "SET status = %s, error = %s WHERE job_id = %s",
                       (status, error, job_id))
        pg_conn.commit()


def _handle_job(pg_conn, job: dict) -> bool:
    job_id: int = job['job_id']
    project: str = job['project']
    bounds: str = job['bounds']
    params: dict = job['params']
    logging.info(f"Handling job {job_id}, project {project}")

    if job['soft_dig']:
        soft_ground_buffer_metres = params.get('soft_ground_buffer_metres', 10)
        model_hard_soft_dig(
            pg_conn, job_id, bounds, soft_ground_buffer_metres
        )

    if job['heat_demand'] or job['solar_pv'] or job['lidar']:
        lidar_tiff_paths = get_all_lidar(pg_conn, job_id, os.environ.get("LIDAR_DIR"))

        if job['heat_demand']:
            heat_degree_days = params.get('heat_degree_days', 2033.313)
            model_heat_demand(pg_conn, job_id, bounds, lidar_tiff_paths, os.environ.get("HEAT_DEMAND_DIR"), heat_degree_days)
        if job['solar_pv']:
            horizon_search_radius = params.get('horizon_search_radius', 1000)
            horizon_slices = params.get('horizon_slices', 16)
            max_roof_slope_degrees = params.get('max_roof_slope_degrees', 80)
            min_roof_area_m = params.get('min_roof_area_m', 10)
            min_roof_degrees_from_north = params.get('min_roof_degrees_from_north', 45)
            flat_roof_degrees = params.get('flat_roof_degrees', 10)
            peak_power_per_m2 = params.get('peak_power_per_m2', 0.120)
            pv_tech = params.get('pv_tech', 'crystSi')
            roof_area_percent_usable = params.get('roof_area_percent_usable', 75)
            max_avg_southerly_horizon_degrees = params.get('max_avg_southerly_horizon_degrees', 35)
            model_solar_pv(
                pg_uri=os.environ.get("PG_URI"),
                root_solar_dir=os.environ.get("SOLAR_DIR"),
                job_id=job_id,
                lidar_paths=lidar_tiff_paths,
                horizon_search_radius=horizon_search_radius,
                horizon_slices=horizon_slices,
                max_roof_slope_degrees=max_roof_slope_degrees,
                min_roof_area_m=min_roof_area_m,
                roof_area_percent_usable=roof_area_percent_usable,
                min_roof_degrees_from_north=min_roof_degrees_from_north,
                flat_roof_degrees=flat_roof_degrees,
                peak_power_per_m2=peak_power_per_m2,
                pv_tech=pv_tech,
                max_avg_southerly_horizon_degrees=max_avg_southerly_horizon_degrees)

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
        subject=f"Albion result extraction job '{project}' failed",
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

    with smtplib.SMTP('smtp.office365.com', 587) as mailserver:
        mailserver.ehlo()
        mailserver.starttls()
        mailserver.login(from_email, password)
        mailserver.send_message(msg)


if __name__ == "__main__":
    main_loop()
