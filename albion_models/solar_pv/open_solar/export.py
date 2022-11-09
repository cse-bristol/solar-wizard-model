import logging
import os
from asyncio import Future
from concurrent.futures import ThreadPoolExecutor
from typing import List, Tuple

from psycopg2.extras import DictCursor

from albion_models.db_funcs import sql_command, connect
from albion_models.solar_pv.open_solar import export_panelarray


def _get_jobs_to_export(pg_conn, os_run_id: int) -> List[int]:
    job_ids = sql_command(
        pg_conn,
        """
        SELECT job_id
        FROM models.job_queue q
        JOIN models.open_solar_jobs osj USING (job_id)
        WHERE osj.os_run_id = %(os_run_id)s
        AND q.status = 'COMPLETE'::models.job_status
        """,
        {
            "os_run_id": os_run_id
        },
        result_extractor=lambda rows: [row["job_id"] for row in rows])
    return job_ids


def _is_all_complete(pg_conn, os_run_id: int) -> bool:
    complete = sql_command(
        pg_conn,
        """
        SELECT count(*) = 0 as complete
        FROM models.job_queue q
        JOIN models.open_solar_jobs osj USING (job_id)
        WHERE osj.os_run_id = %(os_run_id)s
        AND q.status <> 'COMPLETE'::models.job_status
        """,
        {
            "os_run_id": os_run_id
        },
        result_extractor=lambda rows: rows[0][0])
    return complete


def _export(pg_conn, pg_uri: str, gpkg_filename: str, os_run_id: int, job_id: int):
    export_panelarray.export(pg_conn, pg_uri, gpkg_filename, os_run_id, job_id)


def export(pg_uri: str, os_run_id: int, gpkg_filename: str):
    with connect(pg_uri, cursor_factory=DictCursor) as pg_conn:
        job_ids: List[int] = _get_jobs_to_export(pg_conn, os_run_id)
        if not job_ids:
            raise ValueError(f"Run {os_run_id} has no successful job runs")
        complete: bool = _is_all_complete(pg_conn, os_run_id)
        if not complete:
            logging.warning(f"Not all jobs in run {os_run_id} are complete")

        gpkg_fname_stem, gpkg_fname_extn = os.path.splitext(gpkg_filename)

        # Run threads that start sub-processes to do the extracts
        executor = ThreadPoolExecutor(max_workers=os.cpu_count())
        futures: List[Tuple[int, Future]] = []
        for job_id in job_ids:
            future = executor.submit(_export, pg_conn, pg_uri,
                                     f"{gpkg_fname_stem}.{job_id}{gpkg_fname_extn}", os_run_id, job_id)
            futures.append((job_id, future))

        exc_str: str = ""
        for job_id, future in futures:
            try:
                future.result()
            except Exception as ex:
                if exc_str:
                    exc_str += "\n"
                exc_str += f"{job_id} raised '{str(ex)}'"
        if exc_str:
            raise Exception(f"Exception(s) raised\n{exc_str}")


if __name__ == "__main__":
    export("postgresql://albion_webapp:ydBbE3JCnJ4@localhost:5432/albion",
           14,
           "/tmp/test.gpkg")