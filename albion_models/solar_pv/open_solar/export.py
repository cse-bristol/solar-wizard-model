import logging
import os
from asyncio import Future
from concurrent.futures import ThreadPoolExecutor
from os.path import join
from typing import List, Tuple, Optional

from psycopg2.extras import DictCursor

from albion_models.db_funcs import sql_command, connect
from albion_models.solar_pv.open_solar import export_panelarray, export_building, export_lsoa, export_la

_JOB_GPKG_STEM = "job"
_META_GPKG_STEM = "meta"
_GPKG_FNAME_EXTN = ".gpkg"


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


def _export_job(pg_uri: str, gpkg_filename: str, os_run_id: int, job_id: int):
    logging.info(f"Exporting job {job_id}")
    with connect(pg_uri, cursor_factory=DictCursor) as pg_conn:  # Use a separate connection per call / thread
        export_panelarray.export(pg_conn, pg_uri, gpkg_filename, os_run_id, job_id)
        export_building.export(pg_conn, pg_uri, gpkg_filename, os_run_id, job_id)


def _export_meta(pg_uri: str, gpkg_filename: str):
    logging.info(f"Exporting meta info")

    # Attempting to overwrite existing causes database full error - so delete and start again each time
    if os.path.exists(gpkg_filename):
        os.remove(gpkg_filename)

    with connect(pg_uri, cursor_factory=DictCursor) as pg_conn:  # Use a separate connection per call / thread
        #export_lsoa.export(pg_conn, pg_uri, gpkg_filename)
        export_la.export(pg_conn, pg_uri, gpkg_filename)


def export(pg_uri: str, os_run_id: int, gpkg_dir: str):
    with connect(pg_uri, cursor_factory=DictCursor) as pg_conn:
        job_ids: List[int] = _get_jobs_to_export(pg_conn, os_run_id)
        if not job_ids:
            raise ValueError(f"Run {os_run_id} has no successful job runs")
        complete: bool = _is_all_complete(pg_conn, os_run_id)
        if not complete:
            logging.warning(f"Not all jobs in run {os_run_id} are complete")

        # Do serially
        # for job_id in job_ids:
        #     _export(pg_uri, join(gpkg_dir, f"{_JOB_GPKG_STEM}.{job_id}{_GPKG_FNAME_EXTN}"), os_run_id, job_id)

        # Run threads that start sub-processes to do the extracts
        executor = ThreadPoolExecutor(max_workers=os.cpu_count())
        futures: List[Tuple[Optional[int], Future]] = []
        # for job_id in job_ids:
        #     future = executor.submit(_export_job, pg_uri,
        #                              join(gpkg_dir, f"{_JOB_GPKG_STEM}.{job_id}{_GPKG_FNAME_EXTN}"), os_run_id, job_id)
        #     futures.append((job_id, future))

        future = executor.submit(_export_meta, pg_uri,
                                 join(gpkg_dir, f"{_META_GPKG_STEM}{_GPKG_FNAME_EXTN}"))
        futures.append((None, future))

        exc_str: str = ""
        for _id, future in futures:
            try:
                future.result()
            except Exception as ex:
                if exc_str:
                    exc_str += "\n"
                exc_str += f"Export of {f'job {_id}' if _id is not None else 'meta info'} raised '{str(ex)}'"
        if exc_str:
            raise Exception(f"Exception(s) raised\n{exc_str}")


if __name__ == "__main__":
    logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)
    export("postgresql://albion_ddl:albion320@localhost:5432/albion",
           22,
           "/tmp")
