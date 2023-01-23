import logging
import os
from asyncio import Future
from concurrent.futures import ThreadPoolExecutor
from os.path import join
from threading import current_thread
from typing import List, Tuple, Optional

from psycopg2.extras import DictCursor

from albion_models.db_funcs import sql_command, connection, get_max_connections
from albion_models.ogr_helpers import get_layer_names
from albion_models.solar_pv.open_solar import export_panelarray, export_building, export_geographies, \
    export_conservation_area, export_paf
from albion_models.solar_pv.open_solar.export_building import L_BUILDINGS
from albion_models.solar_pv.open_solar.export_panelarray import L_PANELS, L_INSTALLATIONS

_JOB_GPKG_STEM = "job"
_BASE_GPKG_STEM = "base_info"
_GPKG_FNAME_EXTN = ".gpkg"


def _get_jobs_to_export(pg_conn, os_run_id: int, start_job_id: Optional[int], end_job_id: Optional[int]) -> List[int]:
    sql_cmd: str = (
        "SELECT job_id "
        "FROM models.job_queue q "
        "JOIN models.open_solar_jobs osj USING (job_id) "
        "WHERE osj.os_run_id = %(os_run_id)s "
        "AND q.status = 'COMPLETE'::models.job_status "
    )
    bindings = {
        "os_run_id": os_run_id
    }
    if start_job_id is not None:
        sql_cmd += "AND job_id >= %(start_job_id)s "
        bindings["start_job_id"] = start_job_id
    if end_job_id is not None:
        sql_cmd += "AND job_id <= %(end_job_id)s "
        bindings["end_job_id"] = end_job_id

    job_ids = sql_command(pg_conn,
                          sql_cmd, bindings,
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


def _export_job(pg_uri: str, gpkg_dir: str, os_run_id: int, job_id: int, regenerate: bool):
    """Job info goes into one gpkg in multiple layers
    """
    thread = current_thread()
    thread.name = f"job_id_{job_id}"

    gpkg_fname: str = join(gpkg_dir, f"{_JOB_GPKG_STEM}.{job_id}{_GPKG_FNAME_EXTN}")
    gpkg_fname_exporting: str = join(gpkg_dir, f"exp.{_JOB_GPKG_STEM}.{job_id}{_GPKG_FNAME_EXTN}")

    if not regenerate:
        layer_names: List[str] = get_layer_names(gpkg_fname)
        if L_PANELS not in layer_names or L_INSTALLATIONS not in layer_names or L_BUILDINGS not in layer_names:
            regenerate = True

    if regenerate:
        logging.info(f"Exporting job {job_id}")
        if os.path.isfile(gpkg_fname):
            os.remove(gpkg_fname)
        if os.path.isfile(gpkg_fname_exporting):  # Will be present if export failed last time
            os.remove(gpkg_fname_exporting)
        try:
            with connection(pg_uri, cursor_factory=DictCursor) as pg_conn:  # Use a separate connection per call / thread
                export_panelarray.export(pg_conn, pg_uri, gpkg_fname_exporting, os_run_id, job_id)
                export_building.export(pg_conn, pg_uri, gpkg_fname_exporting, os_run_id, job_id)
            os.rename(gpkg_fname_exporting, gpkg_fname)  # Rename if export succeeded, leave with "exp." prefix if fails
            logging.info(f"Renamed {gpkg_fname_exporting} to {gpkg_fname}")
        except Exception as e:
            logging.exception(f"Exporting job {job_id}")
            raise e


def _export_base_lsoa(pg_uri: str, gpkg_dir: str, regenerate: bool):
    """ Base info goes into multiple gpkgs due to gpkg db full issues using overwrite
    and so that extracts can be run concurrently """
    logging.info(f"Exporting base LSOA info")
    gpkg_filename: str = join(gpkg_dir, f"{_BASE_GPKG_STEM}.lsoa{_GPKG_FNAME_EXTN}")
    with connection(pg_uri, cursor_factory=DictCursor) as pg_conn:  # Use a separate connection per call / thread
        export_geographies.export_lsoa(pg_conn, pg_uri, gpkg_filename, regenerate)


def _export_base_la(pg_uri: str, gpkg_dir: str, regenerate: bool):
    """ Base info goes into multiple gpkgs due to gpkg db full issues using overwrite
    and so that extracts can be run concurrently """
    logging.info(f"Exporting base LA info")
    gpkg_filename: str = join(gpkg_dir, f"{_BASE_GPKG_STEM}.la{_GPKG_FNAME_EXTN}")
    with connection(pg_uri, cursor_factory=DictCursor) as pg_conn:  # Use a separate connection per call / thread
        export_geographies.export_la(pg_conn, pg_uri, gpkg_filename, regenerate)


def _export_base_msoa(pg_uri: str, gpkg_dir: str, regenerate: bool):
    """ Base info goes into multiple gpkgs due to gpkg db full issues using overwrite
    and so that extracts can be run concurrently """
    logging.info(f"Exporting base MSOA info")
    gpkg_filename: str = join(gpkg_dir, f"{_BASE_GPKG_STEM}.msoa{_GPKG_FNAME_EXTN}")
    with connection(pg_uri, cursor_factory=DictCursor) as pg_conn:  # Use a separate connection per call / thread
        export_geographies.export_msoa(pg_conn, pg_uri, gpkg_filename, regenerate)


def _export_base_parish(pg_uri: str, gpkg_dir: str, regenerate: bool):
    """ Base info goes into multiple gpkgs due to gpkg db full issues using overwrite
    and so that extracts can be run concurrently """
    logging.info(f"Exporting base parish info")
    gpkg_filename: str = join(gpkg_dir, f"{_BASE_GPKG_STEM}.parish{_GPKG_FNAME_EXTN}")
    with connection(pg_uri, cursor_factory=DictCursor) as pg_conn:  # Use a separate connection per call / thread
        export_geographies.export_parish(pg_conn, pg_uri, gpkg_filename, regenerate)


def _export_base_cons_area(pg_uri: str, gpkg_dir: str, regenerate: bool):
    """ Base info goes into multiple gpkgs due to gpkg db full issues using overwrite
    and so that extracts can be run concurrently """
    logging.info(f"Exporting base conservation area info")
    gpkg_filename: str = join(gpkg_dir, f"{_BASE_GPKG_STEM}.conservation_areas{_GPKG_FNAME_EXTN}")
    with connection(pg_uri, cursor_factory=DictCursor) as pg_conn:  # Use a separate connection per call / thread
        export_conservation_area.export(pg_conn, pg_uri, gpkg_filename, regenerate)


def _export_paf(pg_uri: str, output_dir: str, regenerate: bool):
    """Export full PAF DB"""
    logging.info(f"Exporting PAF")
    output_filename: str = join(output_dir, f"paf.csv.gz")
    with connection(pg_uri, cursor_factory=DictCursor) as pg_conn:  # Use a separate connection per call / thread
        export_paf.export(pg_conn, output_filename, regenerate)


def export(pg_uri: str, os_run_id: int, gpkg_dir: str,
           extract_job_info: bool, extract_base_info: bool,
           start_job_id: Optional[int], end_job_id: Optional[int],
           regenerate: bool):
    if not extract_job_info and not extract_base_info:
        extract_job_info = True
        extract_base_info = True

    if extract_job_info and os_run_id is None:
        extract_job_info = False
        logging.warning("No run id specified so not exporting job information")

    if gpkg_dir is None:
        gpkg_dir = "."

    with connection(pg_uri, cursor_factory=DictCursor) as pg_conn:
        # Using 0.25 here as 0.75 gives errors on bats
        mw = int(min(0.25 * os.cpu_count(), 0.25 * get_max_connections(pg_conn)))
        executor = ThreadPoolExecutor(max_workers=mw)
        futures: List[Tuple[Optional[int], Future]] = []

        # Run threads that start sub-processes to do the extracts
        if extract_job_info:
            job_ids = _get_jobs_to_export(pg_conn, os_run_id, start_job_id, end_job_id)
            if not job_ids:
                raise ValueError(f"Run {os_run_id} has no in-range successful job runs")
            if not _is_all_complete(pg_conn, os_run_id):
                logging.warning(f"Not all jobs in run {os_run_id} are complete")

            for job_id in job_ids:
                futures.append((job_id, executor.submit(_export_job, pg_uri, gpkg_dir, os_run_id, job_id, regenerate)))

        if extract_base_info:
            futures.append((None, executor.submit(_export_base_lsoa, pg_uri, gpkg_dir, regenerate)))
            futures.append((None, executor.submit(_export_base_la, pg_uri, gpkg_dir, regenerate)))
            futures.append((None, executor.submit(_export_base_cons_area, pg_uri, gpkg_dir, regenerate)))
            futures.append((None, executor.submit(_export_paf, pg_uri, gpkg_dir, regenerate)))
            futures.append((None, executor.submit(_export_base_msoa, pg_uri, gpkg_dir, regenerate)))
            futures.append((None, executor.submit(_export_base_parish, pg_uri, gpkg_dir, regenerate)))

        exc_str: str = ""
        for _id, future in futures:
            try:
                future.result()
            except Exception as ex:
                if exc_str:
                    exc_str += "\n"
                exc_str += f"Export of {f'job {_id}' if _id is not None else 'base info'} raised '{str(ex)}'"
        if exc_str:
            raise Exception(f"Exception(s) raised\n{exc_str}")
