import logging
import os
from os.path import join
import csv
import time
import traceback
from typing import List, Dict, Iterable, Optional
import multiprocessing as mp

import requests
from requests.adapters import HTTPAdapter
import psycopg2.extras
from psycopg2.sql import SQL, Identifier

from albion_models.solar_pv.pv_gis.flatten import flatten
import albion_models.solar_pv.tables as tables
from albion_models.db_funcs import sql_script, connect, copy_csv, sql_script_with_bindings


_PI = 3.14159265359
_API_RATE_LIMIT_SECONDS = 1 / 25
_WORKERS = 4
_API_RATE_LIMIT_SECONDS_PER_WORKER = _API_RATE_LIMIT_SECONDS * _WORKERS
_ALLOWED_ERRORS = ("Location over the sea. Please, select another location",)

_session: requests.Session


def pv_gis(pg_uri: str, job_id: int, peak_power_per_m2: float, pv_tech: str, solar_dir: str):
    """
    Module entrypoint: run each roof plane through PVGIS.
    """
    solar_pv_csv = join(solar_dir, 'solar_pv.csv')
    pg_conn = connect(pg_uri, cursor_factory=psycopg2.extras.DictCursor)
    try:
        with pg_conn.cursor() as cursor:
            cursor.execute(SQL("SELECT * FROM {panel_horizons} WHERE usable = true").format(
                panel_horizons=Identifier(tables.schema(job_id), tables.PANEL_HORIZON_TABLE))
            )
            roof_planes = cursor.fetchall()
            pg_conn.commit()

        logging.info(f"{len(roof_planes)} queries to send:")
        _estimate_solar_pv(roof_planes, peak_power_per_m2, pv_tech, solar_pv_csv)
        _write_results_to_db(pg_conn, job_id, solar_pv_csv)
        os.remove(solar_pv_csv)
    finally:
        pg_conn.close()


def _write_results_to_db(pg_conn, job_id: int, csv_file: str):
    sql_script(
        pg_conn,
        'pv/create.solar-pv.sql',
        solar_pv=Identifier(tables.schema(job_id), tables.SOLAR_PV_TABLE))

    copy_csv(pg_conn, csv_file, f'{tables.schema(job_id)}.{tables.SOLAR_PV_TABLE}')

    sql_script_with_bindings(
        pg_conn,
        'pv/post-load.solar-pv.sql',
        {"job_id": job_id},
        solar_pv=Identifier(tables.schema(job_id), tables.SOLAR_PV_TABLE),
        panel_horizons=Identifier(tables.schema(job_id), tables.PANEL_HORIZON_TABLE),
        job_view=Identifier(f"solar_pv_job_{job_id}"))


def init_process():
    """
    Function passed to the multiprocessing pool as the `initializer` arg, that will
    be run on startup by each subprocess.
    """
    # Each process will have it's own version of this global value:
    global _session
    _session = requests.Session()
    # Retry on things like socket, timeout, connection errors, not HTTP error codes:
    adapter = HTTPAdapter(max_retries=5)
    _session.mount('http://', adapter)
    _session.mount('https://', adapter)


def _estimate_solar_pv(roof_planes: Iterable[dict],
                       peak_power_per_m2: float,
                       pv_tech: str,
                       out_filename: str,
                       log_frequency: int = 250):
    """
    Estimate the solar PV output for an Iterable of rows from the `panel_horizons`
    database table.

    This is performed as a parallel operation and results are written to a CSV file.
    """
    with open(out_filename, 'w') as out, \
            mp.Pool(_WORKERS, initializer=init_process) as pool:
        csv_writer = None
        processed: int = 0
        errors: int = 0

        wrapped_iterable = (dict(row,
                                 peak_power_per_m2=peak_power_per_m2,
                                 pv_tech=pv_tech) for row in roof_planes)
        for res in pool.imap_unordered(_handle_row, wrapped_iterable, chunksize=10):
            if res is not None:
                if csv_writer is None:
                    csv_writer = csv.DictWriter(out, res.keys())
                    csv_writer.writeheader()
                csv_writer.writerow(res)
            else:
                errors += 1
            processed += 1
            if processed % log_frequency == 0 and log_frequency > 0 and processed > 0:
                print(f"Sent {processed} queries.")

        print(f"Total allowed PV-GIS errors: {errors}")


def _handle_row(row: Dict[str, str]) -> Optional[dict]:
    """
    For a given row from the `panel_horizons` table, query PVGIS and shape
    the outputs into a useful object, unless there's been an error, in which
    case return None.
    """
    try:
        lon, lat, horizon, angle, aspect, peakpower, loss, pv_tech = _row_to_pv_gis_params(row)

        start_time = time.time()
        response = _get_pvgis(lon, lat, horizon, angle, aspect, peakpower, loss, pv_tech)
        if response is None:
            return None

        time_taken = time.time() - start_time
        # Stay under the API rate limit:
        if time_taken < _API_RATE_LIMIT_SECONDS_PER_WORKER:
            time.sleep(_API_RATE_LIMIT_SECONDS_PER_WORKER - time_taken)

        results = flatten(response['outputs'])
        results.update({
            'easting': row['easting'],
            'northing': row['northing'],
            'toid': row['toid'],
            'roof_plane_id': row['roof_plane_id'],
            'peak_power': response['inputs']['pv_module']['peak_power'],
            'horizon_sd': row['horizon_sd'],
            'southerly_horizon_sd': row['southerly_horizon_sd'],
        })
        return results
    except Exception as e:
        print('Caught exception in worker process:')
        traceback.print_exc()
        print()
        raise e


def _get_pvgis(lon: float,
               lat: float,
               horizon: List[float],
               angle: float,
               aspect: float,
               peakpower: float,
               loss: float,
               pvtechchoice: str) -> Optional[dict]:
    """
    Make a single request to PVGIS.
    * PV-GIS API params are here: https://ec.europa.eu/jrc/en/PVGIS/docs/noninteractive
    * More detail on them in the user manual: https://ec.europa.eu/jrc/en/PVGIS/docs/usermanual
    """
    global _session
    url = 'https://re.jrc.ec.europa.eu/api/PVcalc'
    res = _session.get(url, params={
        "outputformat":  "json",
        "browser": 0,
        "userhorizon": ','.join([str(i) for i in horizon]),
        "lon": lon,
        "lat": lat,
        "peakpower": peakpower,
        "mountingplace": "free",
        "loss": loss,
        "angle": angle,
        "aspect": aspect,
        "pvtechchoice": pvtechchoice
    })

    if res.status_code == 400:
        error = res.json()
        error_message = error['message']
        print(error_message)
        if error['message'] in _ALLOWED_ERRORS:
            return None
        else:
            res.raise_for_status()
    else:
        res.raise_for_status()

    body = res.json()
    return body


def _row_to_pv_gis_params(row: dict) -> tuple:
    """
    Convert a `panel_horizons` row into PVGIS API params.
    """
    lon, lat = _easting_northing_to_lon_lat(row['easting'], row['northing'])

    # SAGA and PV-GIS both expect starting at North, moving clockwise
    horizon = [_rad_to_deg(v) for k, v in row.items() if 'horizon_slice' in k]

    # angle: in degrees from horizontal
    # corresponds to slope field in patched SAGA csv output (in degrees from horizontal)
    angle = row['slope']

    # aspect: in degrees clockwise from south
    # aspect field in patched SAGA csv output: in degrees clockwise from north
    aspect = row['aspect'] - 180.0

    area = float(row['area'])
    peakpower = float(row['peak_power_per_m2']) * area

    loss = 14
    pv_tech = row['pv_tech']

    return lon, lat, horizon, angle, aspect, peakpower, loss, pv_tech


def _rad_to_deg(rad):
    return float(rad) * 180 / _PI


def _easting_northing_to_lon_lat(easting, northing):
    from osgeo import ogr
    from osgeo import osr

    InSR = osr.SpatialReference()
    InSR.ImportFromEPSG(27700)  # Easting/Northing SRS
    OutSR = osr.SpatialReference()
    OutSR.ImportFromEPSG(4326)  # lon/lat SRS

    Point = ogr.Geometry(ogr.wkbPoint)
    Point.AddPoint(float(easting), float(northing))
    Point.AssignSpatialReference(InSR)
    Point.TransformTo(OutSR)
    return Point.GetY(), Point.GetX()


# if __name__ == '__main__':
#     res = _handle_row({
#         'x': '2956',
#         'y': '1',
#         'easting': '374474.973649',
#         'northing': '161297.831967',
#         'slope': '1.055491',
#         'aspect': '3.275080',
#         'sky_view_factor': '0.719803',
#         'percent_visible': '63.753561',
#         'horizon_slice_0': '1.290979',
#         'horizon_slice_45': '1.293249',
#         'horizon_slice_90': '0.542866',
#         'horizon_slice_135': '0.000000',
#         'horizon_slice_180': '0.000000',
#         'horizon_slice_225': '0.000000',
#         'horizon_slice_270': '0.139095',
#         'horizon_slice_315': '1.28867',
#         'area': '15',
#         'peak_power_per_m2': '0.120',
#         'pv_tech': 'crystSi',
#
#     })
#     print(res)
