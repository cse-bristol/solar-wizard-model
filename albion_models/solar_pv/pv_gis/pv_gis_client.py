import logging
from os.path import join
import csv
import time
import traceback
from typing import List, Dict, Iterable, Optional
import multiprocessing as mp

import requests
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


def pv_gis(pg_uri: str, job_id: int, peak_power_per_m2: float, pv_tech: str, roof_area_percent_usable: int, solar_dir: str):
    solar_pv_csv = join(solar_dir, 'solar_pv.csv')
    pg_conn = connect(pg_uri, cursor_factory=psycopg2.extras.DictCursor)
    try:
        with pg_conn.cursor() as cursor:
            cursor.execute(SQL("SELECT * FROM {roof_horizons} WHERE usable = true").format(
                roof_horizons=Identifier(tables.schema(job_id), tables.ROOF_HORIZON_TABLE))
            )
            rows = cursor.fetchall()
            pg_conn.commit()
            logging.info(f"{len(rows)} queries to send:")
            _solar_pv_estimate(rows, peak_power_per_m2, pv_tech, roof_area_percent_usable, solar_pv_csv)
    finally:
        pg_conn.close()

    _write_results_to_db(pg_uri, job_id, solar_pv_csv)


def _write_results_to_db(pg_uri: str, job_id: int, csv_file: str):
    pg_conn = connect(pg_uri)
    try:
        sql_script(pg_conn, 'create.solar-pv.sql', solar_pv=Identifier(tables.schema(job_id), tables.SOLAR_PV_TABLE))
        copy_csv(pg_conn, csv_file, f'{tables.schema(job_id)}.{tables.SOLAR_PV_TABLE}')
        sql_script_with_bindings(
            pg_conn, 'post-load.solar-pv.sql', {"job_id": job_id},
            solar_pv=Identifier(tables.schema(job_id), tables.SOLAR_PV_TABLE),
            roof_horizons=Identifier(tables.schema(job_id), tables.ROOF_HORIZON_TABLE),
            job_view=Identifier(f"solar_pv_job_{job_id}")
        )
    finally:
        pg_conn.close()


def _solar_pv_estimate(iterable: Iterable[dict],
                       peak_power_per_m2: float,
                       pv_tech: str,
                       roof_area_percent_usable: int,
                       out_filename: str,
                       log_frequency: int = 250):
    with open(out_filename, 'w') as out, mp.Pool(_WORKERS) as pool:
        csv_writer = None
        processed: int = 0
        errors: int = 0

        wrapped_iterable = (dict(row,
                                 peak_power_per_m2=peak_power_per_m2,
                                 pv_tech=pv_tech,
                                 roof_area_percent_usable=roof_area_percent_usable) for row in iterable)
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


# PV-GIS API params, from https://ec.europa.eu/jrc/en/PVGIS/docs/noninteractive
# More detail on them is in the user manual https://ec.europa.eu/jrc/en/PVGIS/docs/usermanual
# Name 	                Type* 	Obligatory 	Default 	    Comments
# lat           	    F 	    Yes 	    - 	            Latitude, in decimal degrees, south is negative.
# lon 	                F 	    Yes 	    -       	    Longitude, in decimal degrees, west is negative.
# usehorizon 	        I 	    No 	        1 	            Calculate taking into account shadows from high horizon. Value of 1 for "yes".
# userhorizon 	        L 	    No 	        - 	            Height of the horizon at equidistant directions around the point of interest, in degrees.
#                                                           Starting at north and moving clockwise. The series '0,10,20,30,40,15,25,5' would mean the
#                                                           horizon height is 0° due north, 10° for north-east, 20° for east, 30° for south-east, etc.
# raddatabase 	        T 	    No          'PVGIS-SARAH'   Name of the radiation database. "PVGIS-SARAH" for Europe, Africa and Asia or "PVGIS-NSRDB"
#                                                           for the Americas between 60°N and 20°S,  "PVGIS-ERA5" and "PVGIS-COSMO" for Europe (including
#                                                           high-latitudes), and "PVGIS-CMSAF" for Europe and Africa (will be deprecated)
# peakpower 	        F 	    Yes 	    - 	            Nominal power of the PV system, in kW.
# pvtechchoice 	        T 	    No      	"crystSi" 	    PV technology. Choices are: "crystSi", "CIS", "CdTe" and "Unknown".
# mountingplace 	    T 	    No      	"free"      	Type of mounting of the PV modules. Choices are: "free" for free-standing and "building" for building-integrated.
# loss 	                F 	    Yes     	- 	            Sum of system losses, in percent.
# fixed          	    I 	    No      	1 	            Calculate a fixed mounted system. Value of 0 for "no". All other values (or no value)
#                                                           mean "Yes". Note that this means the default is "yes".
# angle         	    F 	    No 	        0 	            Inclination angle from horizontal plane of the (fixed) PV system.
# aspect 	            F 	    No 	        0 	            Orientation (azimuth) angle of the (fixed) PV system, 0=south, 90=west, -90=east.
# optimalinclination 	I 	    No 	        0 	            Calculate the optimum inclination angle. Value of 1 for "yes". All other values (or no value) mean "no".
# optimalangles 	    I 	    No 	        0 	            Calculate the optimum inclination AND orientation angles. Value of 1 for "yes". All other
#                                                           values (or no value) mean "no".
# inclined_axis 	    I 	    No 	        0 	            Calculate a single inclined axis system. Value of 1 for "yes". All other values (or no value) mean "no".
# inclined_optimum 	    I 	    No 	        0 	            Calculate optimum angle for a single inclined axis system. Value of 1 for "yes". All other values
#                                                           (or no value) mean "no".
# inclinedaxisangle 	F 	    No 	        0 	            Inclination angle for a single inclined axis system. Ignored if the optimum angle should be
#                                                           calculated (parameter "inclined_optimum").
# vertical_axis 	    I 	    No 	        0 	            Calculate a single vertical axis system. Value of 1 for "yes". All other values (or no value) mean "no".
# vertical_optimum 	    I 	    No 	        0 	            Calculate optimum angle for a single vertical axis system. Value of 1 for "yes". All other values
#                                                           (or no value) mean "no".
# verticalaxisangle 	F 	    No 	        0 	            Inclination angle for a single vertical axis system. Ignored if the optimum angle should be
#                                                           calculated (parameter "vertical_optimum" set to 1).
# twoaxis 	            I 	    No 	        0 	            Calculate a two axis tracking system. Value of 1 for "yes". All other values (or no value) mean "no".
# pvprice 	            I 	    No 	        0 	            Calculate the PV electricity price [kwh/year] in the currency introduced by the user for the system cost.
# systemcost 	        F 	    if pvprice 	- 	            Total cost of installing the PV system [your currency].
# interest 	            F 	    if pvprice 	- 	            Interest in %/year
# lifetime 	            I 	    No 	        25 	            Expected lifetime of the PV system in years.
# outputformat 	        T 	    No 	        "csv" 	        Type of output. Choices are: "csv" for the normal csv output with text explanations,
#                                                           "basic" to get only the data output with no text, and "json".
# browser 	            I 	    No 	        0 	            Use this with a value of "1" if you access the web service from a web browser and want to save the data to a file.
#
# * F = Floating point number; I = integer number; L = list of numbers; T= text string.


def _single_solar_pv_estimate(lon: float,
                              lat: float,
                              horizon: List[float],
                              angle: float,
                              aspect: float,
                              peakpower: float,
                              loss: float,
                              pvtechchoice: str) -> Optional[dict]:
    url = 'https://re.jrc.ec.europa.eu/api/PVcalc'
    res = requests.get(url, params={
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


def _handle_row(row: Dict[str, str]) -> Optional[dict]:
    try:
        lon, lat, horizon, angle, aspect, peakpower, loss, pv_tech = _row_to_pv_gis_params(row)

        start_time = time.time()
        response = _single_solar_pv_estimate(lon, lat, horizon, angle, aspect, peakpower, loss, pv_tech)
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


def _row_to_pv_gis_params(row: dict) -> tuple:
    lon, lat = _easting_northing_to_lon_lat(row['easting'], row['northing'])

    # SAGA and PV-GIS both expect starting at North, moving clockwise
    horizon = [_rad_to_deg(v) for k, v in row.items() if 'horizon_slice' in k]

    # angle: in degrees from horizontal
    # corresponds to slope field in patched SAGA csv output (in rads from horizontal)
    angle = row['slope']

    # aspect: in degrees clockwise from south
    # aspect field in patched SAGA csv output: in rads clockwise from north
    aspect = row['aspect'] - 180.0

    roof_area_percent_usable = int(row['roof_area_percent_usable']) / 100
    area = float(row['area'])
    area *= roof_area_percent_usable
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
