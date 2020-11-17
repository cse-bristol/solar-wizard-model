import csv
import time
import traceback
from typing import List, Dict
import multiprocessing as mp

import requests

from flatten import flatten

_PI = 3.14159265359
_API_RATE_LIMIT_SECONDS = 1 / 25
_WORKERS = 4
_API_RATE_LIMIT_SECONDS_PER_WORKER = _API_RATE_LIMIT_SECONDS * _WORKERS

# PV-GIS API params, from https://ec.europa.eu/jrc/en/PVGIS/docs/noninteractive
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
                              pvtechchoice: str = 'crystSi'):
    url = 'https://re.jrc.ec.europa.eu/api/PVcalc'
    res = requests.get(url, params={
        "outputformat":  "json",
        "browser": 0,
        "userhorizon": ','.join([str(i) for i in horizon]),
        "lon": lon,
        "lat": lat,
        "peakpower": peakpower,
        "mountingplace": "building",
        "loss": loss,
        "angle": angle,
        "aspect": aspect,
        "pvtechchoice": pvtechchoice
    })
    res.raise_for_status()
    body = res.json()
    return body


def _handle_row(row: Dict[str, str]):
    try:
        lon, lat, horizon, angle, aspect, peakpower, loss = _csv_row_to_pv_gis_params(row)

        start_time = time.time()
        results = _single_solar_pv_estimate(lon, lat, horizon, angle, aspect, peakpower, loss)
        time_taken = time.time() - start_time
        print(time_taken)
        # Stay under the API rate limit:
        if time_taken < _API_RATE_LIMIT_SECONDS_PER_WORKER:
            time.sleep(_API_RATE_LIMIT_SECONDS_PER_WORKER - time_taken)

        results = flatten(results['outputs'])
        results.update({
            'easting': row['easting'],
            'northing': row['northing'],
            'x': row['x'],
            'y': row['y'],
        })
        return results
    except Exception as e:
        print('Caught exception in worker process:')
        traceback.print_exc()
        print()
        raise e


def solar_pv_estimate(csv_filename: str, out_filename: str):
    with open(csv_filename) as f, open(out_filename, 'w') as out, mp.Pool(_WORKERS) as pool:
        csv_reader = csv.DictReader(f)
        csv_writer = None

        # todo remove
        start_time = time.time()
        i = 0
        for res in pool.imap_unordered(_handle_row, csv_reader, chunksize=10):
            if csv_writer is None:
                csv_writer = csv.DictWriter(out, res.keys())
                csv_writer.writeheader()
            csv_writer.writerow(res)
            # todo remove
            i += 1
            if i == 100:
                time_taken = time.time() - start_time
                print(f"{time_taken} seconds to do 100")
                exit(0)


def _csv_row_to_pv_gis_params(row: Dict[str, str]) -> tuple:
    lon, lat = _easting_northing_to_lon_lat(row['easting'], row['northing'])

    # SAGA and PV-GIS both expect starting at North, moving clockwise
    horizon = [_rad_to_deg(v) for k, v in row.items() if 'angle_rad' in k]

    # angle: in degrees from horizontal
    # corresponds to slope field in patched SAGA csv output (in rads from horizontal)
    angle = _rad_to_deg(row['slope'])

    # aspect: in degrees clockwise from south
    # corresponds to aspect field in patched SAGA csv output (in rads clockwise from north)
    aspect = _rad_to_deg(row['aspect']) - 180.0

    peakpower = 1
    loss = 14

    return lon, lat, horizon, angle, aspect, peakpower, loss


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


if __name__ == '__main__':
    # solar_pv_estimate('../data/csv_out4.csv', '../data/res.csv')
    res = _handle_row({
        'x': '2956',
        'y': '1',
        'easting': '374474.973649',
        'northing': '161297.831967',
        'slope': '1.055491',
        'aspect': '3.275080',
        'sky_view_factor': '0.719803',
        'percent_visible': '63.753561',
        '0_angle_rad': '1.290979',
        '45_angle_rad': '1.293249',
        '90_angle_rad': '0.542866',
        '135_angle_rad': '0.000000',
        '180_angle_rad': '0.000000',
        '225_angle_rad': '0.000000',
        '270_angle_rad': '0.139095',
        '315_angle_rad': '1.28867',
    })
    print(res)
