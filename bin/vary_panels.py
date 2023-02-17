"""
This is a script for exploring what changes in the kWh output of a PV installation
as you move it around in lat/long.
"""
import csv
from statistics import stdev
from typing import List

import requests

from solar_model.solar_pv.pvgis_old.flatten import flatten


def pv_gis(lon: float, lat: float, peakpower: float, slope: float, aspect: float, horizon: List[float]):
    params = {
        "outputformat": "json",
        "browser": 0,
        "userhorizon": ','.join([str(i) for i in horizon]) if horizon else None,
        "lon": lon,
        "lat": lat,
        "peakpower": peakpower,
        "mountingplace": "free",
        "loss": 14,
        "angle": slope,
        "aspect": aspect,
        "pvtechchoice": 'crystSi'
    }
    if params['userhorizon'] is None:
        del params['userhorizon']

    url = 'https://re.jrc.ec.europa.eu/api/PVcalc'
    res = requests.get(url, params=params)
    res.raise_for_status()
    body = res.json()
    return flatten(body['outputs'])


def _rad_to_deg(rad):
    return float(rad) * 180 / 3.14159265359


def check(filename: str, pv_gis_params: List[dict]):
    header = ['N', 'NNE', 'NE', 'ENE', 'E', 'ESE', 'SE', 'SSE', 'S', 'SSW', 'SW', 'WSW',
              'W', 'WNW', 'NW', 'NNW', 'kWh', 'kWh_sd', 'kWp', 'lat', 'lon', 'slope',
              'aspect']
    row_format = "{:<7}" * len(header)
    print(row_format.format(*header))

    all_results = []
    for params in pv_gis_params:
        pg_res = pv_gis(**params)
        horizon = params['horizon']
        kwh = float(pg_res[f'totals_fixed_E_y'])
        kwh_sd = float(pg_res[f'totals_fixed_SD_y'])
        res = {
            'N': round(horizon[0], 2),
            'NNE': round(horizon[1], 2),
            'NE': round(horizon[2], 2),
            'ENE': round(horizon[3], 2),
            'E': round(horizon[4], 2),
            'ESE': round(horizon[5], 2),
            'SE': round(horizon[6], 2),
            'SSE': round(horizon[7], 2),
            'S': round(horizon[8], 2),
            'SSW': round(horizon[9], 2),
            'SW': round(horizon[10], 2),
            'WSW': round(horizon[11], 2),
            'W': round(horizon[12], 2),
            'WNW': round(horizon[13], 2),
            'NW': round(horizon[14], 2),
            'NNW': round(horizon[15], 2),
            'kWh': round(kwh, 1),
            'kWh_sd': round(kwh_sd, 1),
            'kWp': params['peakpower'],
            'lat': round(params['lat'], 3),
            'lon': round(params['lon'], 3),
            'slope': round(params['slope'], 2),
            'aspect': round(params['aspect'], 2),
        }
        print(row_format.format(*res.values()))
        all_results.append(res)

    sd = stdev([r['kWh'] for r in all_results])
    print(f"kWh standard deviation: {sd}")
    with open(filename, "w") as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow(all_results[0].keys())
        for row in all_results:
            csv_writer.writerow(row.values())


def gen_inputs(base_lat: float, base_lon: float) -> List[dict]:
    base = {
        "lat": base_lat,
        "lon": base_lon,
        "peakpower": 0.2,
        "slope": _rad_to_deg(0.3251753474088293),
        "aspect": _rad_to_deg(2.1694196314779273) - 180.0,
        "horizon": [
            _rad_to_deg(1.343677),
            _rad_to_deg(1.343677),
            _rad_to_deg(0.693951),
            _rad_to_deg(0.182241),
            _rad_to_deg(0.31027),
            _rad_to_deg(0.261485),
            _rad_to_deg(0.341194),
            _rad_to_deg(0.262712),
            _rad_to_deg(0.167464),
            _rad_to_deg(0.393371),
            _rad_to_deg(1.303246),
            _rad_to_deg(1.391326),
            _rad_to_deg(1.391326),
            _rad_to_deg(1.391326),
            _rad_to_deg(1.395832),
            _rad_to_deg(1.343677),
        ]
    }

    inputs = []
    for lat_diff in [-0.005, -0.003, -0.001, 0, 0.001, 0.003, 0.005]:
        for lon_diff in [-0.005, -0.003, -0.001, 0, 0.001, 0.003, 0.005]:
            input = base.copy()
            input['lat'] += lat_diff
            input['lon'] += lon_diff
            inputs.append(input)

    return inputs


if __name__ == '__main__':
    check("/home/neil/Documents/albion/pv-square-cheshire2.csv", gen_inputs(53.305, -2.105))
    check("/home/neil/Documents/albion/pv-square-braintree2.csv", gen_inputs(51.905, 0.605))
    check("/home/neil/Documents/albion/pv-square-whittlebury2.csv", gen_inputs(52.105, -0.905))
    check("/home/neil/Documents/albion/pv-square-newcastle2.csv", gen_inputs(54.905, -1.605))
    check("/home/neil/Documents/albion/pv-square-bris-small2.csv", gen_inputs(51.405, -2.605))
