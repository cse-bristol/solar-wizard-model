# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
"""
This is a script for exploring what changes in the kWh output of a PV installation
when its horizon varies.
"""
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


def check(pv_gis_params: List[dict]):
    header = ['N', 'NNE', 'NE', 'ENE', 'E', 'ESE', 'SE', 'SSE', 'S', 'SSW', 'SW', 'WSW',
              'W', 'WNW', 'NW', 'NNW', 'kWh', 'kWh_sd', 'kWp', 'lat', 'lon', 'slope',
              'aspect']
    row_format = "{:<7}" * len(header)
    print(row_format.format(*header))

    for params in pv_gis_params:
        pg_res = pv_gis(**params)
        horizon = params['horizon']
        kwh = float(pg_res[f'totals_fixed_E_y'])
        kwh_sd = float(pg_res[f'totals_fixed_SD_y'])
        res = {
            'N': horizon[0],
            'NNE': horizon[1],
            'NE': horizon[2],
            'ENE': horizon[3],
            'E': horizon[4],
            'ESE': horizon[5],
            'SE': horizon[6],
            'SSE': horizon[7],
            'S': horizon[8],
            'SSW': horizon[9],
            'SW': horizon[10],
            'WSW': horizon[11],
            'W': horizon[12],
            'WNW': horizon[13],
            'NW': horizon[14],
            'NNW': horizon[15],
            'kWh': round(kwh, 1),
            'kWh_sd': round(kwh_sd, 1),
            'kWp': params['peakpower'],
            'lat': round(params['lat'], 2),
            'lon': round(params['lon'], 2),
            'slope': round(params['slope'], 2),
            'aspect': round(params['aspect'], 2),
        }
        print(row_format.format(*res.values()))


def gen_inputs() -> List[dict]:
    base = {
        "lat": 50.565732,
        "lon": -4.1205946,
        "peakpower": 0.2,
        "slope": _rad_to_deg(0.3251753474088293),
        "aspect": 135,
        "horizon": []
    }

    inputs = []
    for base_horizon in (0, 10, 20, 30):
        for outlier in (-50, -40, -30, -20, -10, 0, 10, 20, 30, 40, 50):
            if 90 > base_horizon + outlier >= 0:
                input = base.copy()
                input['horizon'] = [base_horizon for _ in range(0, 16)]
                input['horizon'][4] += outlier
                inputs.append(input)
    return inputs


if __name__ == '__main__':
    check(gen_inputs())
