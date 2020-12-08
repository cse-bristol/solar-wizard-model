from os.path import join
from typing import List

import requests

from paths import PROJECT_ROOT
from pv_gis.flatten import flatten
from solar_pv.db_funcs import copy_csv, connect


def import_csv(pg_uri: str, filename: str):
    pg_conn = connect(pg_uri)
    with pg_conn.cursor() as cursor:
        cursor.execute(
            "DROP TABLE IF EXISTS real_pv; CREATE TABLE real_pv (ts timestamp, power double precision)")
        pg_conn.commit()
    copy_csv(pg_conn, filename, 'real_pv', encoding='utf-8')
    with pg_conn.cursor() as cursor:
        cursor.execute("""
            SELECT 
                EXTRACT(month FROM month) AS month, 
                AVG(kwh) AS kwh,
                stddev(kwh) AS stddev 
            FROM (
                SELECT 
                    date_trunc('month', ts) AS month, 
                    sum(power) AS watts, 
                    sum(power * 0.25 / 1000) AS kwh 
                FROM real_pv 
                GROUP BY date_trunc('month', ts)
            ) m 
            WHERE kwh != 0
            GROUP BY extract(month from month) 
            ORDER BY month;
        """)
        res = cursor.fetchall()
        pg_conn.commit()
        return res


def pv_gis(lon, lat, peakpower, slope, aspect, horizon):
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


def print_res(res):
    header = ['month', 'real_avg_kWh', 'real_std_dev', 'pv_gis_avg_kWh',
              'pv_gis_std_dev', 'kWh_diff']
    row_format = "{:<25}" * len(header)
    print(row_format.format(*header))
    for row in res:
        print(row_format.format(*row, float(row[1]) - float(row[3])))


def _rad_to_deg(rad):
    return float(rad) * 180 / 3.14159265359


def check(csv_file: str, pv_gis_params: List[dict]):
    res = import_csv('postgresql://albion_webapp:ydBbE3JCnJ4@localhost:32768/albion',
                     csv_file)
    all_pg = []
    for params in pv_gis_params:
        all_pg.append(pv_gis(**params))

    for i in range(0, 12):
        monthly_kwh = 0.0
        monthly_sd = 0.0
        for pg_res in all_pg:
            monthly_kwh += float(pg_res[f'monthly_fixed_{i}_E_m'])
            monthly_sd += float(pg_res[f'monthly_fixed_{i}_SD_m'])
        res[i] = res[i] + (monthly_kwh, monthly_sd)

    print(csv_file)
    print_res(res)


def check_marshfield():
    check(
        join(PROJECT_ROOT, 'data', 'solaredgepv.Marshfield.csv'),
        [{
            "lat": 51.463409,
            "lon": -2.3140825,
            "peakpower": 3.99,
            "slope": 30,
            "aspect": 10,
            "horizon": None  # No lidar data for marshfield...
        }]
    )


def check_abbey_garden():
    check(
        join(PROJECT_ROOT, 'data', 'solaredgepv.AbbeyGardenMachinery.csv'),
        [{
            "lat": 50.565732,
            "lon": -4.1205946,
            "peakpower": 50,
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
        }]
    )


def check_mount_kelly():
    check(
        join(PROJECT_ROOT, 'data', 'solaredgepv.MountKellySenior.csv'),
        [{
            "lat": 50.556476,
            "lon": -4.1318498,
            "peakpower": 25,
            "slope": _rad_to_deg(0.2984885286343611),
            "aspect": _rad_to_deg(4.580234599118946) - 180.0,
            "horizon": [
                _rad_to_deg(1.141759),
                _rad_to_deg(1.169393),
                _rad_to_deg(1.069821),
                _rad_to_deg(0.941134),
                _rad_to_deg(0.887376),
                _rad_to_deg(0.887376),
                _rad_to_deg(0.841052),
                _rad_to_deg(0.640508),
                _rad_to_deg(0.117453),
                _rad_to_deg(0.305979),
                _rad_to_deg(0.557015),
                _rad_to_deg(0.630621),
                _rad_to_deg(0.772342),
                _rad_to_deg(0.786231),
                _rad_to_deg(0.81745),
                _rad_to_deg(1.125945)
            ]
        }, {
            "lat": 50.556472,
            "lon": -4.1319175,
            "peakpower": 25,
            "slope": _rad_to_deg(0.40948704255319146),
            "aspect": _rad_to_deg(1.4682685021276602) - 180.0,
            "horizon": [
                _rad_to_deg(1.19416),
                _rad_to_deg(1.162805),
                _rad_to_deg(0.968868),
                _rad_to_deg(0.765903),
                _rad_to_deg(0.387812),
                _rad_to_deg(0.444019),
                _rad_to_deg(0.289162),
                _rad_to_deg(0.705657),
                _rad_to_deg(0.763666),
                _rad_to_deg(1.005812),
                _rad_to_deg(1.261743),
                _rad_to_deg(1.258471),
                _rad_to_deg(1.258471),
                _rad_to_deg(1.258471),
                _rad_to_deg(1.363427),
                _rad_to_deg(1.243609)
            ]
        }
        ]
    )


if __name__ == '__main__':
    check_marshfield()
    check_abbey_garden()
    check_mount_kelly()
