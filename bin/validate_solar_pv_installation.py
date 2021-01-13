import datetime as dt
import json
import logging
import os

import psycopg2
from psycopg2.extras import Json

from albion_models.lidar.get_lidar import get_all_lidar
from albion_models.solar_pv.model_single_solar_pv_installation import \
    model_single_solar_pv_installation


def insert_job(pg_conn, geojson: str, project: str, lidar: bool, params: dict) -> int:
    with pg_conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO models.job_queue 
            (project, created_at, bounds, solar_pv, heat_demand, soft_dig, lidar, status, email, params) 
            VALUES 
            (%s, %s, ST_Transform(ST_Multi(ST_GeomFromGeoJSON(%s)), 27700), %s, %s, %s, %s, %s, %s, %s)
            RETURNING job_id;
            """,
            (project, dt.datetime.now(), geojson, False, False, False, lidar,
             'NOT_STARTED', None, Json(params))
        )
        pg_conn.commit()
        return cursor.fetchone()[0]


def finish_job(pg_conn, job_id):
    with pg_conn.cursor() as cursor:
        cursor.execute(
            "UPDATE models.job_queue SET status = 'COMPLETE' WHERE job_id = %s",
            (job_id,))
        pg_conn.commit()


def print_results(pg_conn, job_ids):
    with pg_conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                q.params ->> 'aggregate_fn',
                SUM(jan_avg_energy_prod_kwh_per_month),
                SUM(feb_avg_energy_prod_kwh_per_month),
                SUM(mar_avg_energy_prod_kwh_per_month),
                SUM(apr_avg_energy_prod_kwh_per_month),
                SUM(may_avg_energy_prod_kwh_per_month),
                SUM(jun_avg_energy_prod_kwh_per_month),
                SUM(jul_avg_energy_prod_kwh_per_month),
                SUM(aug_avg_energy_prod_kwh_per_month),
                SUM(sep_avg_energy_prod_kwh_per_month),
                SUM(oct_avg_energy_prod_kwh_per_month),
                SUM(nov_avg_energy_prod_kwh_per_month),
                SUM(dec_avg_energy_prod_kwh_per_month)
            FROM models.solar_pv s
            LEFT JOIN models.job_queue q ON q.job_id = s.job_id 
            WHERE s.job_id IN %s GROUP BY q.job_id""", (tuple(job_ids),))
        pg_conn.commit()
        rows = cursor.fetchall()
        for row in rows:
            print(','.join([str(cell) for cell in row]))

        cursor.execute(
            """
            SELECT
                (q.params ->> 'aggregate_fn') || '-std-dev',
                SUM(jan_energy_std_dev_m),
                SUM(feb_energy_std_dev_m),
                SUM(mar_energy_std_dev_m),
                SUM(apr_energy_std_dev_m),
                SUM(may_energy_std_dev_m),
                SUM(jun_energy_std_dev_m),
                SUM(jul_energy_std_dev_m),
                SUM(aug_energy_std_dev_m),
                SUM(sep_energy_std_dev_m),
                SUM(oct_energy_std_dev_m),
                SUM(nov_energy_std_dev_m),
                SUM(dec_energy_std_dev_m)
            FROM models.solar_pv s
            LEFT JOIN models.job_queue q ON q.job_id = s.job_id 
            WHERE s.job_id IN %s GROUP BY q.job_id""", (tuple(job_ids),))
        pg_conn.commit()
        rows = cursor.fetchall()
        for row in rows:
            print(','.join([str(cell) for cell in row]))


def model(geojson: dict, project_name: str, params: dict):
    logging.basicConfig(level=logging.INFO,
                        format='[%(asctime)s] %(levelname)s: %(message)s')
    pg_uri = os.environ.get("PG_URI")
    pg_conn = psycopg2.connect(pg_uri)
    job_ids = []
    geojson = json.dumps(geojson)

    params['aggregate_fn'] = 'max'
    job_ids.append(insert_job(pg_conn, geojson, project_name, True, params))
    params['aggregate_fn'] = 'min'
    job_ids.append(insert_job(pg_conn, geojson, project_name, False, params))
    params['aggregate_fn'] = 'avg'
    job_ids.append(insert_job(pg_conn, geojson, project_name, False, params))
    all_params = {
        job_ids[0]: params.copy(),
        job_ids[1]: params.copy(),
        job_ids[2]: params.copy(),
    }
    all_params[job_ids[0]]['aggregate_fn'] = 'max'
    all_params[job_ids[1]]['aggregate_fn'] = 'min'
    all_params[job_ids[2]]['aggregate_fn'] = 'avg'

    lidar_tiff_paths = get_all_lidar(pg_conn, job_ids[0], os.environ.get("LIDAR_DIR"))

    for job_id in job_ids:
        params = all_params[job_id]
        horizon_search_radius = params.get('horizon_search_radius', 1000)
        horizon_slices = params.get('horizon_slices', 16)
        flat_roof_degrees = params.get('flat_roof_degrees', 10)
        peak_power_per_m2 = params.get('peak_power_per_m2', 0.120)
        pv_tech = params.get('pv_tech', 'crystSi')
        roof_area_percent_usable = params.get('roof_area_percent_usable', 100)
        aggregate_fn = params['aggregate_fn']
        model_single_solar_pv_installation(
            pg_uri=os.environ.get("PG_URI"),
            root_solar_dir=os.environ.get("SOLAR_DIR"),
            job_id=job_id,
            lidar_paths=lidar_tiff_paths,
            horizon_search_radius=horizon_search_radius,
            horizon_slices=horizon_slices,
            roof_area_percent_usable=roof_area_percent_usable,
            flat_roof_degrees=flat_roof_degrees,
            peak_power_per_m2=peak_power_per_m2,
            pv_tech=pv_tech,
            aggregate_fn=aggregate_fn)
        finish_job(pg_conn, job_id)
    print_results(pg_conn, job_ids)


def pvoutput_81195():
    model(project_name='falmouth road solar pv',
          params={
              "peak_power_per_m2": 0.105,
          },
          geojson={
              "type": "Polygon",
              "coordinates": [[
                  [-5.226457891, 50.229524994],
                  [-5.226428391, 50.229506994],
                  [-5.226465991, 50.229482994],
                  [-5.226496791, 50.229501994],
                  [-5.226457891, 50.229524994]
              ]]
          })


def pvoutput_6717():
    model(project_name='Sunnydale road solar pv',
          params={
              "peak_power_per_m2": 0.101,
          },
          geojson={
              "type": "Polygon",
              "coordinates": [[
                  [-2.922936889, 51.365816995],
                  [-2.922937589, 51.365754995],
                  [-2.922990589, 51.365757995],
                  [-2.922989189, 51.365816995],
                  [-2.922936889, 51.365816995]
              ]]
          })


def pvoutput_16188():
    model(project_name='ogden drive solar pv',
          params={
              "peak_power_per_m2": 0.135,
          },
          geojson={
              "type": "Polygon",
              "coordinates": [[
                  [-2.3335114, 53.686183],
                  [-2.3334491, 53.686117],
                  [-2.3334873, 53.686105],
                  [-2.3335497, 53.686170],
                  [-2.3335114, 53.686183]]]
          })


def pvoutput_9047():
    model(project_name='pvoutput test 9047',
          params={
              "peak_power_per_m2": 0.118,
          },
          geojson={
              "type": "MultiPolygon",
              "coordinates": [
                  [[
                      [-0.11615993, 51.175583],
                      [-0.11612238, 51.175582],
                      [-0.11612774, 51.175558],
                      [-0.11614518, 51.175558],
                      [-0.11614585, 51.175540],
                      [-0.11613042, 51.175540],
                      [-0.11613243, 51.175532],
                      [-0.11616395, 51.175534],
                      [-0.11615993, 51.175583],
                  ]],
                  [[
                      [-0.11606873, 51.175576],
                      [-0.11608074, 51.175528],
                      [-0.11611897, 51.175531],
                      [-0.11611762, 51.175540],
                      [-0.11609019, 51.175539],
                      [-0.11608751, 51.175556],
                      [-0.11610494, 51.175558],
                      [-0.11610025, 51.175580],
                      [-0.11606873, 51.175576],
                  ]],
              ]
          })


def pvoutput_12406():
    model(project_name='pvoutput test 12406',
          params={
              "peak_power_per_m2": 0.134,
          },
          geojson={
              "type": "MultiPolygon",
              "coordinates": [
                  [[
                      [-1.5599697, 50.913349],
                      [-1.5599523, 50.913342],
                      [-1.5600294, 50.913284],
                      [-1.5600448, 50.913294],
                      [-1.5599697, 50.913349],
                  ]],
                  [[
                      [-1.5599429, 50.913335],
                      [-1.5599268, 50.913328],
                      [-1.5599986, 50.913272],
                      [-1.5600180, 50.913279],
                      [-1.5599429, 50.913335],
                  ]],
              ]
          })


def pvoutput_7986():
    model(project_name='pvoutput test 7986',
          params={
              "peak_power_per_m2": 0.129,
          },
          geojson={
              "type": "Polygon",
              "coordinates": [
                  [
                      [-0.15629936, 51.117137],
                      [-0.15629869, 51.117101],
                      [-0.15633289, 51.117101],
                      [-0.15633222, 51.117138],
                      [-0.15629936, 51.117137],
                  ]
              ]
          })


def pvoutput_8602():
    model(project_name='pvoutput test 8602',
          params={
              "peak_power_per_m2": 0.101,
          },
          geojson={
              "type": "Polygon",
              "coordinates": [
                  [
                      [-1.0332774, 51.472468],
                      [-1.0331862, 51.472421],
                      [-1.0332144, 51.472394],
                      [-1.0333002, 51.472437],
                      [-1.0332774, 51.472468],
                  ]
              ]
          })


def pvoutput_7321a():
    model(project_name='pvoutput test 7321a',
          params={
              "peak_power_per_m2": 0.106,
          },
          geojson={
              "type": "Polygon",
              "coordinates": [
                  [
                      [0.32830607, 50.795894],
                      [0.32833625, 50.795886],
                      [0.32831211, 50.795853],
                      [0.32827992, 50.795862],
                      [0.32830607, 50.795894],
                  ]
              ]
          })


def pvoutput_7321b():
    model(project_name='pvoutput test 7321b',
          params={
              "peak_power_per_m2": 0.120,
          },
          geojson={
              "type": "Polygon",
              "coordinates": [
                  [
                      [0.32829534, 50.795914],
                      [0.32832351, 50.795948],
                      [0.32836374, 50.795941],
                      [0.32832820, 50.795902],
                      [0.32829534, 50.795914],
                  ]
              ]
          })


if __name__ == '__main__':
    # pvoutput_81195()
    # pvoutput_6717()
    # pvoutput_16188()
    # pvoutput_9047()
    # pvoutput_12406()
    # pvoutput_7986()
    # pvoutput_8602()
    # pvoutput_7321a()
    # pvoutput_7321b()
    pass
