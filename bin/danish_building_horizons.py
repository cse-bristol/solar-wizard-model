import logging
import os
import subprocess
from datetime import datetime
from os.path import join
from typing import List

from psycopg2._json import Json
from psycopg2.sql import SQL, Identifier

from albion_models.solar_pv.saga_gis.horizons import find_horizons
from albion_models.db_funcs import connect, to_csv, process_pg_uri
from albion_models.solar_pv.model_solar_pv import _init_schema
from albion_models.solar_pv import tables
from albion_models.solar_pv.polygonize import _horizon_cols, _southerly_horizon_cols, \
    _aggregated_horizon_cols


def _get_horizons(pg_uri: str, table: str, job_id: int, lidar_paths: List[str]):
    _init_schema(pg_uri, job_id)
    solar_dir = join(os.environ.get("SOLAR_DIR"), f"job_{job_id}")
    os.makedirs(solar_dir, exist_ok=True)
    find_horizons(pg_uri, job_id,
                  solar_dir=solar_dir,
                  lidar_paths=lidar_paths,
                  horizon_search_radius=1000,
                  horizon_slices=16,
                  masking_strategy="building",
                  mask_table=table)


def _insert_job(pg_conn, table: str) -> int:
    with pg_conn.cursor() as cursor:
        cursor.execute(SQL(
            """
            INSERT INTO models.job_queue 
            (project, created_at, bounds, 
             solar_pv, heat_demand, soft_dig, lidar, solar_pv_cost_benefit, 
             status, params) 
            SELECT
                %s, %s, ST_Transform(ST_Multi(ST_ConvexHull(ST_Collect(geom_4326))), 27700),
                false, false, false, false, false, 
                'COMPLETE'::models.job_status, %s 
            FROM {table}
            RETURNING job_id
            """).format(table=Identifier(*table.split("."))),
            (table, datetime.now(), Json({}))
        )
        pg_conn.commit()
        return cursor.fetchone()[0]


def _get_lidar(dir_name: str) -> List[str]:
    return [os.path.join(dir_name, name) for name in os.listdir(dir_name)]


# def _saga_solar_radiation(solar_dir: str):
#     command = f'saga_cmd ta_lighting 2 ' \
#               f'-GRD_DEM {join(solar_dir, "cropped_lidar.tif")} ' \
#               f'-MASK {join(solar_dir, "mask.tif")} ' \
#               f'-GRD_SVF {join(solar_dir, "svf_out.sdat")} '
#
#     res = subprocess.run(command, capture_output=True, text=True, shell=True)
#     print(res.stdout)
#     print(res.stderr)
#     if res.returncode != 0:
#         raise ValueError(res.stderr)


def _aggregate_by_building(pg_conn, job_id: int, table: str, horizon_slices: int):
    logging.info("Aggregating by building...")
    with pg_conn.cursor() as cursor:
        cursor.execute(SQL(
            """
            ALTER TABLE {pixel_horizons} ADD COLUMN IF NOT EXISTS lonlat geometry(Point, 4326);
            UPDATE {pixel_horizons} SET lonlat = ST_Transform(en, 4326);
            CREATE INDEX IF NOT EXISTS pixel_horizons_lonlat_idx ON {pixel_horizons} USING GIST (lonlat);
            COMMIT;

            CREATE TABLE IF NOT EXISTS {building_horizon} AS
            SELECT
                t.ogc_fid,
                t.feat_id, 
                t.fotfeat_id, 
                t.feat_kode,
                avg(h.sky_view_factor) AS avg_sky_view_factor,
                stddev(h.sky_view_factor) AS sky_view_factor_sd,
                avg(h.percent_visible) AS avg_percent_visible,
                stddev(h.percent_visible) AS percent_visible_sd,
                {aggregated_horizon_cols}
            FROM {table} t LEFT JOIN {pixel_horizons} h
            ON ST_Contains(t.geom_4326, h.lonlat)
            GROUP BY t.ogc_fid;
            COMMIT;
            
            ALTER TABLE {building_horizon} ADD COLUMN IF NOT EXISTS avg_horizon double precision;
            ALTER TABLE {building_horizon} ADD COLUMN IF NOT EXISTS avg_southerly_horizon double precision;
            ALTER TABLE {building_horizon} ADD COLUMN IF NOT EXISTS horizon_sd double precision;
            ALTER TABLE {building_horizon} ADD COLUMN IF NOT EXISTS southerly_horizon_sd double precision;
            WITH sd AS (
                SELECT
                    ogc_fid,
                    avg(horizon) AS avg_horizon,
                    stddev(horizon) AS horizon_sd,
                    avg(southerly_horizon) AS avg_southerly_horizon,
                    stddev(southerly_horizon) AS southerly_horizon_sd
            FROM (
                SELECT
                    ogc_fid,
                    unnest(array[{horizon_cols}]) AS horizon,
                    unnest(array[{southerly_horizon_cols}]) AS southerly_horizon
                FROM {building_horizon} h) sub
              GROUP BY ogc_fid)
            UPDATE {building_horizon} SET 
                avg_horizon = sd.avg_horizon,
                horizon_sd = sd.horizon_sd,
                avg_southerly_horizon = sd.avg_southerly_horizon,
                southerly_horizon_sd = sd.southerly_horizon_sd
            FROM sd
            WHERE {building_horizon}.ogc_fid = sd.ogc_fid;
            """).format(
            table=Identifier(*table.split(".")),
            pixel_horizons=Identifier(tables.schema(job_id), tables.PIXEL_HORIZON_TABLE),
            building_horizon=Identifier(tables.schema(job_id), "building_horizon"),
            aggregated_horizon_cols=SQL(_aggregated_horizon_cols(horizon_slices, 'avg')),
            horizon_cols=SQL(','.join(_horizon_cols(horizon_slices))),
            southerly_horizon_cols=SQL(','.join(_southerly_horizon_cols(horizon_slices))),
        ))
        pg_conn.commit()


def main(table: str, lidar_dir: str, out_csv: str, job_id: int = None):
    logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')
    pg_uri = process_pg_uri(os.environ.get("PG_URI"))
    pg_conn = connect(pg_uri)
    try:
        if job_id is None:
            job_id = _insert_job(pg_conn, table)
        _get_horizons(pg_uri, table, job_id, _get_lidar(lidar_dir))
        _aggregate_by_building(pg_conn, job_id, table, horizon_slices=16)
        to_csv(pg_conn, out_csv, f"{tables.schema(job_id)}.building_horizon")
    finally:
        pg_conn.close()


if __name__ == '__main__':
    main("denmark.cph",
         lidar_dir="/home/neil/data/albion-data/denmark/lidar/copenhagen/lidar-dsm",
         out_csv="/home/neil/data/albion-data/cph.csv",
         job_id=12)
    main("denmark.aalborg",
         lidar_dir="/home/neil/data/albion-data/denmark/lidar/aalborg/lidar-dsm",
         out_csv="/home/neil/data/albion-data/aalborg.csv")
