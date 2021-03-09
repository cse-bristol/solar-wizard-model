import logging
import os
import subprocess
from datetime import datetime
from os.path import join
from typing import List

from psycopg2._json import Json
from psycopg2.sql import SQL, Identifier

from albion_models.db_funcs import connect, to_csv, process_pg_uri, copy_tsv, count
from albion_models.solar_pv import tables
from albion_models.solar_pv.model_solar_pv import _init_schema
from albion_models.solar_pv.polygonize import _horizon_cols, _southerly_horizon_cols, \
    _aggregated_horizon_cols
from albion_models.solar_pv.saga_gis.horizons import find_horizons


def _get_horizons(pg_uri: str, solar_dir: str, table: str, job_id: int, lidar_paths: List[str]):
    _init_schema(pg_uri, job_id)
    os.makedirs(solar_dir, exist_ok=True)
    find_horizons(pg_uri, job_id,
                  solar_dir=solar_dir,
                  lidar_paths=lidar_paths,
                  horizon_search_radius=1000,
                  horizon_slices=16,
                  masking_strategy="building",
                  mask_table=table,
                  override_res=1.0)


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


def _saga_solar_radiation(pg_conn, job_id: int, solar_dir: str):
    logging.info("Getting solar radiation from SAGA...")
    command = f'saga_cmd ta_lighting 2 ' \
              f'-GRD_DEM {join(solar_dir, "cropped_lidar.tif")} ' \
              f'-MASK {join(solar_dir, "mask.tif")} ' \
              f'-GRD_SVF {join(solar_dir, "svf_out.sgrd")} ' \
              f'-GRD_TOTAL {join(solar_dir, "total.sgrd")} ' \
              f'-DAY 2021-06-21 ' \
              f'-HOUR_STEP 2 '

    res = subprocess.run(command, capture_output=True, text=True, shell=True)
    print(res.stdout)
    print(res.stderr)
    if res.returncode != 0 and "corrupted double-linked list" not in res.stderr:
        raise ValueError(res.stderr)

    logging.info("Converting raster to TSV for copy to postgres...")
    import gdal
    import numpy as np
    file = gdal.Open(join(solar_dir, "total.sdat"))
    band = file.GetRasterBand(1)
    a = band.ReadAsArray()

    tsv = join(solar_dir, "total.tsv")
    with open(tsv, 'w') as f:
        for index, val in np.ndenumerate(a):
            if int(val) != -99999:
                x = index[1]
                y = a.shape[0] - index[0]
                f.write(f"{x}\t{y}\t{val}\n")

    with pg_conn.cursor() as cursor:
        cursor.execute(SQL(
            "CREATE TABLE {radiation} (x int, y int, kwh_m2 double precision);"
        ).format(radiation=Identifier(tables.schema(job_id), "radiation")))
        pg_conn.commit()

    logging.info("Copying TSV to postgres...")
    copy_tsv(pg_conn, tsv, f"{tables.schema(job_id)}.radiation")


def _aggregate_by_building(pg_conn, job_id: int, table: str, horizon_slices: int):
    logging.info("Aggregating by building...")
    with pg_conn.cursor() as cursor:
        cursor.execute(SQL(
            """
            CREATE INDEX ON {radiation} (x,y);
            CREATE INDEX ON {pixel_horizons} (x,y);

            ALTER TABLE {pixel_horizons} ADD COLUMN IF NOT EXISTS lonlat geometry(Point, 4326);
            UPDATE {pixel_horizons} SET lonlat = ST_Transform(en, 4326);
            CREATE INDEX IF NOT EXISTS pixel_horizons_lonlat_idx ON {pixel_horizons} USING GIST (lonlat);
            COMMIT;

            DROP TABLE IF EXISTS {building_horizon};
            CREATE TABLE {building_horizon} AS
            SELECT
                t.ogc_fid,
                t.feat_id, 
                t.fotfeat_id, 
                avg(h.sky_view_factor) AS avg_sky_view_factor,
                stddev(h.sky_view_factor) AS sky_view_factor_sd,
                avg(h.percent_visible) AS avg_percent_visible,
                stddev(h.percent_visible) AS percent_visible_sd,
                avg(r.kwh_m2) AS radiation_kwh_m2,
                {aggregated_horizon_cols}
            FROM {table} t LEFT JOIN {pixel_horizons} h
            ON ST_Contains(t.geom_4326, h.lonlat)
            LEFT JOIN {radiation} r
            ON r.x = h.x and r.y = h.y
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
            COMMIT;
            
            ALTER TABLE {building_horizon} {drop_horizon_cols};
            """).format(
            table=Identifier(*table.split(".")),
            pixel_horizons=Identifier(tables.schema(job_id), tables.PIXEL_HORIZON_TABLE),
            radiation=Identifier(tables.schema(job_id), "radiation"),
            building_horizon=Identifier(tables.schema(job_id), "building_horizon"),
            aggregated_horizon_cols=SQL(_aggregated_horizon_cols(horizon_slices, 'avg')),
            horizon_cols=SQL(','.join(_horizon_cols(horizon_slices))),
            drop_horizon_cols=SQL(','.join([f'DROP COLUMN horizon_slice_{i}' for i in range(0, horizon_slices)])),
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
        solar_dir = join(os.environ.get("SOLAR_DIR"), f"job_{job_id}")
        _get_horizons(pg_uri, solar_dir, table, job_id, _get_lidar(lidar_dir))

        if count(pg_uri, tables.schema(job_id), "radiation") > 0:
            logging.info("Not detecting radiation, data already loaded.")
        else:
            _saga_solar_radiation(pg_conn, job_id, solar_dir)

        _aggregate_by_building(pg_conn, job_id, table, horizon_slices=16)
        to_csv(pg_conn, out_csv, f"{tables.schema(job_id)}.building_horizon")
    finally:
        pg_conn.close()


if __name__ == '__main__':
    # main("denmark.cph",
    #      lidar_dir="/home/neil/data/albion-data/denmark/lidar/copenhagen/lidar-dsm",
    #      out_csv="/home/neil/data/albion-data/cph.csv")
    main("denmark.aalborg",
         lidar_dir="/home/neil/data/albion-data/denmark/lidar/aalborg/lidar-dsm",
         out_csv="/home/neil/data/albion-data/aalborg.csv")
