import os
from os.path import join

import psycopg2.extras
from psycopg2.sql import SQL, Literal, Identifier

from albion_models.db_funcs import connect, sql_script_with_bindings
from albion_models import gdal_helpers
from albion_models.lidar.get_lidar import LIDAR_VRT


def calculate_lidar_coverage(job_id: int, lidar_dir: str, pg_uri: str):
    """
    Calculate LiDAR coverage of the bounds of the job.

    Produces:
     * multipolygon of LiDAR coverage within bounds
     * raw coverage percentage (i.e. total percentage of ground covered)
     * number of buildings within bounds
     * number of buildings within bounds with at least 1 m^2 of LiDAR coverage

     Results are in table `models.lidar_info`.
    """
    job_lidar_dir = join(lidar_dir, f"job_{job_id}")
    lidar_vrt_file = join(job_lidar_dir, LIDAR_VRT)
    srid = gdal_helpers.get_srid(lidar_vrt_file, fallback=27700)
    res = gdal_helpers.get_res(lidar_vrt_file)

    bounds_mask = _create_bounds_mask(job_id, job_lidar_dir, pg_uri, res, srid)

    cropped_lidar = join(job_lidar_dir, "cropped_lidar.tif")
    gdal_helpers.crop_or_expand(lidar_vrt_file, bounds_mask, cropped_lidar, adjust_resolution=True)

    lidar_cov_tif = join(job_lidar_dir, "lidar_cov.tif")
    gdal_helpers.run(f'''
        gdal_calc.py 
        -A {cropped_lidar}
        -B {bounds_mask} 
        --outfile={lidar_cov_tif}
        --quiet
        --calc="logical_and(A!=-9999,B==1)" ''')

    lidar_cov_gpkg = join(job_lidar_dir, "lidar_cov.gpkg")
    gdal_helpers.run(f'gdal_polygonize.py -8 -q {lidar_cov_tif} {lidar_cov_gpkg} lidar_cov')

    gdal_helpers.run(f'''
        ogr2ogr
        -f PostgreSQL PG:{pg_uri}
        -overwrite
        -nln models.lidar_cov_temp_{job_id}
        -nlt MULTIPOLYGON
        -lco GEOMETRY_NAME=geom_27700
        -gt 65536
        -s_srs EPSG:{srid}
        -t_srs EPSG:27700
        {lidar_cov_gpkg}
        --config PG_USE_COPY YES
    ''')

    pg_conn = connect(pg_uri, cursor_factory=psycopg2.extras.DictCursor)
    try:
        sql_script_with_bindings(
            pg_conn,
            "create.lidar-info.sql",
            {"job_id": job_id},
            temp_table=Identifier("models", f"lidar_cov_temp_{job_id}"),
            clean_table=Identifier("models", f"lidar_cov_clean_{job_id}"))
    finally:
        pg_conn.close()

    os.remove(bounds_mask)
    os.remove(cropped_lidar)
    os.remove(lidar_cov_tif)
    os.remove(lidar_cov_gpkg)


def _create_bounds_mask(job_id: int, job_lidar_dir: str, pg_uri: str, res: float, srid: int) -> str:
    """
    Create a raster mask from the bounds polygon associated with the job. Pixels
    inside the bounds will be 1, all others 0.
    """
    pg_conn = connect(pg_uri, cursor_factory=psycopg2.extras.DictCursor)
    try:
        mask_sql = SQL(
            "SELECT ST_Transform(bounds, {srid}) FROM models.job_queue q WHERE q.job_id={job_id}"
        ).format(job_id=Literal(job_id), srid=Literal(srid)).as_string(pg_conn)
    finally:
        pg_conn.close()

    mask_file = join(job_lidar_dir, 'bounds-mask.tif')
    gdal_helpers.rasterize(pg_uri, mask_sql, mask_file, res, srid)
    return mask_file


if __name__ == '__main__':
    calculate_lidar_coverage(
        30,
        '/home/neil/data/albion-models/lidar/job_30',
        'postgresql://albion_webapp:ydBbE3JCnJ4@localhost:5432/albion?application_name=blah')
