import logging

import psycopg2.extras
from psycopg2.sql import Identifier

from albion_models.db_funcs import connection, sql_script


def calculate_lidar_coverage(job_id: int, pg_uri: str):
    """
    Calculate LiDAR coverage of the bounds of the job.

    Produces:
     * multipolygon of LiDAR coverage within bounds
     * raw coverage percentage (i.e. total percentage of ground covered)
     * number of buildings within bounds
     * number of buildings within bounds with at least 1 m^2 of LiDAR coverage

     Results are in table `models.lidar_info`.
    """
    logging.info("Calculating LiDAR coverage polygons")
    with connection(pg_uri, cursor_factory=psycopg2.extras.DictCursor) as pg_conn:
        sql_script(
            pg_conn,
            "create.lidar-info.sql",
            {"job_id": job_id},
            per_tile_table=Identifier("models", f"lidar_cov_per_tile_{job_id}"),
            bounds_table=Identifier("models", f"lidar_temp_bounds_{job_id}"))
    logging.info("Finished calculating LiDAR coverage polygons")


if __name__ == '__main__':
    calculate_lidar_coverage(
        1189,
        'postgresql://albion_webapp:ydBbE3JCnJ4@localhost:5432/albion?application_name=blah')
