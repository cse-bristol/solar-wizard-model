import logging
import subprocess
from os.path import join

from psycopg2.sql import SQL, Identifier, Literal

import albion_models.solar_pv.tables as tables
from albion_models.db_funcs import sql_script, copy_csv, connect, count
from albion_models.solar_pv import mask
from albion_models import gdal_helpers


def find_horizons(pg_uri: str,
                  job_id: int,
                  solar_dir: str,
                  lidar_vrt_file: str,
                  horizon_search_radius: int,
                  horizon_slices: int,
                  masking_strategy: str,
                  mask_table: str = "mastermap.building",
                  override_res: float = None) -> float:
    """
    Detect the horizon height in degrees for each `horizon_slices` slice of the
    compass for each LIDAR pixel that falls inside one of the OS MasterMap building
    polygons.

    Returns the resolution of the LIDAR, or the override_res value if passed.
    """
    srid = gdal_helpers.get_srid(lidar_vrt_file, fallback=27700)
    if override_res is None:
        res = gdal_helpers.get_res(lidar_vrt_file)
    else:
        res = override_res

    if count(pg_uri, tables.schema(job_id), tables.PIXEL_HORIZON_TABLE) > 0:
        logging.info("Not detecting horizon, horizon data already loaded.")
        return res

    unit_dims, unit = gdal_helpers.get_srs_units(lidar_vrt_file)
    if unit_dims != 1.0 or unit != 'metre':
        # If this ever needs changing - the `resolution_metres` param of `aggregate_horizons()`
        # needs a resolution per metre rather than per whatever the unit of the SRS is -
        # otherwise the calculated areas/footprints of PV installations will be wrong.
        # See `create.roof-horizons.sql`
        raise ValueError(f"Albion cannot currently handle LIDAR where the SRS unit is "
                         f"not 1m: was {unit} {unit_dims}")

    logging.info("Creating raster mask...")
    if masking_strategy == 'building':
        mask_file = mask.create_buildings_mask(job_id, solar_dir, pg_uri, res=res, mask_table=mask_table, srid=srid)
    elif masking_strategy == 'bounds':
        mask_file = mask.create_bounds_mask(job_id, solar_dir, pg_uri, res=res, srid=srid)
    else:
        raise ValueError(f"Unknown masking strategy {masking_strategy}")

    logging.info("Cropping lidar to mask dimensions...")
    if masking_strategy == 'bounds':
        gdal_helpers.crop_or_expand(mask_file, lidar_vrt_file, mask_file, adjust_resolution=False)

    cropped_lidar = join(solar_dir, 'cropped_lidar.tif')
    gdal_helpers.crop_or_expand(lidar_vrt_file, mask_file, cropped_lidar, adjust_resolution=True)

    logging.info("Using 320-albion-saga-gis to find horizons...")
    horizons_csv = join(solar_dir, 'horizons.csv')
    _get_horizons(cropped_lidar, solar_dir, mask_file, horizons_csv, horizon_search_radius, horizon_slices)
    _load_horizons_to_db(pg_uri, job_id, horizons_csv, horizon_slices, srid)

    return res


def _get_horizons(lidar_tif: str, solar_dir: str, mask_tif: str, csv_out: str, search_radius: int, slices: int):
    command = f'saga_cmd ta_lighting 3 ' \
              f'-DEM {lidar_tif} ' \
              f'-VISIBLE {join(solar_dir, "vis_out.tiff")} ' \
              f'-SVF {join(solar_dir, "svf_out.tiff")} ' \
              f'-CSV {csv_out} ' \
              f'-MASK {mask_tif} ' \
              f'-RADIUS {search_radius} ' \
              f'-NDIRS {slices} '

    res = subprocess.run(command, capture_output=True, text=True, shell=True)
    print(res.stdout)
    print(res.stderr)
    # Seems like SAGA GIS sometimes crashes during cleanup due to some c++ use-after-free bug
    # with the message "corrupted double-linked list". We can ignore as it has generated
    # all outputs by this time.
    if res.returncode != 0 and "corrupted double-linked list" not in res.stderr:
        raise ValueError(res.stderr)


def _load_horizons_to_db(pg_uri: str, job_id: int, horizon_csv: str, horizon_slices: int, srid: int):
    pg_conn = connect(pg_uri)
    schema = tables.schema(job_id)
    pixel_horizons_table = tables.PIXEL_HORIZON_TABLE
    horizon_cols = ','.join([f'horizon_slice_{i} double precision' for i in range(0, horizon_slices)])
    try:
        sql_script(
            pg_conn, 'pv/create.pixel-horizons.sql',
            pixel_horizons=Identifier(schema, pixel_horizons_table),
            horizon_cols=SQL(horizon_cols),
        )

        copy_csv(pg_conn, horizon_csv, f"{schema}.{pixel_horizons_table}")

        sql_script(
            pg_conn, 'pv/post-load.pixel-horizons.sql',
            pixel_horizons=Identifier(schema, pixel_horizons_table),
            srid=Literal(srid)
        )
    finally:
        pg_conn.close()
