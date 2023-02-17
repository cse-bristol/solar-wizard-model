# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import json
import logging
import os
import re
from calendar import mdays
from os.path import join
from typing import List

import psycopg2
from psycopg2.extras import Json
from psycopg2.sql import SQL, Literal
import shapely.wkt
from osgeo import gdal, ogr
import numpy as np

from solar_model import paths, gdal_helpers, geos
from solar_model.db_funcs import connection, sql_command
from solar_model.geos import project_geom, from_geojson
from solar_model.lidar.bulk_lidar_client import LidarSource
from solar_model.lidar.en_to_grid_ref import en_to_grid_ref
from solar_model.lidar.lidar import LidarTile, Resolution, zip_to_geotiffs, \
    ZippedTiles
from solar_model.postgis import load_lidar
from solar_model.solar_pv.constants import FLAT_ROOF_DEGREES_THRESHOLD, SYSTEM_LOSS
from solar_model.solar_pv.mask import create_mask
from solar_model.solar_pv.pvgis import pvmaps
from solar_model.transformations import _7_PARAM_SHIFT


def _mask_sql(pg_uri: str, wkt: str) -> str:
    with connection(pg_uri, cursor_factory=psycopg2.extras.DictCursor) as pg_conn:
        # r.pv breaks if region is smaller than 10x10, so we stick a buffer on
        # the mask to ensure it's greater than that:
        sql = SQL(
            "SELECT ST_Buffer(ST_GeomFromText({wkt}, 27700), 5)"
        ).format(
            wkt=Literal(wkt)
        ).as_string(pg_conn)
        return sql


def _load_lidar(pg_uri: str, wkt: str, solar_dir: str, horizon_search_radius: int):
    coords = [float(c) for c in re.split(r"[a-zA-Z(), ]+", wkt) if c != '']
    e, n = coords[0], coords[1]
    hsr = horizon_search_radius
    square_size = LidarSource.ENGLAND.cell_size
    grid_refs = set()
    for easting, northing in [(e, n),
                              (e + hsr, n),
                              (e + hsr, n + hsr),
                              (e, n + hsr),
                              (e - hsr, n),
                              (e - hsr, n - hsr),
                              (e, n - hsr)]:
        grid_ref = en_to_grid_ref(easting, northing, square_size)
        grid_refs.add(grid_ref)

    logging.info(f"LiDAR grid refs: {grid_refs}")
    any_missing = False
    for grid_ref in grid_refs:
        lidar_filepath = LidarSource.ENGLAND.filepath(
            os.environ.get("BULK_LIDAR_DIR"),
            grid_ref,
            Resolution.R_1M)
        if not os.path.exists(lidar_filepath):
            server_filepath = LidarSource.ENGLAND.filepath(
                "/srv/lidar",
                grid_ref,
                Resolution.R_1M)
            rsync_cmd = f"rsync bolt.r.cse.org.uk:{server_filepath} {lidar_filepath}"
            print(f"LIDAR tile needed not present - get from bolt:\n{rsync_cmd}")
            any_missing = True

    if any_missing:
        raise ValueError("Some LIDAR missing, see above")

    for grid_ref in grid_refs:
        lidar_filepath = LidarSource.ENGLAND.filepath(
            os.environ.get("BULK_LIDAR_DIR"),
            grid_ref,
            Resolution.R_1M)
        lidar_tiles = zip_to_geotiffs(
            zt=ZippedTiles.from_filename(lidar_filepath, LidarSource.ENGLAND.year),
            lidar_dir=os.environ.get("LIDAR_DIR"))

        with connection(pg_uri) as pg_conn:
            load_lidar(pg_conn, lidar_tiles, solar_dir)


def _get_1m_lidar(pg_conn, wkt: str, buffer: int, output_file: str):
    raster = sql_command(
        pg_conn,
        """
        SELECT ST_AsGDALRaster(ST_Union(rast), 'GTiff') AS rast 
        FROM models.lidar_1m 
        WHERE st_intersects(rast, ST_Buffer(ST_GeomFromText( %(wkt)s, 27700), %(buffer)s ))
        """,
        {"wkt": wkt,
         "buffer": buffer},
        result_extractor=lambda res: res[0][0])

    with open(output_file, 'wb') as f:
        f.write(raster)


def _gen_rasters(pg_uri: str, solar_dir: str, wkt: str, horizon_search_radius: int):
    elevation_raster_27700 = join(solar_dir, 'elevation_27700.tif')
    elevation_raster = join(solar_dir, 'elevation.tif')
    mask_raster_27700 = join(solar_dir, "mask_27700.tif")
    mask_raster = join(solar_dir, "mask.tif")

    with connection(pg_uri, cursor_factory=psycopg2.extras.DictCursor) as pg_conn:
        _get_1m_lidar(pg_conn, wkt, horizon_search_radius, elevation_raster_27700)

    gdal_helpers.reproject(
        raster_in=elevation_raster_27700,
        raster_out=elevation_raster,
        src_srs=_7_PARAM_SHIFT,
        dst_srs="EPSG:4326")

    create_mask(
        mask_sql=_mask_sql(pg_uri, wkt),
        mask_out=mask_raster_27700,
        pg_uri=pg_uri,
        res=1.0,
        srid=27700)

    gdal_helpers.reproject(
        raster_in=mask_raster_27700,
        raster_out=mask_raster,
        src_srs=_7_PARAM_SHIFT,
        dst_srs="EPSG:4326")

    gdal_helpers.crop_or_expand(
        file_to_crop=mask_raster,
        reference_file=elevation_raster,
        out_tiff=mask_raster,
        adjust_resolution=True)

    return elevation_raster, mask_raster


def model(project_name: str, kwp: float, wkt: str):
    print(project_name)
    if os.environ.get("LIDAR_DIR", None) is None:
        raise ValueError(f"env var LIDAR_DIR must be set")
    if os.environ.get("BULK_LIDAR_DIR", None) is None:
        raise ValueError(f"env var BULK_LIDAR_DIR must be set")
    if os.environ.get("PG_URI", None) is None:
        raise ValueError(f"env var PG_URI must be set")
    if os.environ.get("SOLAR_DIR", None) is None:
        raise ValueError(f"env var SOLAR_DIR must be set")
    if os.environ.get("PVGIS_DATA_TAR_FILE_DIR", None) is None:
        raise ValueError(f"env var PVGIS_DATA_TAR_FILE_DIR must be set")
    if os.environ.get("PVGIS_GRASS_DBASE_DIR", None) is None:
        raise ValueError(f"env var PVGIS_GRASS_DBASE_DIR must be set")

    logging.basicConfig(level=logging.WARN,
                        format='[%(asctime)s] %(levelname)s: %(message)s')

    pg_uri = os.environ.get("PG_URI")
    solar_dir = join(os.environ.get("SOLAR_DIR"), project_name.replace(" ", "_"))
    horizon_search_radius = 1000
    horizon_slices = 36
    flat_roof_degrees = 10
    pv_tech = 'cSi'
    system_loss = SYSTEM_LOSS

    geom = shapely.wkt.loads(wkt)
    peak_power_per_m2 = kwp / geom.area

    pvmaps_dir = join(solar_dir, "pvmaps")
    os.makedirs(pvmaps_dir, exist_ok=True)

    _load_lidar(pg_uri, wkt, solar_dir, horizon_search_radius)

    elevation_raster, mask_raster = _gen_rasters(
        pg_uri, solar_dir, wkt, horizon_search_radius
    )

    pvm = pvmaps.PVMaps(
        grass_dbase_dir=os.environ.get("PVGIS_GRASS_DBASE_DIR"),
        input_dir=solar_dir,
        output_dir=pvmaps_dir,
        pvgis_data_tar_file=join(os.environ.get("PVGIS_DATA_TAR_FILE_DIR"), "pvgis_data.tar"),
        pv_model_coeff_file_dir=paths.RESOURCES_DIR,
        keep_temp_mapset=True,
        num_processes=2,
        output_direct_diffuse=False,
        horizon_step_degrees=360 // horizon_slices,
        horizon_search_distance=horizon_search_radius,
        flat_roof_degrees=flat_roof_degrees,
        flat_roof_degrees_threshold=FLAT_ROOF_DEGREES_THRESHOLD,
        panel_type=pv_tech,
    )
    pvm.create_pvmap(
        elevation_filename=os.path.basename(elevation_raster),
        mask_filename=os.path.basename(mask_raster))

    monthly_kwhs = []
    for r_in, month in zip(pvm.monthly_wh_rasters, range(1, 13)):
        monthly_wh_27700 = join(pvmaps_dir, f"wh_m{str(month).zfill(2)}_27700.tif")
        gdal_helpers.reproject(r_in, monthly_wh_27700, src_srs="EPSG:4326", dst_srs=_7_PARAM_SHIFT)

        ds = gdal.Open(monthly_wh_27700)
        band = ds.GetRasterBand(1)
        a = band.ReadAsArray()
        to_en = ds.GetGeoTransform()

        monthly_kwh = 0
        for y, row in enumerate(a):
            for x, wh in enumerate(row):
                if not np.isnan(wh):
                    e, n = gdal.ApplyGeoTransform(to_en, x, y)
                    cell = geos.square(e - 0.5, n - 0.5, 1)
                    if cell.intersects(geom):
                        intersect = cell.intersection(geom)
                        monthly_kwh += wh * intersect.area * 0.001 * mdays[month] * peak_power_per_m2 * (1 - system_loss)

        monthly_kwhs.append(monthly_kwh)
        print(monthly_kwh)

    # print(monthly_kwhs)


def _geojson_to_wkt(geojson) -> str:
    """Helper function to migrate from old validation setup
    (took geojson in long/lat) to new validation setup (WKT in EN)"""
    ll = from_geojson(geojson)
    en = project_geom(ll, 4326, 27700)
    print(en.wkt)
    return en.wkt


def pvoutput_81195():
    model(project_name='falmouth road solar pv',
          kwp=1.62,
          wkt="Polygon ((169984.01241839 41595.17629588, 169986.51871453 41592.48296272, 169989.51130693 41595.10148107, 169987.15464041 41597.98185126, 169984.01241839 41595.17629588))")


def pvoutput_6717():
    model(project_name='Sunnydale road solar pv',
          kwp=3.92,
          wkt="Polygon ((335844.60642652 163377.74434855, 335844.47090278 163370.84988831, 335840.78542633 163371.22996628, 335840.9654867 163377.79018043, 335844.60642652 163377.74434855))")


def pvoutput_16188():
    model(project_name='ogden drive solar pv',
          kwp=4.0,
          wkt="Polygon ((378071.20729895 421131.94110285, 378075.28732655 421124.57880494, 378072.75820668 421123.2555576, 378068.67105522 421130.50663135, 378071.20729895 421131.94110285))")


def pvoutput_9047():
    model(project_name='pvoutput test 9047',
          kwp=4.08,
          wkt="MultiPolygon (((531782.25747756 143502.88881274, 531784.8850125 143502.84486204, 531784.57873306 143500.16652072, 531783.35970383 143500.135288, 531783.36415345 143498.13253146, 531784.44268718 143498.16016453, 531784.32498319 143497.26698414, 531782.11608444 143497.43293152, 531782.25747756 143502.88881274)),((531788.65215773 143502.2737586, 531787.94943315 143496.91476521, 531785.26866607 143497.17989177, 531785.33738788 143498.18308779, 531787.25755256 143498.12101455, 531787.39644641 143500.01617324, 531786.17241805 143500.20735294, 531786.43756281 143502.66209927, 531788.65215773 143502.2737586)))")


def pvoutput_12406():
    model(project_name='pvoutput test 12406',
          kwp=4.0,
          wkt="MultiPolygon (((431030.57431621 112746.60988658, 431031.8022062 112745.83874145, 431026.42035328 112739.35657822, 431025.33107926 112740.46217625, 431030.57431621 112746.60988658)),((431032.46768159 112745.06424572, 431033.60417978 112744.2925565, 431028.59360295 112738.03502047, 431027.2251077 112738.80532878, 431032.46768159 112745.06424572)))")


def pvoutput_7986():
    model(project_name='pvoutput test 7986',
          kwp=1.4,
          wkt="Polygon ((529139.23164784 136932.9113343, 529139.37883168 136928.90941244, 529136.98526852 136928.84944537, 529136.92908351 136932.96491396, 529139.23164784 136932.9113343))")


def pvoutput_8602():
    model(project_name='pvoutput test 8602',
          kwp=3.824,
          wkt="Polygon ((467238.31856274 175273.90582796, 467244.7217904 175268.7624356, 467242.80280438 175265.73384699, 467236.7804995 175270.43734106, 467238.31856274 175273.90582796))")


def pvoutput_7321a():
    model(project_name='pvoutput test 7321a',
          kwp=2.4,
          wkt="Polygon ((564182.15033521 102176.59991014, 564184.30506338 102175.77742984, 564182.71956657 102172.0547441, 564180.41969499 102172.98394782, 564182.15033521 102176.59991014))")


def pvoutput_7321b():
    model(project_name='pvoutput test 7321b',
          kwp=2.4,
          wkt="Polygon ((564182.67835655 102178.10084899, 564184.54433261 102181.94366545, 564187.40375273 102181.25468106, 564185.035947 102176.83957885, 564182.67835655 102178.10084899))")


if __name__ == '__main__':
    # pvoutput_81195()
    # pvoutput_6717()
    pvoutput_16188()
    pvoutput_9047()
    pvoutput_12406()
    pvoutput_7986()
    pvoutput_8602()
    pvoutput_7321a()
    pvoutput_7321b()
    pass
