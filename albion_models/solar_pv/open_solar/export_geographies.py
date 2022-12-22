import logging
import os

from albion_models.db_funcs import command_to_gpkg
from albion_models.solar_pv.open_solar.tippecanoe import cmd_tippecanoe

_LA = "la"
_LSOA = "lsoa"
_MSOA = "msoa"
_PARISH = "parish"


def export_la(pg_conn, pg_uri: str, gpkg_fname: str, regenerate: bool):
    if regenerate or not os.path.isfile(gpkg_fname):
        if command_to_gpkg(
            pg_conn, pg_uri, gpkg_fname, _LA,
            src_srs=4326, dst_srs=4326,
            overwrite=True,
            command=
            "SELECT "
            " la_code, "
            " name, "
            " geom_4326 AS geom, "
            " ST_AsGeoJSON(geom_4326) as geom_str "
            "FROM boundaryline.local_authority"
        ) is not None:
            raise RuntimeError(f"Error running ogr2ogr")

        # Don't include the geom_str field as it causes tippecanoe to take ages
        fields = ["geom", "la_code", "name"]
        cmd_tippecanoe(gpkg_fname, _LA, fields)
    else:
        logging.info(f"Not regenerating existing {gpkg_fname}")


def export_lsoa(pg_conn, pg_uri: str, gpkg_fname: str, regenerate: bool):
    if regenerate or not os.path.isfile(gpkg_fname):
        if command_to_gpkg(
            pg_conn, pg_uri, gpkg_fname, _LSOA,
            src_srs=4326, dst_srs=4326,
            overwrite=True,
            command=
            "SELECT "
            " lsoa_2011, "
            " name, "
            " geom_4326 AS geom, "
            " ST_AsGeoJSON(geom_4326) AS geom_str "
            "FROM census_boundaries.lsoa"
        ) is not None:
            raise RuntimeError(f"Error running ogr2ogr")

        # Don't include the geom_str field as it causes tippecanoe to take ages
        fields = ["geom", "lsoa_2011", "name"]
        cmd_tippecanoe(gpkg_fname, _LSOA, fields)
    else:
        logging.info(f"Not regenerating existing {gpkg_fname}")


def export_msoa(pg_conn, pg_uri: str, gpkg_fname: str, regenerate: bool):
    if regenerate or not os.path.isfile(gpkg_fname):
        if command_to_gpkg(
            pg_conn, pg_uri, gpkg_fname, _MSOA,
            src_srs=4326, dst_srs=4326,
            overwrite=True,
            command="""
                SELECT
                    msoa_2011,
                    name,
                    geom_4326 AS geom,
                    ST_AsGeoJSON(geom_4326) AS geom_str
                FROM census_boundaries.msoa"""
        ) is not None:
            raise RuntimeError(f"Error running ogr2ogr")
    else:
        logging.info(f"Not regenerating existing {gpkg_fname}")


def export_parish(pg_conn, pg_uri: str, gpkg_fname: str, regenerate: bool):
    if regenerate or not os.path.isfile(gpkg_fname):
        if command_to_gpkg(
            pg_conn, pg_uri, gpkg_fname, _PARISH,
            src_srs=4326, dst_srs=4326,
            overwrite=True,
            command="""
                SELECT
                    parish,
                    name,
                    geom_4326 AS geom,
                    ST_AsGeoJSON(geom_4326) AS geom_str
                FROM boundaryline.parish"""
        ) is not None:
            raise RuntimeError(f"Error running ogr2ogr")
    else:
        logging.info(f"Not regenerating existing {gpkg_fname}")
