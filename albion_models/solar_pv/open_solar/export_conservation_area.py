import logging
import os

from albion_models.db_funcs import command_to_gpkg

_CONS_AREAS = "conservation_areas"


def export(pg_conn, pg_uri: str, gpkg_fname: str, regenerate: bool):
    if regenerate or not os.path.isfile(gpkg_fname):
        if command_to_gpkg(
            pg_conn, pg_uri, gpkg_fname, _CONS_AREAS,
            src_srs=27700, dst_srs=27700,
            overwrite=True,
            command=
            "SELECT "
            " SUBSTR(country::text, 1, 1) || ':' || uid AS uid, "
            " name, "
            " date_of_designation, "
            " date_updated, "
            " capture_scale, "
            " local_planning_authority, "
            " geom_27700, "
            " ST_AsGeoJSON(geom_27700) as geom_str "
            "FROM conservation_areas.conservation_areas"
        ) is not None:
            raise RuntimeError(f"Error running ogr2ogr")
    else:
        logging.info(f"Not regenerating existing {gpkg_fname}")
