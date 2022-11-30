import logging

from psycopg2.sql import Identifier, Literal

from albion_models.db_funcs import command_to_gpkg
from albion_models.ogr_helpers import get_layer_names
from albion_models.solar_pv import tables
from albion_models.solar_pv.open_solar.mapshaper import ms_simplify

_BUILDINGS = "buildings"


def export(pg_conn, pg_uri: str, gpkg_fname: str, os_run_id: int, job_id: int, regenerate: bool):
    """
    Export data needed for Building in
    https://github.com/cse-bristol/710-open-solar-webapp/blob/master/opensolar/backend/models.py
    :param pg_conn:
    :param pg_uri:
    :param gpkg_fname: file name of gpkg file to add data to (or create if doesn't exist yet)
    :param os_run_id: Run to export from (no check is done that that job_id is from this run, just used in the o/p)
    :param job_id: Job to export from
    """
    if regenerate or _BUILDINGS not in get_layer_names(gpkg_fname):
        # Get simplified versions of the building geometries for job_id in a temporary table
        ms_simplify(
            pg_conn,
            Identifier(tables.schema(job_id), tables.SIMPLIFIED_BUILDING_GEOM_TABLE),
            "FROM models.pv_building mpb "
            "JOIN mastermap.building mb USING (toid) "
            "WHERE mpb.job_id = %(job_id)s ",
            "toid",
            Identifier("mb", "geom_4326"),
            {"job_id": job_id})

        if command_to_gpkg(
            pg_conn, pg_uri, gpkg_fname, "%s" % _BUILDINGS,
            src_srs=4326, dst_srs=4326,
            overwrite=True,
            command=
            "WITH cte AS (SELECT toid, SUM(kwp) AS kwp, SUM(kwh_year) AS kwh "
            " FROM models.pv_panel WHERE job_id = {job_id} GROUP BY toid) "
            "SELECT "
            " {os_run_id} AS run_id, "
            " mp.job_id AS job_id, "
            " toid AS toid, "
            " ab.geom_4326 AS geom, "
            " ab.is_residential AS is_residential, "                # aggregates.building.is_residential (+3 other tables) Derived from AddressBase class (True if class is one of 'RD', 'RH', 'RI')
            " ab.has_rooftop_pv AS has_rooftop_pv, "                # aggregates.building.has_rooftop_pv (+3 other tables) Derived from EPC and pv_installations dataset
            " ab.pv_roof_area_pct AS pv_rooftop_area_pct, "         # aggregates.building.pv_roof_area_pct (+4 other tables) PV roof area % coverage (derived from photo_supply)
            " ab.pv_peak_power AS pv_peak_power, "                  # aggregates.building.pv_peak_power (+4 other tables) PV peak power, kWp (derived from photo_supply)
            " ab.listed_building_grade AS listed_building_grade, "  # aggregates.building.listed_building_grade (+3 other tables) Derived from Historic England listed buildings dataset.
            " cte.kwh AS total_avg_energy_prod_kwh_per_year, "
            " ab.la AS la_code, "                                   # aggregates.building.la Derived using ABP and OS BoundaryLine (Open government license)
            " ab.lsoa_2011 AS lsoa_2011, "                          # aggregates.building.lsoa_2011 Derived using ABP, ONSPD and census_boundaries (Open government license). Can be null if the OA is not in ONSPD
            " ST_AsGeoJSON(ab.geom_4326) AS geom_str, "
            " ST_X(ab.centroid) AS lon, "
            " ST_Y(ab.centroid) AS lat, "
            " ST_X(ST_Transform(ab.centroid, 27700)) AS easting, "
            " ST_Y(ST_Transform(ab.centroid, 27700)) AS northing, "
            " ab.centroid AS centroid, "
            " ST_AsGeoJSON(ab.centroid) AS centroid_str, "
            " ab.height AS height, "
            " tt.geojson AS geom_str_simplified, "
            " ST_Area(ab.geom_4326) AS footprint, "
            " CASE "
            "  WHEN cte.kwp = 0 THEN 0 "
            "  ELSE cte.kwh / cte.kwp "
            " END AS kwh_per_kwp "
            "FROM aggregates.building ab "
            "JOIN models.pv_building mp USING (toid) "
            "JOIN cte USING (toid) "
            "JOIN {simp_table} tt ON (tt.id = toid) "
            "WHERE mp.job_id = {job_id} ",
            job_id=Literal(job_id),
            os_run_id=Literal(os_run_id),
            simp_table=Identifier(tables.schema(job_id), tables.SIMPLIFIED_BUILDING_GEOM_TABLE),
        ) is not None:
            raise RuntimeError(f"Error running ogr2ogr")
    else:
        logging.info(f"Not regenerating existing {gpkg_fname}")
