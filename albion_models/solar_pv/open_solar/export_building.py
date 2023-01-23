import logging

from psycopg2.sql import Identifier, Literal

from albion_models.db_funcs import command_to_gpkg, sql_command
from albion_models.solar_pv import tables
from albion_models.solar_pv.open_solar.mapshaper import ms_simplify

L_BUILDINGS = "buildings"


def export(pg_conn, pg_uri: str, gpkg_fname: str, os_run_id: int, job_id: int):
    """
    Export data needed for Building in
    https://github.com/cse-bristol/710-open-solar-webapp/blob/master/opensolar/backend/models.py
    :param pg_conn:
    :param pg_uri:
    :param gpkg_fname: file name of gpkg file to add data to (or create if doesn't exist yet)
    :param os_run_id: Run to export from (no check is done that that job_id is from this run, just used in the o/p)
    :param job_id: Job to export from
    """
    simplified_building_geoms_tbl: Identifier = \
        Identifier("models", f"{tables.SIMPLIFIED_BUILDING_GEOM_TABLE}_{job_id}")

    # Get simplified versions of the building geometries for job_id in a temporary table
    ms_simplify(
        pg_conn,
        simplified_building_geoms_tbl,
        "FROM models.pv_building mpb "
        "JOIN mastermap.building mb USING (toid) "
        "WHERE mpb.job_id = %(job_id)s ",
        "toid",
        Identifier("mb", "geom_4326"),
        {"job_id": job_id})

    if command_to_gpkg(
        pg_conn, pg_uri, gpkg_fname, "%s" % L_BUILDINGS,
        src_srs=4326, dst_srs=4326,
        overwrite=True,
        command=
        "WITH cte AS (SELECT toid, SUM(kwp) AS kwp, SUM(kwh_year) AS kwh "
        " FROM models.pv_panel WHERE job_id = {job_id} GROUP BY toid) "
        "SELECT "
        " {os_run_id} AS run_id, "
        " mp.job_id AS job_id, "
        " toid AS toid, "
        # ensure panels are aligned with buildings by putting them through the same transformation:
        """
        ST_Transform(mb.geom_27700,
         '+proj=tmerc +lat_0=49 +lon_0=-2 +k=0.9996012717 +x_0=400000 '
         '+y_0=-100000 +datum=OSGB36 +nadgrids=OSTN15_NTv2_OSGBtoETRS.gsb +units=m +no_defs',
         4326) AS geom,
        """
        " bt.address as address, "
        " bt.postcode as postcode, "
        """
        CASE 
            WHEN ab.num_dom_epcs >= ab.num_epc_certs / 2 THEN true
            WHEN ab.num_non_dom_epcs + ab.num_decs > ab.num_epc_certs / 2 THEN false
        ELSE NULL END AS is_residential,
        """
        " ab.has_rooftop_pv AS has_rooftop_pv, "                # Derived from EPC and pv_installations dataset
        " ab.pv_roof_area_pct AS pv_rooftop_area_pct, "         # EPC
        " ab.pv_peak_power AS pv_peak_power, "                  # EPC
        " ab.listed_building_grade AS listed_building_grade, "  # Derived from Historic England listed buildings dataset.
        " cte.kwh AS total_avg_energy_prod_kwh_per_year, "
        " ab.la AS la_code, "                                   # Derived using OSMM and OS BoundaryLine (Open government license)
        " ab.msoa_2011 AS msoa_2011, "                          # Derived using OSMM and census_boundaries (Open government license)
        " ab.lsoa_2011 AS lsoa_2011, "                          # Derived using OSMM and census_boundaries (Open government license). Can be null if OA is not in ONSPD
        " ab.oa_2011 AS oa_2011, "                              # Derived using OSMM and census_boundaries (Open government license)
        " ab.ward AS ward, "                                    # Derived using OSMM and OS BoundaryLine (Open government license)
        " ab.parish AS parish, "                                # Derived using OSMM and OS BoundaryLine (Open government license)
        " ST_AsGeoJSON(ab.geom_4326) AS geom_str, "
        " ST_X(ab.centroid) AS lon, "
        " ST_Y(ab.centroid) AS lat, "
        " ST_X(ST_Transform(ab.centroid, 27700)) AS easting, "
        " ST_Y(ST_Transform(ab.centroid, 27700)) AS northing, "
        " ST_AsGeoJSON(ab.centroid) AS centroid_str, "
        " mp.height AS height, "
        " tt.geojson AS geom_str_simplified, "
        " CASE "
        "  WHEN cte.kwp = 0 THEN 0 "
        "  ELSE cte.kwh / cte.kwp "
        " END AS kwh_per_kwp, "
        " mp.exclusion_reason AS exclusion_reason "
        "FROM aggregates.building ab "
        "LEFT JOIN paf.by_toid bt USING (toid) "
        "JOIN mastermap.building_27700 mb USING (toid) "
        "JOIN models.pv_building mp USING (toid) "
        "LEFT JOIN cte USING (toid) "
        "JOIN {simp_table} tt ON (tt.id = toid) "
        "WHERE mp.job_id = {job_id} ",
        job_id=Literal(job_id),
        os_run_id=Literal(os_run_id),
        simp_table=simplified_building_geoms_tbl,
    ) is not None:
        raise RuntimeError(f"Error running ogr2ogr")

    sql_command(
        pg_conn,
        "DROP TABLE {simp_table}",
        simp_table=simplified_building_geoms_tbl
    )
