from albion_models.db_funcs import command_to_gpkg


def export(pg_conn, pg_uri: str, gpkg: str, os_run_id: int, job_id: int):
    """
    Export data needed for Building in
    https://github.com/cse-bristol/710-open-solar-webapp/blob/master/opensolar/backend/models.py
    :param pg_conn:
    :param pg_uri:
    :param gpkg: file name of gpkg file to add data to (or create if doesn't exist yet)
    :param os_run_id: Run to export from (no check is done that that job_id is from this run, just used in the o/p)
    :param job_id: Job to export from
    """
    # TODO: add EPC data, maybe other things?
    command_to_gpkg(
        pg_conn, pg_uri, gpkg, "buildings",
        src_srs=4326, dst_srs=4326,
        command="""
        SELECT 
            b.toid, 
            b.postcode,
            b.addresses,
            pvb.exclusion_reason,
            pvb.height,
            b.is_residential,
            b.heating_fuel,
            b.heating_system,
            b.has_rooftop_pv,
            b.pv_roof_area_pct,
            b.pv_peak_power,
            b.listed_building_grade,
            b.msoa_2011,  
            b.lsoa_2011,  
            b.oa_2011, 
            b.ward, 
            b.ward_name,
            b.la,
            b.la_name,
            b.geom_4326
        FROM
            models.job_queue q
            LEFT JOIN models.open_solar_jobs osj ON osj.job_id = q.job_id
            LEFT JOIN models.pv_building bpv ON bpv.job_id = osj.job_id
            LEFT JOIN aggregates.building b ON b.toid = pvb.toid
        WHERE osj.os_run_id = %(os_run_id)s
        """,
        os_run_id=os_run_id)
