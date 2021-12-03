

INSERT INTO models.pv_cost_benefit (
    job_id,
    solar_pv_job_id,
    period_years,
    discount_rate,
    electricity_kwh_cost,
    toid,
    installation_job_id,
    roof_plane_ids,
    peak_power,
    usable_area,
    total_yield_kwh_year,
    yield_kwh_m2_year,
    installation_cost,
    geom_4326)
SELECT
    %(job_id)s,
    %(solar_pv_job_id)s,
    %(period_years)s,
    %(discount_rate)s,
    %(electricity_kwh_cost)s,
    pv.toid,
    row_number() OVER w AS installation_job_id,
    array_agg(pv.roof_plane_id) OVER w AS roof_plane_ids,
    SUM(pv.peak_power) OVER w AS peak_power,
    SUM(pv.area) OVER w AS usable_area,
    SUM(pv.total_avg_energy_prod_kwh_per_year) OVER w AS total_yield_kwh_year,
    SUM(pv.total_avg_energy_prod_kwh_per_year) OVER w / SUM(pv.area) OVER w AS yield_kwh_m2_year,
    CASE
        WHEN SUM(pv.peak_power) OVER w <= 10  THEN
            ((SUM(pv.peak_power) OVER w * %(small_inst_cost_per_kwp)s) + %(small_inst_fixed_cost)s) * (1 + %(small_inst_vat)s)
        WHEN SUM(pv.peak_power) OVER w <= 100 THEN
            ((SUM(pv.peak_power) OVER w * %(med_inst_cost_per_kwp)s)   + %(med_inst_fixed_cost)s)   * (1 + %(med_inst_vat)s)
        ELSE
            ((SUM(pv.peak_power) OVER w * %(large_inst_cost_per_kwp)s) + %(large_inst_fixed_cost)s) * (1 + %(large_inst_vat)s)
        END AS installation_cost,
    ST_Multi(ST_CollectionHomogenize(ST_Collect(pv.roof_geom_4326) OVER w)) AS geom_4326
FROM models.solar_pv pv
LEFT JOIN models.job_queue jq ON pv.job_id = jq.job_id
LEFT JOIN historic_england.listed_buildings lb USING (toid)
LEFT JOIN pv_installations.has_pv has_pv USING (toid)
WHERE pv.job_id = %(solar_pv_job_id)s
    AND (lb.grade IS NULL OR NOT %(exclude_listed)s)
    AND (has_pv.toid IS NULL OR NOT %(exclude_already_have_pv)s)
WINDOW w AS (
    PARTITION BY pv.toid
    ORDER BY pv.total_avg_energy_prod_kwh_per_year / pv.area DESC
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
