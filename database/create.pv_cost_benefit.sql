

-- TODO handle usable area param
-- TODO query params for installation cost params
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
    installation_cost)
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
    SUM(pv.total_avg_energy_prod_kwh_per_year / pv.area) OVER w AS yield_kwh_m2_year,
    CASE
        WHEN SUM(pv.peak_power) OVER w <= 10  THEN SUM(pv.peak_power) OVER w * 1429 * 1.05
        WHEN SUM(pv.peak_power) OVER w <= 100 THEN SUM(pv.peak_power) OVER w *  922 * 1.20
        ELSE                                       SUM(pv.peak_power) OVER w *  714 * 1.20 END
FROM models.solar_pv pv
WHERE pv.job_id = %(solar_pv_job_id)s
WINDOW w AS (
    PARTITION BY pv.toid
    ORDER BY pv.total_avg_energy_prod_kwh_per_year / pv.area DESC
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
