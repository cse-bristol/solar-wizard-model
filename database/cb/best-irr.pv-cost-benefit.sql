-- adapted from https://github.com/cse-bristol/781-liverpool/blob/master/src/pv.R
-- 1. select job with best IRR per toid/elec_cost
-- 2. join with experian tenure (most common per toid)
-- 3. (output 1) L107-126
-- See also `summarise.pv-cost-benefit.sql`


INSERT INTO models.pv_cb_best_irr (
    job_id,
    installation_job_id,
    roof_plane_ids,
    toid,
    address,
    postcode,
    main_tenure,
    main_class,
    mw,
    gwh,
    area_m2,
    kwh_m2_year,
    capex_k,
    electricity_kwh_cost,
    annual_revenue_k,
    npv_k,
    irr,
    irr_percentile,
    irr_rank,
    panel_geom_4326
)
WITH best_irr AS (
    SELECT DISTINCT ON (toid, electricity_kwh_cost) *
    FROM models.pv_cost_benefit cb
    WHERE cb.job_id = %(job_id)s
    ORDER BY toid, electricity_kwh_cost, irr DESC
),
tenure AS (
    SELECT
        toid,
        concat_ws(' ',
            MAX(pao),
            MAX(dependent_thoroughfare),
            MAX(thoroughfare),
            MAX(double_dependent_locality),
            MAX(dependent_locality),
            MAX(post_town),
            MAX(postcode)
        ) AS address,
        MAX(b.postcode) AS postcode,
        MODE() WITHIN GROUP (ORDER BY hh.tenure_type) AS main_tenure,
        MAX(b.most_common_classification) AS main_class
    FROM
        models.pv_cost_benefit cb
        LEFT JOIN building.building b USING (toid)
        LEFT JOIN experian.household hh ON hh.uprn = ANY(b.uprns)
    WHERE cb.job_id = %(job_id)s
    GROUP by toid
)
SELECT
    irr.job_id,
    irr.installation_job_id,
    irr.roof_plane_ids,
    irr.toid,
    t.address,
    t.postcode,
    CASE WHEN t.main_tenure IS NOT NULL THEN t.main_tenure
         WHEN SUBSTRING(t.main_class FOR 2) NOT IN ('RD', 'RH', 'RI') THEN 'Non-residential'
    END AS main_tenure,
    t.main_class,
    irr.peak_power / 1000.0 AS mw,
    irr.total_yield_kwh_year / 1000000.0 AS gwh,
    irr.usable_area AS area_m2,
    irr.yield_kwh_m2_year AS kwh_m2_year,
    irr.installation_cost / 1000.0 AS capex_k,
    irr.electricity_kwh_cost,
    (irr.total_yield_kwh_year / 1000000.0) * irr.electricity_kwh_cost * 1000.0 AS annual_revenue_k,
    irr.npv / 1000.0 AS npv_k,
    irr.irr,
    percent_rank() OVER (ORDER BY irr) AS irr_percentile,
    rank() OVER (ORDER BY irr DESC) AS irr_rank,
    irr.geom_4326 AS panel_geom_4326
FROM
    best_irr irr
    LEFT JOIN tenure t USING (toid);
