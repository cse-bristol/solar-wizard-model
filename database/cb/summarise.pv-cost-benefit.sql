-- adapted from https://github.com/cse-bristol/781-liverpool/blob/master/src/pv.R
-- 1. define IRR bands from 0 - 0.3, size 0.01
-- 2. (output 2) extract summary data for each (IRR band, tenure, elec_cost), 1 table per tenure (and 1 for across all tenures), see L87-102

INSERT INTO models.pv_cb_report (
    job_id,
    irr_band,
    electricity_kwh_cost,
    main_tenure,
    mean_irr,
    mw,
    gwh,
    installations,
    total_capex_k,
    average_capex_k,
    total_npv_k,
    average_npv_k,
    cumulative_mw,
    cumulative_gwh,
    cumulative_installations
)
WITH irr_bands AS (
    -- Generate 30 ranges, from [0.00,0.01) to [0.29,)
    SELECT
        numrange(round(a / 100.0, 2), round(a / 100.0 + 0.01, 2), '[)') AS irr_band
    FROM generate_series(0, 28) AS s(a)
    UNION
    SELECT numrange(0.29, NULL, '[)')
    ORDER BY irr_band
)
SELECT
    %(job_id)s AS job_id,
    irr_bands.irr_band AS irr_band,
    cb.electricity_kwh_cost,
    %(main_tenure)s AS main_tenure,
    AVG(cb.irr) AS mean_irr,
    SUM(cb.mw) AS mw,
    SUM(cb.gwh) AS gwh,
    COUNT(*) AS installations,
    SUM(cb.capex_k) As total_capex_k,
    AVG(cb.capex_k) As average_capex_k,
    SUM(cb.npv_k) As total_npv_k,
    AVG(cb.npv_k) As average_npv_k,
    SUM(SUM(cb.mw)) OVER w_cost AS cumulative_mw,
    SUM(SUM(cb.gwh)) OVER w_cost AS cumulative_gwh,
    SUM(COUNT(*)) OVER w_cost AS cumulative_installations
FROM
    irr_bands
    LEFT JOIN models.pv_cb_best_irr cb ON irr_bands.irr_band @> cb.irr::numeric
WHERE
    cb.job_id = %(job_id)s
    AND cb.irr > 0
    AND (main_tenure = %(main_tenure)s OR %(main_tenure)s = 'All' )
GROUP BY irr_band, electricity_kwh_cost
WINDOW w_cost AS (PARTITION BY electricity_kwh_cost ORDER BY irr_band DESC);
