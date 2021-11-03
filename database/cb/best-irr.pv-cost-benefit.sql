-- adapted from https://github.com/cse-bristol/781-liverpool/blob/master/src/pv.R
-- 1. select job with best IRR per toid/elec_cost
-- 2. join with experian tenure (most common per toid)
-- 3. (output 1) L107-126
-- See also `summarise.pv-cost-benefit.sql`

--
-- Select one PV installation per toid:
--
CREATE TABLE {best_irr} AS
SELECT DISTINCT ON (toid, electricity_kwh_cost) *
FROM models.pv_cost_benefit cb
WHERE cb.job_id = %(job_id)s
ORDER BY toid, electricity_kwh_cost, irr DESC;

CREATE INDEX ON {best_irr} (toid);
CREATE INDEX ON {best_irr} (irr);

COMMIT;
START TRANSACTION;

--
-- Get tenure, abp class, address per toid:
--
CREATE TABLE {tenure} AS
SELECT
    toid,
    NULLIF(concat_ws(' ',
        (array_agg(a.pao))[1],
        (array_agg(a.dependent_thoroughfare))[1],
        (array_agg(a.thoroughfare))[1],
        (array_agg(a.double_dependent_locality))[1],
        (array_agg(a.dependent_locality))[1],
        (array_agg(a.post_town))[1],
        (array_agg(a.postcode))[1]
    ), '') AS address,
    MAX(a.postcode) AS postcode,
    MODE() WITHIN GROUP (ORDER BY hh.tenure_type) AS main_tenure,
    MODE() WITHIN GROUP (ORDER BY a.classification_code) AS main_class
FROM
    models.pv_cost_benefit cb
    LEFT JOIN addressbase.address a USING (toid)
    LEFT JOIN experian.household hh USING (uprn)
WHERE cb.job_id = %(job_id)s
GROUP by toid;

CREATE INDEX ON {tenure} (toid);

COMMIT;
START TRANSACTION;

--
-- Build outputs table:
--
INSERT INTO models.pv_cb_best_irr (
    job_id,
    installation_job_id,
    roof_plane_ids,
    toid,
    address,
    postcode,
    main_tenure,
    main_class,
    main_class_desc,
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
    panel_geom_4326,
    already_has_pv,
    listed_building_grade
)
SELECT
    irr.job_id,
    irr.installation_job_id,
    irr.roof_plane_ids,
    irr.toid,
    t.address,
    t.postcode,
    CASE WHEN t.main_tenure IS NOT NULL THEN t.main_tenure::text
         WHEN SUBSTRING(t.main_class FOR 2) NOT IN ('RD', 'RH', 'RI') THEN 'Non-residential'
         ELSE 'Unknown'
    END AS main_tenure,
    t.main_class,
    concat_ws(' : ',
        primary_description,
        secondary_description,
        tertiary_description,
        quaternary_description) AS main_class_desc,
    irr.peak_power / 1000.0 AS mw,
    irr.total_yield_kwh_year / 1000000.0 AS gwh,
    irr.usable_area AS area_m2,
    irr.yield_kwh_m2_year AS kwh_m2_year,
    irr.installation_cost / 1000.0 AS capex_k,
    irr.electricity_kwh_cost,
    (irr.total_yield_kwh_year / 1000000.0) * irr.electricity_kwh_cost * 1000.0 AS annual_revenue_k,
    irr.npv / 1000.0 AS npv_k,
    irr.irr,
    percent_rank() OVER (PARTITION BY electricity_kwh_cost ORDER BY irr) AS irr_percentile,
    rank() OVER (PARTITION BY electricity_kwh_cost ORDER BY irr DESC) AS irr_rank,
    irr.geom_4326 AS panel_geom_4326,
    pv.toid IS NOT NULL AS already_has_pv,
    lb.grade::text AS listed_building_grade
FROM
    {best_irr} irr
    LEFT JOIN {tenure} t USING (toid)
    LEFT JOIN historic_england.listed_buildings lb USING (toid)
    LEFT JOIN pv_installations.has_pv pv USING (toid)
    LEFT JOIN addressbase.classification_scheme cs ON t.main_class = cs.concatenated_classification;

--
-- Cleanup
--

DROP TABLE {best_irr};
DROP TABLE {tenure};
