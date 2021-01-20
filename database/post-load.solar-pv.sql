
ALTER TABLE {solar_pv} DROP COLUMN jan_month;
ALTER TABLE {solar_pv} DROP COLUMN feb_month;
ALTER TABLE {solar_pv} DROP COLUMN mar_month;
ALTER TABLE {solar_pv} DROP COLUMN apr_month;
ALTER TABLE {solar_pv} DROP COLUMN may_month;
ALTER TABLE {solar_pv} DROP COLUMN jun_month;
ALTER TABLE {solar_pv} DROP COLUMN jul_month;
ALTER TABLE {solar_pv} DROP COLUMN aug_month;
ALTER TABLE {solar_pv} DROP COLUMN sep_month;
ALTER TABLE {solar_pv} DROP COLUMN oct_month;
ALTER TABLE {solar_pv} DROP COLUMN nov_month;
ALTER TABLE {solar_pv} DROP COLUMN dec_month;

ALTER TABLE {solar_pv} ALTER COLUMN spectral_loss_percentage SET DATA TYPE real USING
CASE
    WHEN spectral_loss_percentage = '?(0)' THEN null
    ELSE spectral_loss_percentage::real
END;

INSERT INTO models.solar_pv
SELECT
    pv.jan_avg_energy_prod_kwh_per_day,
    pv.jan_avg_energy_prod_kwh_per_month,
    pv.jan_avg_irr_kwh_per_m2_per_day,
    pv.jan_avg_irr_kwh_per_m2_per_month,
    pv.jan_energy_std_dev_m,
    pv.feb_avg_energy_prod_kwh_per_day,
    pv.feb_avg_energy_prod_kwh_per_month,
    pv.feb_avg_irr_kwh_per_m2_per_day,
    pv.feb_avg_irr_kwh_per_m2_per_month,
    pv.feb_energy_std_dev_m,
    pv.mar_avg_energy_prod_kwh_per_day,
    pv.mar_avg_energy_prod_kwh_per_month,
    pv.mar_avg_irr_kwh_per_m2_per_day,
    pv.mar_avg_irr_kwh_per_m2_per_month,
    pv.mar_energy_std_dev_m,
    pv.apr_avg_energy_prod_kwh_per_day,
    pv.apr_avg_energy_prod_kwh_per_month,
    pv.apr_avg_irr_kwh_per_m2_per_day,
    pv.apr_avg_irr_kwh_per_m2_per_month,
    pv.apr_energy_std_dev_m,
    pv.may_avg_energy_prod_kwh_per_day,
    pv.may_avg_energy_prod_kwh_per_month,
    pv.may_avg_irr_kwh_per_m2_per_day,
    pv.may_avg_irr_kwh_per_m2_per_month,
    pv.may_energy_std_dev_m,
    pv.jun_avg_energy_prod_kwh_per_day,
    pv.jun_avg_energy_prod_kwh_per_month,
    pv.jun_avg_irr_kwh_per_m2_per_day,
    pv.jun_avg_irr_kwh_per_m2_per_month,
    pv.jun_energy_std_dev_m,
    pv.jul_avg_energy_prod_kwh_per_day,
    pv.jul_avg_energy_prod_kwh_per_month,
    pv.jul_avg_irr_kwh_per_m2_per_day,
    pv.jul_avg_irr_kwh_per_m2_per_month,
    pv.jul_energy_std_dev_m,
    pv.aug_avg_energy_prod_kwh_per_day,
    pv.aug_avg_energy_prod_kwh_per_month,
    pv.aug_avg_irr_kwh_per_m2_per_day,
    pv.aug_avg_irr_kwh_per_m2_per_month,
    pv.aug_energy_std_dev_m,
    pv.sep_avg_energy_prod_kwh_per_day,
    pv.sep_avg_energy_prod_kwh_per_month,
    pv.sep_avg_irr_kwh_per_m2_per_day,
    pv.sep_avg_irr_kwh_per_m2_per_month,
    pv.sep_energy_std_dev_m,
    pv.oct_avg_energy_prod_kwh_per_day,
    pv.oct_avg_energy_prod_kwh_per_month,
    pv.oct_avg_irr_kwh_per_m2_per_day,
    pv.oct_avg_irr_kwh_per_m2_per_month,
    pv.oct_energy_std_dev_m,
    pv.nov_avg_energy_prod_kwh_per_day,
    pv.nov_avg_energy_prod_kwh_per_month,
    pv.nov_avg_irr_kwh_per_m2_per_day,
    pv.nov_avg_irr_kwh_per_m2_per_month,
    pv.nov_energy_std_dev_m,
    pv.dec_avg_energy_prod_kwh_per_day,
    pv.dec_avg_energy_prod_kwh_per_month,
    pv.dec_avg_irr_kwh_per_m2_per_day,
    pv.dec_avg_irr_kwh_per_m2_per_month,
    pv.dec_energy_std_dev_m,
    pv.total_avg_energy_prod_kwh_per_day,
    pv.total_avg_energy_prod_kwh_per_month,
    pv.total_avg_energy_prod_kwh_per_year,
    pv.total_avg_irr_kwh_per_m2_per_day,
    pv.total_avg_irr_kwh_per_m2_per_month,
    pv.total_avg_irr_kwh_per_m2_per_year,
    pv.total_energy_std_dev_m,
    pv.total_energy_std_dev_y,
    pv.aoi_loss_percentage,
    pv.spectral_loss_percentage,
    pv.temp_irr_loss_percentage,
    pv.total_loss_percentage,
    pv.easting,
    pv.northing,
    pv.toid,
    pv.roof_id,
    pv.peak_power,
    ST_SetSrid(ST_Transform(h.roof_geom_27700, 4326), 4326)::geometry(polygon, 4326) AS roof_geom_4326,
    h.slope,
    h.aspect,
    h.sky_view_factor,
    h.percent_visible,
    h.area,
    h.footprint,
    %(job_id)s AS job_id,
    pv.horizon_sd,
    pv.southerly_horizon_sd
FROM {solar_pv} pv
LEFT JOIN {roof_horizons} h ON pv.roof_id = h.roof_id;

DROP TABLE {solar_pv};

CREATE OR REPLACE VIEW models.{job_view} AS
SELECT * FROM models.solar_pv WHERE job_id = %(job_id)s;