
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
    pv.*,
    ST_SetSrid(ST_Transform(h.roof_geom_27700, 4326), 4326)::geometry(polygon, 4326) AS roof_geom_4326,
    h.slope,
    h.aspect,
    h.sky_view_factor,
    h.percent_visible,
    h.area,
    h.footprint,
    %(job_id)s AS job_id
FROM {solar_pv} pv
LEFT JOIN {roof_horizons} h ON pv.roof_id = h.roof_id;

DROP TABLE {solar_pv};

CREATE VIEW models.{job_view} AS
SELECT * FROM models.solar_pv WHERE job_id = %(job_id)s;