
ALTER TABLE {solar_pv} ADD COLUMN kwh_m01 double precision;
ALTER TABLE {solar_pv} ADD COLUMN kwh_m02 double precision;
ALTER TABLE {solar_pv} ADD COLUMN kwh_m03 double precision;
ALTER TABLE {solar_pv} ADD COLUMN kwh_m04 double precision;
ALTER TABLE {solar_pv} ADD COLUMN kwh_m05 double precision;
ALTER TABLE {solar_pv} ADD COLUMN kwh_m06 double precision;
ALTER TABLE {solar_pv} ADD COLUMN kwh_m07 double precision;
ALTER TABLE {solar_pv} ADD COLUMN kwh_m08 double precision;
ALTER TABLE {solar_pv} ADD COLUMN kwh_m09 double precision;
ALTER TABLE {solar_pv} ADD COLUMN kwh_m10 double precision;
ALTER TABLE {solar_pv} ADD COLUMN kwh_m11 double precision;
ALTER TABLE {solar_pv} ADD COLUMN kwh_m12 double precision;
--ALTER TABLE {solar_pv} ADD COLUMN horizon double precision[];
ALTER TABLE {solar_pv} RENAME COLUMN val TO kwh;

UPDATE {solar_pv} sp SET kwh_m01 = m.val * 0.001 * 31 FROM {m01} m WHERE sp.x = m.x AND sp.y = m.y;
UPDATE {solar_pv} sp SET kwh_m02 = m.val * 0.001 * 28 FROM {m02} m WHERE sp.x = m.x AND sp.y = m.y;
UPDATE {solar_pv} sp SET kwh_m03 = m.val * 0.001 * 31 FROM {m03} m WHERE sp.x = m.x AND sp.y = m.y;
UPDATE {solar_pv} sp SET kwh_m04 = m.val * 0.001 * 30 FROM {m04} m WHERE sp.x = m.x AND sp.y = m.y;
UPDATE {solar_pv} sp SET kwh_m05 = m.val * 0.001 * 31 FROM {m05} m WHERE sp.x = m.x AND sp.y = m.y;
UPDATE {solar_pv} sp SET kwh_m06 = m.val * 0.001 * 30 FROM {m06} m WHERE sp.x = m.x AND sp.y = m.y;
UPDATE {solar_pv} sp SET kwh_m07 = m.val * 0.001 * 31 FROM {m07} m WHERE sp.x = m.x AND sp.y = m.y;
UPDATE {solar_pv} sp SET kwh_m08 = m.val * 0.001 * 31 FROM {m08} m WHERE sp.x = m.x AND sp.y = m.y;
UPDATE {solar_pv} sp SET kwh_m09 = m.val * 0.001 * 30 FROM {m09} m WHERE sp.x = m.x AND sp.y = m.y;
UPDATE {solar_pv} sp SET kwh_m10 = m.val * 0.001 * 31 FROM {m10} m WHERE sp.x = m.x AND sp.y = m.y;
UPDATE {solar_pv} sp SET kwh_m11 = m.val * 0.001 * 30 FROM {m11} m WHERE sp.x = m.x AND sp.y = m.y;
UPDATE {solar_pv} sp SET kwh_m12 = m.val * 0.001 * 31 FROM {m12} m WHERE sp.x = m.x AND sp.y = m.y;

--UPDATE {solar_pv} sp SET horizon = h.val FROM {h01} h WHERE sp.x = h.x AND sp.y = h.y;

DROP TABLE {m01};
DROP TABLE {m02};
DROP TABLE {m03};
DROP TABLE {m04};
DROP TABLE {m05};
DROP TABLE {m06};
DROP TABLE {m07};
DROP TABLE {m08};
DROP TABLE {m09};
DROP TABLE {m10};
DROP TABLE {m11};
DROP TABLE {m12};

ALTER TABLE {solar_pv} ADD COLUMN en geometry(Point, 27700);
UPDATE {solar_pv} p SET en = ST_Transform(ST_SetSRID(ST_MakePoint(p.x,p.y), 4326), 27700);
CREATE INDEX ON {solar_pv} USING GIST (en);

ALTER TABLE {solar_pv} ADD COLUMN pixel geometry(Polygon, 27700);
UPDATE {solar_pv} p SET pixel = ST_Buffer(p.en, {res} / 2.0, 'endcap=square');
CREATE INDEX ON {solar_pv} USING GIST (pixel);


-- These are the fields needed by the cost-benefit model:
-- pv.toid
-- pv.job_id
-- pv.peak_power
-- pv.kwh_year
-- pv.roof_plane_id
-- pv.roof_geom_4326
-- pv.area

COMMIT;
START TRANSACTION;

-- do a spatial join with all {solar_pv} pixels that intersect each
-- panel installation, work out what proportion of the pixel overlaps
-- the installation, and add up the kWh to get kWh of installation:
DROP TABLE IF EXISTS {panel_kwh};
CREATE TABLE {panel_kwh} AS
SELECT
    pp.toid,
    pp.roof_plane_id,
    -- sum of the kwh of each pixel that intersects the panel,
    -- multiplied by the proportion of the pixel that intersects the panel.
    -- PVMAPS produces kWh values per pixel as if a pixel was a 1kWp panel
    -- so the values are adjusted accordingly
    SUM((ST_Area(ST_Intersection(pixel, pp.panel_geom_27700)) / ({res} * {res})) * pv.kwh) * %(peak_power_per_m2)s AS kwh_year,

    SUM((ST_Area(ST_Intersection(pixel, pp.panel_geom_27700)) / ({res} * {res})) * pv.kwh_m01) * %(peak_power_per_m2)s AS kwh_m01,
    SUM((ST_Area(ST_Intersection(pixel, pp.panel_geom_27700)) / ({res} * {res})) * pv.kwh_m02) * %(peak_power_per_m2)s AS kwh_m02,
    SUM((ST_Area(ST_Intersection(pixel, pp.panel_geom_27700)) / ({res} * {res})) * pv.kwh_m03) * %(peak_power_per_m2)s AS kwh_m03,
    SUM((ST_Area(ST_Intersection(pixel, pp.panel_geom_27700)) / ({res} * {res})) * pv.kwh_m04) * %(peak_power_per_m2)s AS kwh_m04,
    SUM((ST_Area(ST_Intersection(pixel, pp.panel_geom_27700)) / ({res} * {res})) * pv.kwh_m05) * %(peak_power_per_m2)s AS kwh_m05,
    SUM((ST_Area(ST_Intersection(pixel, pp.panel_geom_27700)) / ({res} * {res})) * pv.kwh_m06) * %(peak_power_per_m2)s AS kwh_m06,
    SUM((ST_Area(ST_Intersection(pixel, pp.panel_geom_27700)) / ({res} * {res})) * pv.kwh_m07) * %(peak_power_per_m2)s AS kwh_m07,
    SUM((ST_Area(ST_Intersection(pixel, pp.panel_geom_27700)) / ({res} * {res})) * pv.kwh_m08) * %(peak_power_per_m2)s AS kwh_m08,
    SUM((ST_Area(ST_Intersection(pixel, pp.panel_geom_27700)) / ({res} * {res})) * pv.kwh_m09) * %(peak_power_per_m2)s AS kwh_m09,
    SUM((ST_Area(ST_Intersection(pixel, pp.panel_geom_27700)) / ({res} * {res})) * pv.kwh_m10) * %(peak_power_per_m2)s AS kwh_m10,
    SUM((ST_Area(ST_Intersection(pixel, pp.panel_geom_27700)) / ({res} * {res})) * pv.kwh_m11) * %(peak_power_per_m2)s AS kwh_m11,
    SUM((ST_Area(ST_Intersection(pixel, pp.panel_geom_27700)) / ({res} * {res})) * pv.kwh_m12) * %(peak_power_per_m2)s AS kwh_m12
FROM
    {panel_polygons} pp
    LEFT JOIN {solar_pv} pv ON ST_Intersects(pixel, pp.panel_geom_27700)
GROUP BY pp.roof_plane_id;

ALTER TABLE {panel_kwh} ADD PRIMARY KEY (roof_plane_id);

COMMIT;
START TRANSACTION;

INSERT INTO models.solar_pv
SELECT
    pp.toid,
    pp.roof_plane_id,
    %(job_id)s AS job_id,
    ST_SetSrid(ST_Transform(pp.panel_geom_27700, 4326), 4326)::geometry(multipolygon, 4326) AS panel_geom_4326,
    kwh_m01,
    kwh_m02,
    kwh_m03,
    kwh_m04,
    kwh_m05,
    kwh_m06,
    kwh_m07,
    kwh_m08,
    kwh_m09,
    kwh_m10,
    kwh_m11,
    kwh_m12
    kwh.kwh_year,
    pp.area * %(peak_power_per_m2)s AS kwp,
    pp.slope,
    pp.aspect,
    pp.area,
    pp.footprint,
    pp.x_coef,
    pp.y_coef,
    pp.intercept,
    pp.is_flat
FROM
    {panel_polygons} pp
    LEFT JOIN {panel_kwh} kwh ON kwh.roof_plane_id = pp.roof_plane_id;

DROP TABLE {panel_kwh};
DROP TABLE {solar_pv};

CREATE OR REPLACE VIEW models.{job_view} AS
SELECT * FROM models.solar_pv WHERE job_id = %(job_id)s;

INSERT INTO models.pv_building_exclusions
SELECT %(job_id)s, toid, exclusion_reason
FROM {building_exclusion_reasons};
