
-- TODO need to add a db migration in 320-albion-webapp repo which changes the shape of models.solar_pv
--  maybe move the existing table to somewhere like solar_pv_old?
-- TODO also update the PV table creation in create.db.sql script in 320-albion-webapp


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

UPDATE {solar_pv} sp SET kwh_m01 = m.kwh FROM {m01} m WHERE lp.lon = m.lon AND lp.lat = m.lat;
UPDATE {solar_pv} sp SET kwh_m02 = m.kwh FROM {m02} m WHERE lp.lon = m.lon AND lp.lat = m.lat;
UPDATE {solar_pv} sp SET kwh_m03 = m.kwh FROM {m03} m WHERE lp.lon = m.lon AND lp.lat = m.lat;
UPDATE {solar_pv} sp SET kwh_m04 = m.kwh FROM {m04} m WHERE lp.lon = m.lon AND lp.lat = m.lat;
UPDATE {solar_pv} sp SET kwh_m05 = m.kwh FROM {m05} m WHERE lp.lon = m.lon AND lp.lat = m.lat;
UPDATE {solar_pv} sp SET kwh_m06 = m.kwh FROM {m06} m WHERE lp.lon = m.lon AND lp.lat = m.lat;
UPDATE {solar_pv} sp SET kwh_m07 = m.kwh FROM {m07} m WHERE lp.lon = m.lon AND lp.lat = m.lat;
UPDATE {solar_pv} sp SET kwh_m08 = m.kwh FROM {m08} m WHERE lp.lon = m.lon AND lp.lat = m.lat;
UPDATE {solar_pv} sp SET kwh_m09 = m.kwh FROM {m09} m WHERE lp.lon = m.lon AND lp.lat = m.lat;
UPDATE {solar_pv} sp SET kwh_m10 = m.kwh FROM {m10} m WHERE lp.lon = m.lon AND lp.lat = m.lat;
UPDATE {solar_pv} sp SET kwh_m11 = m.kwh FROM {m11} m WHERE lp.lon = m.lon AND lp.lat = m.lat;
UPDATE {solar_pv} sp SET kwh_m12 = m.kwh FROM {m12} m WHERE lp.lon = m.lon AND lp.lat = m.lat;

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
UPDATE {solar_pv} p SET en = ST_Transform(ST_SetSRID(ST_MakePoint(p.lon,p.lat), {srid}), 27700);
CREATE INDEX ON {solar_pv} USING GIST (en);

ALTER TABLE {solar_pv} ADD COLUMN pixel geometry(Polygon, 27700);
UPDATE {solar_pv} p SET pixel = ST_Buffer(p.en, {res} / 2.0, 'endcap=square')
CREATE INDEX ON {solar_pv} USING GIST (pixel);


-- These are the fields needed by the cost-benefit model:
-- pv.toid
-- pv.job_id
-- pv.peak_power
-- pv.kwh_year
-- pv.roof_plane_id
-- pv.roof_geom_4326
-- pv.area

-- do a spatial join with all {solar_pv} pixels that intersect each
-- panel installation, work out what proportion of the pixel overlaps
-- the installation, and add up the kWh to get kWh of installation:
DROP TABLE IF EXISTS {panel_kwh};
CREATE TABLE {panel_kwh} AS
SELECT
    pp.toid,
    pp.roof_plane_id,
    -- sum of the kwh of each pixel that intersects the panel,
    -- multiplied by the proportion of the pixel that intersects the panel:
    -- TODO: PVMAPS produces kWh values per pixel as if a pixel was a 1kWp panel
    -- so the ratio of pixel coverage to installation kWp will need adjusting
    SUM((ST_Area(ST_Intersection(pixel, pp.panel_geom_27700)) / ({res} * {res})) * pv.kwh_year) AS kwh_year,

    SUM((ST_Area(ST_Intersection(pixel, pp.panel_geom_27700)) / ({res} * {res})) * pv.kwh_m01) AS kwh_m01,
    SUM((ST_Area(ST_Intersection(pixel, pp.panel_geom_27700)) / ({res} * {res})) * pv.kwh_m02) AS kwh_m02,
    SUM((ST_Area(ST_Intersection(pixel, pp.panel_geom_27700)) / ({res} * {res})) * pv.kwh_m03) AS kwh_m03,
    SUM((ST_Area(ST_Intersection(pixel, pp.panel_geom_27700)) / ({res} * {res})) * pv.kwh_m04) AS kwh_m04,
    SUM((ST_Area(ST_Intersection(pixel, pp.panel_geom_27700)) / ({res} * {res})) * pv.kwh_m05) AS kwh_m05,
    SUM((ST_Area(ST_Intersection(pixel, pp.panel_geom_27700)) / ({res} * {res})) * pv.kwh_m06) AS kwh_m06,
    SUM((ST_Area(ST_Intersection(pixel, pp.panel_geom_27700)) / ({res} * {res})) * pv.kwh_m07) AS kwh_m07,
    SUM((ST_Area(ST_Intersection(pixel, pp.panel_geom_27700)) / ({res} * {res})) * pv.kwh_m08) AS kwh_m08,
    SUM((ST_Area(ST_Intersection(pixel, pp.panel_geom_27700)) / ({res} * {res})) * pv.kwh_m09) AS kwh_m09,
    SUM((ST_Area(ST_Intersection(pixel, pp.panel_geom_27700)) / ({res} * {res})) * pv.kwh_m10) AS kwh_m10,
    SUM((ST_Area(ST_Intersection(pixel, pp.panel_geom_27700)) / ({res} * {res})) * pv.kwh_m11) AS kwh_m11,
    SUM((ST_Area(ST_Intersection(pixel, pp.panel_geom_27700)) / ({res} * {res})) * pv.kwh_m12) AS kwh_m12
FROM
    {panel_polygons} pp
    LEFT JOIN {solar_pv} pv ON ST_Intersects(pixel, pp.panel_geom_27700)
GROUP BY pp.roof_plane_id;

ALTER TABLE {panel_kwh} ADD PRIMARY KEY (roof_plane_id);

INSERT INTO models.solar_pv
SELECT
    pp.toid,
    pp.roof_plane_id,
    %(job_id)s AS job_id,
    ST_SetSrid(ST_Transform(pp.panel_geom_27700, 4326), 4326)::geometry(multipolygon, 4326) AS panel_geom_4326,
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

CREATE OR REPLACE VIEW models.{job_view} AS
SELECT * FROM models.solar_pv WHERE job_id = %(job_id)s;

INSERT INTO models.pv_building_exclusions
SELECT %(job_id)s, toid, exclusion_reason
FROM {building_exclusion_reasons};
