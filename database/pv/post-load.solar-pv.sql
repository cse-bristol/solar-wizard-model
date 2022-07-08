
-- TODO need to add a db migration in 320-albion-webapp repo which changes the shape of models.solar_pv
--  maybe move the existing table to somewhere like solar_pv_old?

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
DROP TABLE IF EXISTS {toid_kwh};
CREATE TABLE {toid_kwh} AS
SELECT
    pp.toid,
    -- sum of the kwh of each pixel that intersects the panel,
    -- multiplied by the proportion of the pixel that intersects the panel:
    SUM(
        (ST_Area(ST_Intersection(
            ST_Buffer(pv.en, {res} / 2.0, 'endcap=square'),
            pp.panel_geom_27700
        )) / ({res} * {res})) *  pv.kwh_year
    ) AS kwh_year
FROM
    {panel_polygons} pp
    -- Make the square of the pixel by buffering its centre point and join spatially:
    LEFT JOIN {solar_pv} pv ON ST_Intersects(
        ST_Buffer(pv.en, {res} / 2.0, 'endcap=square'),
        pp.panel_geom_27700
    )
GROUP BY pp.toid;

ALTER TABLE {toid_kwh} ADD PRIMARY KEY (toid);

ALTER TABLE {solar_pv} ADD COLUMN en geometry(Point, 27700);
UPDATE {solar_pv} p SET en = ST_Transform(ST_SetSRID(ST_MakePoint(p.easting,p.northing), {srid}), 27700);
CREATE INDEX ON {solar_pv} USING GIST (en);
UPDATE {solar_pv} SET easting = ST_X(en), northing = ST_Y(en);

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
    LEFT JOIN {toid_kwh} kwh ON kwh.toid = pp.toid;

CREATE OR REPLACE VIEW models.{job_view} AS
SELECT * FROM models.solar_pv WHERE job_id = %(job_id)s;

INSERT INTO models.pv_building_exclusions
SELECT %(job_id)s, toid, exclusion_reason
FROM {building_exclusion_reasons}
WHERE exclusion_reason IS NOT NULL;
