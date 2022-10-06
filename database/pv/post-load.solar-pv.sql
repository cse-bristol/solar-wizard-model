--
-- Convert per-raster-pixel monthly/yearly kWh and horizon data
-- to per-panel monthly/yearly kWh and horizon data
--

ALTER TABLE {pixel_kwh} RENAME COLUMN val TO kwh;

ALTER TABLE {pixel_kwh} ADD COLUMN en geometry(Point, 27700);
UPDATE {pixel_kwh} p SET en = ST_SetSRID(ST_MakePoint(p.x,p.y), 27700);
CREATE INDEX ON {pixel_kwh} USING GIST (en);

ALTER TABLE {pixel_kwh} ADD COLUMN pixel geometry(Polygon, 27700);
UPDATE {pixel_kwh} p SET pixel = ST_Buffer(p.en, {res} / 2.0, 'endcap=square');
CREATE INDEX ON {pixel_kwh} USING GIST (pixel);

COMMIT;
START TRANSACTION;

-- create many-to-many from pixels to panel_polygons
-- each entry in the many-to-many is a pct of pixel that overlaps the panel:
DROP TABLE IF EXISTS {pixels_in_panels};
CREATE TABLE {pixels_in_panels} AS
SELECT
    pp.roof_plane_id,
    ST_Area(ST_Intersection(pixel, pp.panel_geom_27700)) AS overlap_area,
    pv.x,
    pv.y
FROM
    {panel_polygons} pp
    LEFT JOIN {pixel_kwh} pv ON ST_Intersects(pixel, pp.panel_geom_27700);

CREATE INDEX ON {pixels_in_panels} (roof_plane_id);
CREATE INDEX ON {pixels_in_panels} (x, y);

DROP TABLE IF EXISTS {panel_kwh};
CREATE TABLE {panel_kwh} AS
SELECT
    pp.toid,
    pp.roof_plane_id,
    st_centroid(pp.panel_geom_27700)::geometry(point,27700) AS centroid,
    -- sum of the kwh of each pixel that intersects the panel,
    -- multiplied by the proportion of the pixel that intersects the panel.
    -- PVMAPS produces kWh values per pixel as if a pixel was a 1kWp panel
    -- so the values are adjusted accordingly
    -- TODO multiplying by res^2 assumes that PVMAPS always gives outputs as if
    -- TODO  peak power per m2 was 1kWp - but is it actually 1kWp per pixel? in
    -- TODO  which case this should be removed.
    SUM(overlap_area * pv.kwh) * %(peak_power_per_m2)s * {res} * {res} AS kwh_year,

    SUM(overlap_area * pv.kwh_m01) * %(peak_power_per_m2)s * {res} * {res} AS kwh_m01,
    SUM(overlap_area * pv.kwh_m02) * %(peak_power_per_m2)s * {res} * {res} AS kwh_m02,
    SUM(overlap_area * pv.kwh_m03) * %(peak_power_per_m2)s * {res} * {res} AS kwh_m03,
    SUM(overlap_area * pv.kwh_m04) * %(peak_power_per_m2)s * {res} * {res} AS kwh_m04,
    SUM(overlap_area * pv.kwh_m05) * %(peak_power_per_m2)s * {res} * {res} AS kwh_m05,
    SUM(overlap_area * pv.kwh_m06) * %(peak_power_per_m2)s * {res} * {res} AS kwh_m06,
    SUM(overlap_area * pv.kwh_m07) * %(peak_power_per_m2)s * {res} * {res} AS kwh_m07,
    SUM(overlap_area * pv.kwh_m08) * %(peak_power_per_m2)s * {res} * {res} AS kwh_m08,
    SUM(overlap_area * pv.kwh_m09) * %(peak_power_per_m2)s * {res} * {res} AS kwh_m09,
    SUM(overlap_area * pv.kwh_m10) * %(peak_power_per_m2)s * {res} * {res} AS kwh_m10,
    SUM(overlap_area * pv.kwh_m11) * %(peak_power_per_m2)s * {res} * {res} AS kwh_m11,
    SUM(overlap_area * pv.kwh_m12) * %(peak_power_per_m2)s * {res} * {res} AS kwh_m12,
    NULL::real[] AS horizon -- gets filled out below
FROM
    {panel_polygons} pp
    LEFT JOIN {pixels_in_panels} pip ON pp.roof_plane_id = pip.roof_plane_id
    LEFT JOIN {pixel_kwh} pv ON pv.x = pip.x AND pv.y = pip.y
WHERE pp.panel_geom_27700 IS NOT NULL
GROUP BY pp.roof_plane_id;

ALTER TABLE {panel_kwh} ADD PRIMARY KEY (roof_plane_id);
CREATE INDEX ON {panel_kwh} USING GIST (centroid);

COMMIT;
START TRANSACTION;

-- Update horizons: by taking the horizon pixel nearest the panel centroid
UPDATE {panel_kwh} pkwh
SET horizon = (
    SELECT pv.horizon
    FROM {pixel_kwh} pv
    LEFT JOIN {pixels_in_panels} pip ON pv.x = pip.x AND pv.y = pip.y
    WHERE pip.roof_plane_id = pkwh.roof_plane_id
    ORDER BY pkwh.centroid <-> pv.en
    LIMIT 1
);

COMMIT;
START TRANSACTION;

INSERT INTO models.solar_pv
SELECT
    pp.toid,
    pp.roof_plane_id,
    %(job_id)s AS job_id,
    ST_SetSrid(
      ST_Transform(pp.panel_geom_27700,
                   '+proj=tmerc +lat_0=49 +lon_0=-2 +k=0.9996012717 +x_0=400000 '
                   '+y_0=-100000 +datum=OSGB36 +nadgrids=@OSTN15_NTv2_OSGBtoETRS.gsb +units=m +no_defs'
    ), 4326)::geometry(multipolygon, 4326) AS panel_geom_4326,
    kwh.kwh_m01,
    kwh.kwh_m02,
    kwh.kwh_m03,
    kwh.kwh_m04,
    kwh.kwh_m05,
    kwh.kwh_m06,
    kwh.kwh_m07,
    kwh.kwh_m08,
    kwh.kwh_m09,
    kwh.kwh_m10,
    kwh.kwh_m11,
    kwh.kwh_m12,
    kwh.kwh_year,
    pp.area * %(peak_power_per_m2)s AS kwp,
    kwh.horizon,
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
DROP TABLE {pixel_kwh};
DROP TABLE {pixels_in_panels};

CREATE OR REPLACE VIEW models.{job_view} AS
SELECT * FROM models.solar_pv WHERE job_id = %(job_id)s;

INSERT INTO models.pv_building
SELECT %(job_id)s, toid, exclusion_reason
FROM {building_exclusion_reasons};
