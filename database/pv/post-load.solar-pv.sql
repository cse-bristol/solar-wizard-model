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

DELETE FROM {pixel_kwh} WHERE kwh IS NULL;

COMMIT;
START TRANSACTION;

-- create many-to-many from pixels to panel_polygons
-- each entry in the many-to-many is a pct of pixel that overlaps the panel:
DROP TABLE IF EXISTS {pixels_in_panels};
CREATE TABLE {pixels_in_panels} AS
SELECT
    pp.panel_id,
    ST_Area(ST_Intersection(pixel, pp.panel_geom_27700)) AS overlap_area,
    pv.x,
    pv.y
FROM
    {panel_polygons} pp
    LEFT JOIN {pixel_kwh} pv ON ST_Intersects(pixel, pp.panel_geom_27700);

CREATE INDEX ON {pixels_in_panels} (panel_id);
CREATE INDEX ON {pixels_in_panels} (x, y);

DROP TABLE IF EXISTS {panel_kwh};
CREATE TABLE {panel_kwh} AS
SELECT
    pp.toid,
    pp.roof_plane_id,
    pp.panel_id,
    st_centroid(pp.panel_geom_27700)::geometry(point,27700) AS centroid,
    -- sum of the kwh of each pixel that intersects the panel,
    -- multiplied by the proportion of the pixel that intersects the panel.
    -- PVMAPS produces kWh values per pixel as if a pixel was a 1kWp panel
    -- so the values are adjusted accordingly.
    SUM(overlap_area * pv.kwh) * %(peak_power_per_m2)s * {res} * {res} * (1 - {system_loss}) AS kwh_year,

    SUM(overlap_area * pv.kwh_m01) * %(peak_power_per_m2)s * {res} * {res} * (1 - {system_loss}) AS kwh_m01,
    SUM(overlap_area * pv.kwh_m02) * %(peak_power_per_m2)s * {res} * {res} * (1 - {system_loss}) AS kwh_m02,
    SUM(overlap_area * pv.kwh_m03) * %(peak_power_per_m2)s * {res} * {res} * (1 - {system_loss}) AS kwh_m03,
    SUM(overlap_area * pv.kwh_m04) * %(peak_power_per_m2)s * {res} * {res} * (1 - {system_loss}) AS kwh_m04,
    SUM(overlap_area * pv.kwh_m05) * %(peak_power_per_m2)s * {res} * {res} * (1 - {system_loss}) AS kwh_m05,
    SUM(overlap_area * pv.kwh_m06) * %(peak_power_per_m2)s * {res} * {res} * (1 - {system_loss}) AS kwh_m06,
    SUM(overlap_area * pv.kwh_m07) * %(peak_power_per_m2)s * {res} * {res} * (1 - {system_loss}) AS kwh_m07,
    SUM(overlap_area * pv.kwh_m08) * %(peak_power_per_m2)s * {res} * {res} * (1 - {system_loss}) AS kwh_m08,
    SUM(overlap_area * pv.kwh_m09) * %(peak_power_per_m2)s * {res} * {res} * (1 - {system_loss}) AS kwh_m09,
    SUM(overlap_area * pv.kwh_m10) * %(peak_power_per_m2)s * {res} * {res} * (1 - {system_loss}) AS kwh_m10,
    SUM(overlap_area * pv.kwh_m11) * %(peak_power_per_m2)s * {res} * {res} * (1 - {system_loss}) AS kwh_m11,
    SUM(overlap_area * pv.kwh_m12) * %(peak_power_per_m2)s * {res} * {res} * (1 - {system_loss}) AS kwh_m12,
    NULL::real[] AS horizon -- gets filled out below
FROM
    {panel_polygons} pp
    LEFT JOIN {roof_polygons} rp ON pp.roof_plane_id = rp.roof_plane_id
    LEFT JOIN {pixels_in_panels} pip ON pp.panel_id = pip.panel_id
    LEFT JOIN {pixel_kwh} pv ON pv.x = pip.x AND pv.y = pip.y
WHERE pp.panel_geom_27700 IS NOT NULL AND rp.usable
GROUP BY pp.panel_id;

ALTER TABLE {panel_kwh} ADD PRIMARY KEY (panel_id);
CREATE INDEX ON {panel_kwh} USING GIST (centroid);

COMMIT;
START TRANSACTION;

-- Update horizons: by taking the horizon pixel nearest the panel centroid
UPDATE {panel_kwh} pkwh
SET horizon = (
    SELECT pv.horizon
    FROM {pixel_kwh} pv
    LEFT JOIN {pixels_in_panels} pip ON pv.x = pip.x AND pv.y = pip.y
    WHERE pip.panel_id = pkwh.panel_id
    ORDER BY pkwh.centroid <-> pv.en
    LIMIT 1
);

--
-- roof-level horizons:
--

-- create many-to-many from pixels to roof_polygons
DROP TABLE IF EXISTS {pixels_in_roofs};
CREATE TABLE {pixels_in_roofs} AS
SELECT
    rp.roof_plane_id,
    pv.x,
    pv.y
FROM
    {roof_polygons} rp
    LEFT JOIN {pixel_kwh} pv ON ST_Intersects(pixel, rp.roof_geom_27700);

CREATE INDEX ON {pixels_in_roofs} (roof_plane_id);
CREATE INDEX ON {pixels_in_roofs} (x, y);

-- average each slice of the horizon array to create a new average array:
-- based on https://stackoverflow.com/a/66647867
DROP TABLE IF EXISTS {roof_horizons};
CREATE TABLE {roof_horizons} AS
SELECT
    roof_plane_id,
    array_agg(avg_h_slice ORDER BY i) AS horizon
FROM (
    SELECT
        pir.roof_plane_id,
        i,
        avg(h_slice) AS avg_h_slice
    FROM
        {pixels_in_roofs} pir
        LEFT JOIN {pixel_kwh} pv ON pv.x = pir.x AND pv.y = pir.y,
        unnest(horizon) WITH ORDINALITY AS u(h_slice, i)
    WHERE h_slice != 'NaN'::real
    GROUP BY pir.roof_plane_id, i
) AS sub
GROUP BY roof_plane_id;

COMMIT;
START TRANSACTION;

INSERT INTO models.pv_panel
SELECT
    pp.toid,
    pp.roof_plane_id,
    pp.panel_id,
    %(job_id)s AS job_id,
    ST_SetSrid(
      ST_Transform(pp.panel_geom_27700,
                   '+proj=tmerc +lat_0=49 +lon_0=-2 +k=0.9996012717 +x_0=400000 '
                   '+y_0=-100000 +datum=OSGB36 +nadgrids=@OSTN15_NTv2_OSGBtoETRS.gsb +units=m +no_defs',
                   4326
    ), 4326)::geometry(polygon, 4326) AS panel_geom_4326, -- TODO 3857?
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
    pp.area,
    pp.footprint
FROM
    {panel_polygons} pp
    INNER JOIN {panel_kwh} kwh ON kwh.panel_id = pp.panel_id
WHERE pp.panel_geom_27700 IS NOT NULL;

INSERT INTO models.pv_roof_plane
SELECT
    rp.toid,
    rp.roof_plane_id,
    %(job_id)s AS job_id,
    rh.horizon,
    rp.slope,
    rp.aspect,
    rp.x_coef,
    rp.y_coef,
    rp.intercept,
    rp.is_flat
FROM {roof_polygons} rp
LEFT JOIN {roof_horizons} rh ON rp.roof_plane_id = rh.roof_plane_id;

DROP TABLE {panel_kwh};
DROP TABLE {pixel_kwh};
DROP TABLE {pixels_in_panels};
DROP TABLE {roof_horizons};
DROP TABLE {pixels_in_roofs};

INSERT INTO models.pv_building
SELECT %(job_id)s, toid, exclusion_reason, height
FROM {buildings};
