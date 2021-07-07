--
-- Aggregate the per-pixel horizon data by roof polygons, and filter out
-- unwanted roof polygons (too small, too steep etc), as well as any
-- pixels where the southerly horizon is too high.
--

CREATE TABLE {roof_horizons} AS
SELECT
    ST_Multi(ST_Buffer(
        ST_Union(ST_Rotate(
            ST_Expand(h.en, sqrt(%(resolution)s * %(resolution)s * 2.0) / 2),
            -radians(p.aspect),
            h.en)),
         -((sqrt(%(resolution)s * %(resolution)s * 2.0) - 1) / 2),
        'endcap=square join=mitre quad_segs=2'))::geometry(MultiPolygon, 27700) AS roof_geom_27700,
    p.roof_plane_id,
    p.toid,
    p.x_coef,
    p.y_coef,
    p.intercept,
    p.slope,
    p.aspect,
    avg(sky_view_factor) AS sky_view_factor,
    avg(percent_visible) AS percent_visible,
    (count(*) * %(resolution)s * %(resolution)s) / cos(radians(p.slope)) AS raw_area,
    (count(*) * %(resolution)s * %(resolution)s) AS raw_footprint,
    p.slope <= 5 AS is_flat,
    {aggregated_horizon_cols}
FROM
    {roof_planes} p LEFT JOIN {pixel_horizons} h
    ON p.roof_plane_id = h.roof_plane_id
WHERE {avg_southerly_horizon_rads} <= radians(%(max_avg_southerly_horizon_degrees)s)
GROUP BY p.roof_plane_id;
COMMIT;

--
-- Mark roof areas as unusable where they don't match the job
-- parameters:
--
ALTER TABLE {roof_horizons} ADD COLUMN usable boolean;

UPDATE {roof_horizons} p SET usable =
p.slope <= %(max_roof_slope_degrees)s
AND ((p.aspect < (360-%(min_roof_degrees_from_north)s)
        AND p.aspect > %(min_roof_degrees_from_north)s)
    OR p.slope <= 5)
AND raw_area >= %(min_roof_area_m)s;

CREATE INDEX ON {roof_horizons} USING GIST (roof_geom_27700);
ALTER TABLE {roof_horizons} ADD PRIMARY KEY (roof_plane_id);
COMMIT;

--
-- Constrain roof planes to building polygon:
--
UPDATE {roof_horizons} h SET roof_geom_27700 = ST_Multi(ST_Intersection(roof_geom_27700, geom_27700))
FROM {buildings} b
WHERE h.toid = b.toid;
COMMIT;

--
-- Add easting and northing:
--
ALTER TABLE {roof_horizons} ADD COLUMN easting double precision;
ALTER TABLE {roof_horizons} ADD COLUMN northing double precision;

UPDATE {roof_horizons} SET
    easting = ST_X(ST_SetSRID(ST_Centroid(roof_geom_27700), 27700)),
    northing = ST_Y(ST_SetSRID(ST_Centroid(roof_geom_27700), 27700));
COMMIT;

--
-- Add 3D version of roof plane:
--
ALTER TABLE {roof_horizons} ADD COLUMN roof_geom_27700_3d geometry(MultiPolygonZ, 27700);

UPDATE {roof_horizons} SET roof_geom_27700_3d = ST_Multi(ST_Translate(
    ST_RotateY(
        ST_RotateX(
            ST_Translate(
                ST_Force3d(ST_Scale(
                    roof_geom_27700,
                    ST_MakePoint(sqrt((x_coef * x_coef) + 1), sqrt((y_coef * y_coef) + 1)),
                    ST_Centroid(roof_geom_27700))),
                -easting, -northing),
            atan(y_coef)),
        atan(x_coef)),
    easting,
    northing,
    (easting * x_coef) + (northing * y_coef) + intercept))::geometry(MultiPolygonZ, 27700);

CREATE INDEX ON {roof_horizons} USING GIST (roof_geom_27700_3d);
COMMIT;

--
-- Add horizon standard deviation info:
--
ALTER TABLE {roof_horizons} ADD COLUMN horizon_sd double precision;
ALTER TABLE {roof_horizons} ADD COLUMN southerly_horizon_sd double precision;

WITH sd AS (
	  SELECT
	      roof_plane_id,
	      stddev(horizon) AS horizon_sd,
	      stddev(southerly_horizon) AS southerly_horizon_sd
    FROM (
        SELECT
            roof_plane_id,
            unnest(array[{horizon_cols}]) AS horizon,
            unnest(array[{southerly_horizon_cols}]) AS southerly_horizon
        FROM {roof_horizons} h) sub
	  GROUP BY roof_plane_id)
UPDATE {roof_horizons} SET horizon_sd = sd.horizon_sd, southerly_horizon_sd = sd.southerly_horizon_sd
FROM sd
WHERE {roof_horizons}.roof_plane_id = sd.roof_plane_id;
COMMIT;

--
-- Handle flat roofs
--
UPDATE {roof_horizons} SET
    slope = %(flat_roof_degrees)s,
    aspect = 180,
    raw_area = raw_footprint / cos(radians(%(flat_roof_degrees)s))
WHERE is_flat;
COMMIT;

--
-- PV panelling:
--
CREATE TABLE {panel_horizons} AS
WITH panels AS (
    SELECT
        models.pv_grid(roof_geom_27700, 0.9, 1.6, aspect, slope, is_flat) AS panel_geom_27700,
        roof_plane_id
    FROM {roof_horizons}
)
SELECT
    ph.panel_geom_27700,
    ST_Area(ph.panel_geom_27700) AS footprint,
    ST_Area(ph.panel_geom_27700) / cos(radians(rh.slope)) AS area,
    rh.*
FROM {roof_horizons} rh INNER JOIN panels ph USING (roof_plane_id);

CREATE INDEX ON {panel_horizons} USING GIST (panel_geom_27700);
ALTER TABLE {panel_horizons} ADD PRIMARY KEY (roof_plane_id);

UPDATE {panel_horizons} p SET usable = false
WHERE usable = true AND area < %(min_roof_area_m)s;
