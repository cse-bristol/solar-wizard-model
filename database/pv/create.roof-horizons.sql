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
         -((sqrt(%(resolution)s * %(resolution)s * 2.0) - %(resolution)s) / 2),
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
START TRANSACTION;

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
START TRANSACTION;

--
-- Constrain roof planes to building polygon, and enforce min_dist_to_edge_m
-- and min_dist_to_edge_large_m:
--
UPDATE {roof_horizons} h
SET roof_geom_27700 = ST_Multi(ST_CollectionExtract(ST_MakeValid(ST_Intersection(
    roof_geom_27700,
    ST_Buffer(geom_27700,
              CASE WHEN ST_Area(geom_27700) > %(large_building_threshold)s
                   THEN -%(min_dist_to_edge_large_m)s
                   ELSE -%(min_dist_to_edge_m)s END,
              'endcap=square join=mitre'))), 3))
FROM {buildings} b
WHERE h.toid = b.toid;

COMMIT;
START TRANSACTION;

--
-- Don't allow roof plane polygons to overlap:
--
UPDATE {roof_horizons} h1
SET roof_geom_27700 = COALESCE(ST_Multi(ST_CollectionExtract(ST_MakeValid(ST_Difference(
    roof_geom_27700,
    (SELECT ST_Union(h2.roof_geom_27700)
     FROM {roof_horizons} h2
     WHERE
        ST_Intersects(h1.roof_geom_27700, h2.roof_geom_27700)
        AND h1.toid = h2.toid
        -- The lowest roof plane IDs take precedence (arbitrarily)
        AND h1.roof_plane_id > h2.roof_plane_id))), 3)), h1.roof_geom_27700);

COMMIT;
START TRANSACTION;

--
-- Add easting and northing:
--
ALTER TABLE {roof_horizons} ADD COLUMN easting double precision;
ALTER TABLE {roof_horizons} ADD COLUMN northing double precision;

UPDATE {roof_horizons} SET
    easting = ST_X(ST_SetSRID(ST_Centroid(roof_geom_27700), 27700)),
    northing = ST_Y(ST_SetSRID(ST_Centroid(roof_geom_27700), 27700));

COMMIT;
START TRANSACTION;

--
-- Add horizon averages and standard deviation info:
--
ALTER TABLE {roof_horizons} ADD COLUMN horizon_avg double precision;
ALTER TABLE {roof_horizons} ADD COLUMN horizon_sd double precision;
ALTER TABLE {roof_horizons} ADD COLUMN southerly_horizon_avg double precision;
ALTER TABLE {roof_horizons} ADD COLUMN southerly_horizon_sd double precision;

WITH sd AS (
	  SELECT
	      roof_plane_id,
	      avg(horizon) AS horizon_avg,
	      stddev(horizon) AS horizon_sd,
	      avg(southerly_horizon) AS southerly_horizon_avg,
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
START TRANSACTION;

--
-- Handle flat roofs
--
UPDATE {roof_horizons} SET
    slope = %(flat_roof_degrees)s,
    aspect = 180,
    raw_area = raw_footprint / cos(radians(%(flat_roof_degrees)s))
WHERE is_flat;

COMMIT;
START TRANSACTION;

--
-- Fix up angles - changes any roof planes where the aspect is almost aligned
-- with the angles of the building to be fully aligned. Treats all buildings
-- as rectangles.
--
WITH azimuth AS (
    SELECT
    toid,
    degrees(st_azimuth(st_pointN(st_boundary(ST_OrientedEnvelope(geom_27700)), 1),
                       st_pointN(st_boundary(ST_OrientedEnvelope(geom_27700)), 2))) AS degs
    FROM {buildings}
),
all_angles AS (
    SELECT toid, degs FROM azimuth
    UNION
    SELECT toid, (degs + 90)::numeric %% 360::numeric FROM azimuth
    UNION
    SELECT toid, (degs + 180)::numeric %% 360::numeric FROM azimuth
    UNION
    SELECT toid, (degs + 270)::numeric %% 360::numeric FROM azimuth
)
UPDATE {roof_horizons} h SET aspect = a.degs
FROM all_angles a
WHERE h.toid = a.toid AND usable AND (
    (NOT is_flat AND abs(a.degs - aspect) < 15) OR
    -- for flat roofs, if there is a sensible angle near-South, align the panels
    -- with that:
    (is_flat AND abs(a.degs - aspect) < 45));
