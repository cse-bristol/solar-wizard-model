--
-- Aggregate the per-pixel horizon data by roof polygons,
-- split the roof polygons by building, and filter out
-- unwanted roof polygons (too small, too steep etc).
--


-- fix any invalid roof polygons:
UPDATE {roof_polygons} p SET wkb_geometry = ST_MakeValid(wkb_geometry)
WHERE NOT ST_IsValid(wkb_geometry);

-- split the roof polygons by building
CREATE TABLE {schema}.building_roofs_multi AS
SELECT
    b.toid,
    p.ogc_fid,
    p.aspect,
    ST_SetSrid(CASE
        WHEN ST_CoveredBy(p.wkb_geometry, b.geom_27700)
        THEN p.wkb_geometry
        ELSE ST_Intersection(p.wkb_geometry,b.geom_27700)
    END, 27700) AS roof_geom_27700
FROM {schema}.buildings b LEFT JOIN {roof_polygons} p
   ON ST_Intersects(p.wkb_geometry, b.geom_27700);

COMMIT;

CREATE TABLE {schema}.building_roofs AS
SELECT
    toid,
    ogc_fid,
    aspect,
    ST_SetSrid((ST_Dump(roof_geom_27700)).geom, 27700)::geometry(polygon, 27700) AS roof_geom_27700
FROM {schema}.building_roofs_multi;

ALTER TABLE {schema}.building_roofs ADD COLUMN roof_id SERIAL PRIMARY KEY;

COMMIT;

DROP TABLE {roof_polygons};
DROP TABLE {schema}.buildings;
DROP TABLE {schema}.building_roofs_multi;

-- Remove pixels with bad horizons:

DELETE FROM {schema}.pixel_horizons h WHERE {avg_southerly_horizon_rads} > radians(%(max_avg_southerly_horizon_degrees)s);

-- Aggregate the per-pixel horizon data by roof polygon:

CREATE TABLE {roof_horizons} AS
SELECT
    c.roof_id,
    c.toid,
    c.roof_geom_27700::geometry(Polygon, 27700),
    avg(h.slope) AS slope,
    avg(h.aspect) AS aspect,
    avg(sky_view_factor) AS sky_view_factor,
    avg(percent_visible) AS percent_visible,
    ST_X(ST_SetSRID(ST_Centroid(c.roof_geom_27700), 27700)) AS easting,
    ST_Y(ST_SetSRID(ST_Centroid(c.roof_geom_27700), 27700)) AS northing,
    count(*) / cos(avg(h.slope)) as area,
    count(*) as footprint,
    {aggregated_horizon_cols}
FROM
    {schema}.building_roofs c
    LEFT JOIN {pixel_horizons} h ON ST_Contains(c.roof_geom_27700, h.en)
GROUP BY c.roof_id
HAVING count(*) / cos(avg(h.slope)) >= %(min_roof_area_m)s;

-- Add horizon standard deviation info:

ALTER TABLE {roof_horizons} ADD COLUMN horizon_sd double precision;
ALTER TABLE {roof_horizons} ADD COLUMN southerly_horizon_sd double precision;

WITH sd AS (
	  SELECT
	      roof_id,
	      stddev(horizon) AS horizon_sd,
	      stddev(southerly_horizon) AS southerly_horizon_sd
    FROM (
        SELECT
            roof_id,
            unnest(array[{horizon_cols}]) AS horizon,
            unnest(array[{southerly_horizon_cols}]) AS southerly_horizon
        FROM {roof_horizons} h) sub
	  GROUP BY roof_id)
UPDATE {roof_horizons} SET horizon_sd = sd.horizon_sd, southerly_horizon_sd = sd.southerly_horizon_sd
FROM sd
WHERE {roof_horizons}.roof_id = sd.roof_id;

-- Remove any roof polygons that are unsuitable for panels:

DELETE FROM {roof_horizons} WHERE degrees(slope) > %(max_roof_slope_degrees)s;
DELETE FROM {roof_horizons} WHERE degrees(aspect) >= (360-%(min_roof_degrees_from_north)s)
                              AND degrees(slope) > 5;
DELETE FROM {roof_horizons} WHERE degrees(aspect) <= %(min_roof_degrees_from_north)s
                              AND degrees(slope) > 5;

UPDATE {roof_horizons} SET
    slope = radians(%(flat_roof_degrees)s),
    aspect = radians(180),
    area = footprint / cos(radians(%(flat_roof_degrees)s))
WHERE degrees(slope) <= 5;

DROP TABLE {schema}.building_roofs;