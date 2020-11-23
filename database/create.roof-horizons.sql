--
-- Aggregate the per-pixel horizon data by roof polygons,
-- split the roof polygons by building, and filter out
-- unwanted roof polygons (too small, too steep etc).
--

-- relevant buildings:
CREATE TABLE {schema}.buildings AS
SELECT
    toid,
    ST_SetSrid(ST_Transform(geom_4326, 27700),27700)::geometry(polygon,27700) as geom_27700
FROM mastermap.building
WHERE ST_Intersects(geom_4326, ST_Transform((
    SELECT bounds FROM models.job_queue WHERE job_id=%(job_id)s LIMIT 1
), 4326));

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
    ST_Area(c.roof_geom_27700) / cos(avg(h.slope)) as area,
    ST_Area(c.roof_geom_27700) as footprint,
    {horizon_cols}
FROM
    {schema}.building_roofs c
    LEFT JOIN {pixel_horizons} h ON ST_Contains(c.roof_geom_27700, h.en)
GROUP BY c.roof_id
HAVING ST_Area(c.roof_geom_27700) / cos(avg(h.slope)) >= %(min_roof_area_m)s;

-- Remove any roof polygons that are unsuitable for panels:
DELETE FROM {roof_horizons} WHERE degrees(slope) > %(max_roof_slope_degrees)s;
DELETE FROM {roof_horizons} WHERE degrees(aspect) >= (360-%(min_roof_degrees_from_north)s)
                              AND degrees(slope) > 5;
DELETE FROM {roof_horizons} WHERE degrees(aspect) <= %(min_roof_degrees_from_north)s
                              AND degrees(slope) > 5;

DROP TABLE {schema}.building_roofs;