-- Create the schema for this job.

CREATE SCHEMA IF NOT EXISTS {schema} AUTHORIZATION albion_webapp;
GRANT USAGE ON SCHEMA {schema} TO research;
GRANT USAGE ON SCHEMA {schema} TO albion_ddl;
ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} GRANT SELECT ON TABLES TO research;
ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} GRANT ALL ON TABLES TO albion_ddl;

--
-- Create the bounds table in 4326 for quick intersection with mastermap buildings:
--
CREATE TABLE IF NOT EXISTS {bounds_4326} AS
SELECT job_id, ST_Transform(bounds, 4326) AS bounds
FROM models.job_queue
WHERE job_id = %(job_id)s;

CREATE INDEX IF NOT EXISTS bounds_4326_bounds_idx ON {bounds_4326} using gist (bounds);

--
-- Extract the buildings that fall within the job bounds:
--
CREATE TABLE IF NOT EXISTS {buildings} AS
SELECT
    toid,
    ST_SetSrid(ST_Transform(geom_4326, 27700),27700)::geometry(polygon,27700) as geom_27700,
--    ST_SetSrid(
--        ST_Transform(geom_4326, '+proj=tmerc +lat_0=49 +lon_0=-2 +k=0.9996012717 +x_0=400000 +y_0=-100000 '
--                                '+ellps=airy +nadgrids=@OSTN15_NTv2_OSGBtoETRS.gsb +units=m +no_defs'
--    ), 27700)::geometry(polygon,27700) as geom_27700,
    NULL::models.pv_exclusion_reason AS exclusion_reason,
    NULL::real AS height
FROM mastermap.building b
LEFT JOIN {bounds_4326} q ON ST_Intersects(b.geom_4326, q.bounds)
WHERE q.job_id=%(job_id)s
-- Only take buildings where the centroid is within the bounds
-- or, if the centroid touches the bounds, the bbox cannot overlap the bounds
-- above or to the left, so that buildings that overlap multiple tiles for
-- open solar runs don't get run twice:
AND ST_Intersects(ST_Centroid(b.geom_4326), q.bounds)
AND (NOT ST_Touches(ST_Centroid(b.geom_4326), q.bounds)
     OR b.geom_4326 &<| q.bounds
     OR b.geom_4326 &>  q.bounds);

CREATE UNIQUE INDEX IF NOT EXISTS buildings_toid_idx ON {buildings} (toid);
CREATE INDEX IF NOT EXISTS buildings_geom_27700_idx ON {buildings} USING GIST (geom_27700);

--
-- Create the table for storing roof planes:
--
CREATE TABLE IF NOT EXISTS {roof_polygons} (
    roof_plane_id SERIAL PRIMARY KEY,
    toid text NOT NULL,
    roof_geom_27700 geometry(polygon, 27700) NOT NULL,
    x_coef double precision NOT NULL,
    y_coef double precision NOT NULL,
    intercept double precision NOT NULL,
    slope double precision NOT NULL,
    aspect double precision NOT NULL,
    sd double precision NOT NULL,
    is_flat bool NOT NULL,
    usable bool NOT NULL,
    easting double precision NOT NULL,
    northing double precision NOT NULL,
    raw_footprint double precision NOT NULL,
    raw_area double precision NOT NULL
);

CREATE INDEX ON {roof_polygons} USING GIST (roof_geom_27700);