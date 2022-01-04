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
    ST_SetSrid(ST_Transform(geom_4326, 27700),27700)::geometry(polygon,27700) as geom_27700
FROM mastermap.building b
LEFT JOIN {bounds_4326} q ON ST_Intersects(b.geom_4326, q.bounds)
WHERE q.job_id=%(job_id)s;

CREATE INDEX IF NOT EXISTS buildings_geom_27700_idx ON {buildings} USING GIST (geom_27700);

--
-- Building exclusion reasons table:
--
CREATE TABLE IF NOT EXISTS {building_exclusion_reasons} AS
SELECT
    toid,
    NULL::models.pv_exclusion_reason AS exclusion_reason
FROM {buildings};

CREATE UNIQUE INDEX IF NOT EXISTS building_exclusions_toid ON {building_exclusion_reasons} (toid);

--
-- Create the table for storing roof planes:
--
CREATE TABLE IF NOT EXISTS {roof_planes} (
    roof_plane_id SERIAL PRIMARY KEY,
    toid text,
    x_coef double precision,
    y_coef double precision,
    intercept double precision,
    slope double precision,
    aspect double precision,
    sd double precision
);
