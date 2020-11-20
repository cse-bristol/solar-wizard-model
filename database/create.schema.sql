-- Create the schema for this job.

CREATE SCHEMA IF NOT EXISTS {schema} AUTHORIZATION albion_webapp;
GRANT USAGE ON SCHEMA {schema} TO research;
GRANT USAGE ON SCHEMA {schema} TO albion_ddl;
ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} GRANT SELECT ON TABLES TO research;
ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} GRANT ALL ON TABLES TO albion_ddl;
DROP TABLE IF EXISTS {pixel_horizons};
DROP TABLE IF EXISTS {roof_polygons};
DROP TABLE IF EXISTS {roof_horizons};
