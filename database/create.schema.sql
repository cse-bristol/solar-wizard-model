-- This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
-- Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
-- Create the schema for this job.

CREATE SCHEMA IF NOT EXISTS {schema} AUTHORIZATION CURRENT_USER;

DO $$ BEGIN
    GRANT USAGE ON SCHEMA models TO research;
    ALTER DEFAULT PRIVILEGES IN SCHEMA models GRANT SELECT ON TABLES TO research;
    GRANT USAGE ON SCHEMA models TO albion_ddl;
    ALTER DEFAULT PRIVILEGES IN SCHEMA models GRANT SELECT ON TABLES TO albion_ddl;
EXCEPTION
    WHEN undefined_object THEN null;
END $$;

--
-- Create the bounds table in 27700 for quick intersection with mastermap buildings:
--
CREATE TABLE IF NOT EXISTS {bounds_27700} AS
SELECT
    %(job_id)s AS job_id,
    ST_Multi(ST_GeomFromText(%(job_bounds_27700)s, 27700))::geometry(multipolygon, 27700) AS bounds_27700;

CREATE INDEX IF NOT EXISTS bounds_27700_bounds_idx ON {bounds_27700} using gist (bounds_27700);

--
-- Extract the buildings that fall within the job bounds:
--
CREATE TABLE IF NOT EXISTS {buildings} AS
SELECT
    toid,
    geom_27700,
    -- For selecting a building 'moat' for detecting outdated LiDAR:
    ST_Buffer(geom_27700, 5, 'endcap=square join=mitre quad_segs=2') AS geom_27700_buffered_5,
    NULL::models.pv_exclusion_reason AS exclusion_reason,
    NULL::real AS height,
    NULL::real AS min_ground_height,
    NULL::real AS max_ground_height
FROM mastermap.building_27700 b
LEFT JOIN {bounds_27700} q ON ST_Intersects(b.geom_27700, q.bounds_27700)
WHERE q.job_id=%(job_id)s
-- Only take buildings where the centroid is within the bounds
-- or, if the centroid touches the bounds, the bbox cannot overlap the bounds
-- above or to the left, so that buildings that overlap multiple tiles if bounds
-- have been created in a tiled layout don't get run twice:
AND ST_Intersects(ST_Centroid(b.geom_27700), q.bounds_27700)
AND (NOT ST_Touches(ST_Centroid(b.geom_27700), q.bounds_27700)
     OR b.geom_27700 &<| q.bounds_27700
     OR b.geom_27700 &>  q.bounds_27700);

CREATE UNIQUE INDEX IF NOT EXISTS buildings_toid_idx ON {buildings} (toid);
CREATE INDEX IF NOT EXISTS buildings_geom_27700_idx ON {buildings} USING GIST (geom_27700);
CREATE INDEX IF NOT EXISTS buildings_geom_27700_buffered_idx ON {buildings} USING GIST (geom_27700_buffered_5);

--
-- Create the table for storing roof planes:
--
CREATE TABLE IF NOT EXISTS {roof_polygons} (
    roof_plane_id SERIAL PRIMARY KEY,
    toid text NOT NULL,
    roof_geom_27700 geometry(polygon, 27700) NOT NULL,
    roof_geom_raw_27700 geometry(polygon, 27700) NOT NULL,
    x_coef double precision NOT NULL,
    y_coef double precision NOT NULL,
    intercept double precision NOT NULL,
    slope double precision NOT NULL,
    aspect double precision NOT NULL,
    is_flat bool NOT NULL,
    usable bool NOT NULL,
    inliers_xy real[][] NOT NULL,
    meta jsonb NOT NULL
);

CREATE INDEX ON {roof_polygons} (toid);
CREATE INDEX ON {roof_polygons} USING GIST (roof_geom_27700);
CREATE INDEX ON {roof_polygons} USING GIST (roof_geom_raw_27700);

--
-- Create the table for storing individual panel polygons:
--
--CREATE TABLE IF NOT EXISTS {panel_polygons} (
--    panel_id SERIAL PRIMARY KEY,
--    roof_plane_id bigint NOT NULL REFERENCES {roof_polygons} (roof_plane_id),
--    toid text NOT NULL,
--    panel_geom_27700 geometry(polygon, 27700) NOT NULL,
--    footprint double precision NOT NULL,
--    area double precision NOT NULL
--);
--
--CREATE INDEX ON {panel_polygons} (roof_plane_id);
--CREATE INDEX ON {panel_polygons} USING GIST (panel_geom_27700);

--
-- elevation raster
--
CREATE TABLE IF NOT EXISTS {elevation} (
    rid serial PRIMARY KEY,
    rast raster NOT NULL,
    filename text NOT NULL
);

CREATE INDEX IF NOT EXISTS elevation_idx ON {elevation} USING gist (st_convexhull(rast));

--
-- aspect raster
--
CREATE TABLE IF NOT EXISTS {aspect} (
    rid serial PRIMARY KEY,
    rast raster NOT NULL,
    filename text NOT NULL
);

CREATE INDEX IF NOT EXISTS aspect_idx ON {aspect} USING gist (st_convexhull(rast));

--
-- slope raster
--
CREATE TABLE IF NOT EXISTS {slope} (
    rid serial PRIMARY KEY,
    rast raster NOT NULL,
    filename text NOT NULL
);

CREATE INDEX IF NOT EXISTS slope_idx ON {slope} USING gist (st_convexhull(rast));

--
-- mask raster
--
CREATE TABLE IF NOT EXISTS {mask} (
    rid serial PRIMARY KEY,
    rast raster NOT NULL,
    filename text NOT NULL
);

CREATE INDEX IF NOT EXISTS mask_idx ON {mask} USING gist (st_convexhull(rast));
