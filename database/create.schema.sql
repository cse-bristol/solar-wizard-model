-- This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
-- Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
-- Create the schema for this job.

CREATE SCHEMA IF NOT EXISTS {schema} AUTHORIZATION CURRENT_USER;

--
-- Tracker for which model stage has been reached:
--
CREATE TABLE IF NOT EXISTS {model_stage} AS SELECT 'INIT' as stage;

--
-- The job bounds in 27700. Populated by the building loader from the extent of the
-- passed buildings (solar_pv/buildings.py), not seeded here.
--
CREATE TABLE IF NOT EXISTS {bounds_27700} (
    job_id int,
    bounds_27700 geometry(multipolygon, 27700)
);

CREATE INDEX IF NOT EXISTS bounds_27700_bounds_idx ON {bounds_27700} using gist (bounds_27700);

--
-- The buildings to model. Created empty here and populated by the building loader
-- (solar_pv/buildings.py) from the caller-supplied buildings.
--
CREATE TABLE IF NOT EXISTS {buildings} (
    building_id text,
    geom_27700 geometry(polygon, 27700),
    -- A building 'moat' for detecting outdated LiDAR:
    geom_27700_buffered_5 geometry(polygon, 27700),
    exclusion_reason models.pv_exclusion_reason,
    height real,
    min_ground_height real,
    max_ground_height real
);

CREATE UNIQUE INDEX IF NOT EXISTS buildings_building_id_idx ON {buildings} (building_id);
CREATE INDEX IF NOT EXISTS buildings_geom_27700_idx ON {buildings} USING GIST (geom_27700);
CREATE INDEX IF NOT EXISTS buildings_geom_27700_buffered_idx ON {buildings} USING GIST (geom_27700_buffered_5);

--
-- Create the table for storing roof planes:
--
CREATE TABLE IF NOT EXISTS {roof_polygons} (
    roof_plane_id SERIAL PRIMARY KEY,
    building_id text NOT NULL,
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

CREATE INDEX ON {roof_polygons} (building_id);
CREATE INDEX ON {roof_polygons} USING GIST (roof_geom_27700);
CREATE INDEX ON {roof_polygons} USING GIST (roof_geom_raw_27700);

--
-- Create the table for storing individual panel polygons:
--
--CREATE TABLE IF NOT EXISTS {panel_polygons} (
--    panel_id SERIAL PRIMARY KEY,
--    roof_plane_id bigint NOT NULL REFERENCES {roof_polygons} (roof_plane_id),
--    building_id text NOT NULL,
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
