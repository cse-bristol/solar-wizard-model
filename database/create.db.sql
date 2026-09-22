-- This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
-- Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
--
-- Should be idempotent as runs every time:
CREATE SCHEMA IF NOT EXISTS models AUTHORIZATION CURRENT_USER;

DO $$ BEGIN
    CREATE TYPE models.pv_exclusion_reason AS ENUM (
        'NO_LIDAR_COVERAGE',
        'OUTDATED_LIDAR_COVERAGE',
        'NO_ROOF_PLANES_DETECTED',
        'ALL_ROOF_PLANES_UNUSABLE',
        'TOO_SMALL'
    );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

CREATE TABLE IF NOT EXISTS models.pv_building (
    job_id int NOT NULL,
    building_id text NOT NULL,
    exclusion_reason models.pv_exclusion_reason,
    height real,
    PRIMARY KEY(job_id, building_id)
);

CREATE TABLE IF NOT EXISTS models.pv_roof_plane (
    building_id text NOT NULL,
    roof_plane_id int NOT NULL,
    job_id int NOT NULL,

    roof_geom_4326 geometry(polygon, 4326) NOT NULL,

    kwh_jan double precision NOT NULL,
    kwh_feb double precision NOT NULL,
    kwh_mar double precision NOT NULL,
    kwh_apr double precision NOT NULL,
    kwh_may double precision NOT NULL,
    kwh_jun double precision NOT NULL,
    kwh_jul double precision NOT NULL,
    kwh_aug double precision NOT NULL,
    kwh_sep double precision NOT NULL,
    kwh_oct double precision NOT NULL,
    kwh_nov double precision NOT NULL,
    kwh_dec double precision NOT NULL,
    kwh_year double precision NOT NULL,
    kwh_year_p90 double precision NOT NULL,
    kwp double precision NOT NULL,
    kwh_per_kwp double precision NOT NULL,
    horizon real[] NOT NULL,
    area double precision NOT NULL,
    confidence real,
    x_coef double precision NOT NULL,
    y_coef double precision NOT NULL,
    intercept double precision NOT NULL,
    slope double precision NOT NULL,
    aspect double precision NOT NULL,
    is_flat bool NOT NULL,
    meta jsonb NOT NULL,

    PRIMARY KEY (roof_plane_id, job_id)
);

CREATE INDEX IF NOT EXISTS pvrp_job_id_idx ON models.pv_roof_plane (job_id);
CREATE INDEX IF NOT EXISTS pvrp_building_id_idx ON models.pv_roof_plane (building_id);
CREATE INDEX IF NOT EXISTS pvp_geom_idx ON models.pv_roof_plane USING GIST (roof_geom_4326);
