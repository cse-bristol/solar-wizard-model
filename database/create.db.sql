-- This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
-- Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
--
-- Should be idempotent as runs every time:
CREATE SCHEMA IF NOT EXISTS models AUTHORIZATION CURRENT_USER;

DO $$ BEGIN
    GRANT USAGE ON SCHEMA models TO research;
    ALTER DEFAULT PRIVILEGES IN SCHEMA models GRANT SELECT ON TABLES TO research;
    GRANT USAGE ON SCHEMA models TO albion_ddl;
    ALTER DEFAULT PRIVILEGES IN SCHEMA models GRANT SELECT ON TABLES TO albion_ddl;
EXCEPTION
    WHEN undefined_object THEN null;
END $$;

--
-- LiDAR
--

CREATE TABLE IF NOT EXISTS models.lidar_50cm (
    rid serial PRIMARY KEY,
    rast raster NOT NULL,
    filename text NOT NULL
);

CREATE INDEX IF NOT EXISTS lidar_50cm_idx ON models.lidar_50cm USING gist (st_convexhull(rast));

CREATE TABLE IF NOT EXISTS models.lidar_1m (
    rid serial PRIMARY KEY,
    rast raster NOT NULL,
    filename text NOT NULL
);

CREATE INDEX IF NOT EXISTS lidar_1m_idx ON models.lidar_1m USING gist (st_convexhull(rast));

CREATE TABLE IF NOT EXISTS models.lidar_2m (
    rid serial PRIMARY KEY,
    rast raster NOT NULL,
    filename text NOT NULL
);

CREATE INDEX IF NOT EXISTS lidar_2m_idx ON models.lidar_2m USING gist (st_convexhull(rast));

--
-- Solar PV:
--
DO $$ BEGIN
    CREATE TYPE models.pv_exclusion_reason AS ENUM (
        'NO_LIDAR_COVERAGE',
        'OUTDATED_LIDAR_COVERAGE',
        'NO_ROOF_PLANES_DETECTED',
        'ALL_ROOF_PLANES_UNUSABLE'
    );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

CREATE TABLE IF NOT EXISTS models.pv_building (
    job_id int NOT NULL,
    toid text NOT NULL,
    exclusion_reason models.pv_exclusion_reason,
    height real,
    PRIMARY KEY(job_id, toid)
);

CREATE TABLE IF NOT EXISTS models.pv_roof_plane (
    toid text NOT NULL,
    roof_plane_id int NOT NULL,
    job_id int NOT NULL,

    roof_geom_4326 geometry(polygon, 4326) NOT NULL,

    kwh_jan_min double precision NOT NULL,
    kwh_jan_avg double precision NOT NULL,
    kwh_jan_max double precision NOT NULL,
    kwh_feb_min double precision NOT NULL,
    kwh_feb_avg double precision NOT NULL,
    kwh_feb_max double precision NOT NULL,
    kwh_mar_min double precision NOT NULL,
    kwh_mar_avg double precision NOT NULL,
    kwh_mar_max double precision NOT NULL,
    kwh_apr_min double precision NOT NULL,
    kwh_apr_avg double precision NOT NULL,
    kwh_apr_max double precision NOT NULL,
    kwh_may_min double precision NOT NULL,
    kwh_may_avg double precision NOT NULL,
    kwh_may_max double precision NOT NULL,
    kwh_jun_min double precision NOT NULL,
    kwh_jun_avg double precision NOT NULL,
    kwh_jun_max double precision NOT NULL,
    kwh_jul_min double precision NOT NULL,
    kwh_jul_avg double precision NOT NULL,
    kwh_jul_max double precision NOT NULL,
    kwh_aug_min double precision NOT NULL,
    kwh_aug_avg double precision NOT NULL,
    kwh_aug_max double precision NOT NULL,
    kwh_sep_min double precision NOT NULL,
    kwh_sep_avg double precision NOT NULL,
    kwh_sep_max double precision NOT NULL,
    kwh_oct_min double precision NOT NULL,
    kwh_oct_avg double precision NOT NULL,
    kwh_oct_max double precision NOT NULL,
    kwh_nov_min double precision NOT NULL,
    kwh_nov_avg double precision NOT NULL,
    kwh_nov_max double precision NOT NULL,
    kwh_dec_min double precision NOT NULL,
    kwh_dec_avg double precision NOT NULL,
    kwh_dec_max double precision NOT NULL,
    kwh_year_min double precision NOT NULL,
    kwh_year_avg double precision NOT NULL,
    kwh_year_max double precision NOT NULL,
    kwp_min double precision NOT NULL,
    kwp_avg double precision NOT NULL,
    kwp_max double precision NOT NULL,
    kwh_per_kwp double precision NOT NULL,
    horizon real[] NOT NULL,
    area_min double precision NOT NULL,
    area_avg double precision NOT NULL,
    area_max double precision NOT NULL,
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
CREATE INDEX IF NOT EXISTS pvrp_toid_idx ON models.pv_roof_plane (toid);
CREATE INDEX IF NOT EXISTS pvp_geom_idx ON models.pv_roof_plane USING GIST (roof_geom_4326);
