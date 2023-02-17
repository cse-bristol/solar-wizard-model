-- Should be idempotent as runs every time
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
    horizon real[] NOT NULL,
    slope double precision NOT NULL,
    aspect double precision NOT NULL,
    x_coef double precision NOT NULL,
    y_coef double precision NOT NULL,
    intercept double precision NOT NULL,
    is_flat boolean NOT NULL,
    PRIMARY KEY (roof_plane_id, job_id)
);

CREATE INDEX IF NOT EXISTS pvrp_job_id_idx ON models.pv_roof_plane (job_id);
CREATE INDEX IF NOT EXISTS pvrp_toid_idx ON models.pv_roof_plane (toid);

CREATE TABLE IF NOT EXISTS models.pv_panel (
    toid text NOT NULL,
    roof_plane_id int NOT NULL,
    panel_id bigint NOT NULL,
    job_id int NOT NULL,
    panel_geom_4326 geometry(polygon, 4326) NOT NULL,
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
    kwp double precision NOT NULL,
    horizon real[] NOT NULL,
    area double precision NOT NULL,
    footprint double precision NOT NULL,
    PRIMARY KEY (panel_id, job_id)
);

CREATE INDEX IF NOT EXISTS pvp_job_id_idx ON models.pv_panel (job_id);
CREATE INDEX IF NOT EXISTS pvp_toid_idx ON models.pv_panel (toid);
CREATE INDEX IF NOT EXISTS pvp_rp_idx ON models.pv_panel (roof_plane_id);
CREATE INDEX IF NOT EXISTS pvp_geom_idx ON models.pv_panel USING GIST (panel_geom_4326);
