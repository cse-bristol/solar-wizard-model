--
-- Add an entry to the models.lidar_info table
--

-- Create a copy of the job bounds in 4326:
CREATE TABLE {bounds_table} AS
SELECT ST_Transform(bounds, 4326) AS bounds_4326
FROM models.job_queue
WHERE job_id = %(job_id)s;

CREATE INDEX ON {bounds_table} USING GIST (bounds_4326);

-- clean up polygonised lidar cov:
DELETE FROM {temp_table}
USING {bounds_table}
WHERE NOT ST_Intersects(geom_4326, bounds_4326);

UPDATE {temp_table}
SET geom_4326 = ST_Multi(ST_CollectionExtract(ST_Buffer(geom_4326, 0), 3))
WHERE NOT ST_IsValid(geom_4326);

UPDATE {temp_table}
SET geom_4326 = ST_Multi(ST_CollectionExtract(ST_Intersection(ST_MakeValid(geom_4326), bounds_4326), 3))
FROM {bounds_table}
WHERE ST_Overlaps(geom_4326, bounds_4326);

COMMIT;
START TRANSACTION;

DROP TABLE IF EXISTS {clean_table};

-- Clean and aggregate the polygons from polygonising the LiDAR coverage tiff:
CREATE TABLE {clean_table} AS
SELECT
    ST_SetSrid(
        ST_Multi(ST_CollectionExtract(ST_MakeValid(ST_Union(ST_MakeValid(t.geom_4326))), 3)),
        4326)::geometry(multipolygon, 4326) AS geom_4326,
    resolution
FROM {temp_table} t
GROUP BY resolution;

CREATE INDEX ON {clean_table} USING GIST (geom_4326);

COMMIT;
START TRANSACTION;

DELETE FROM models.lidar_info WHERE job_id = %(job_id)s;

-- Total coverage:
INSERT into models.lidar_info (
    job_id,
    lidar_coverage_4326,
    lidar_coverage_pct,
    buildings_in_bounds,
    building_coverage_pct,
    resolution)
SELECT
    %(job_id)s,
    ST_Multi(t.geom_4326),
    ST_Area(t.geom_4326) / ST_Area(bounds_4326),
    0,
    0.0,
    'all'
FROM (SELECT ST_Union(geom_4326) geom_4326 FROM {clean_table}) t, {bounds_table} q;

--per-resolution coverage:
INSERT into models.lidar_info (
    job_id,
    lidar_coverage_4326,
    lidar_coverage_pct,
    buildings_in_bounds,
    building_coverage_pct,
    resolution)
SELECT
    %(job_id)s,
    ST_Multi(t.geom_4326),
    ST_Area(t.geom_4326) / ST_Area(bounds_4326),
    0,
    0.0,
    CASE WHEN t.resolution = 0.5 THEN '50cm'::models.lidar_resolution
         WHEN t.resolution = 1.0 THEN '1m'::models.lidar_resolution
         WHEN t.resolution = 2.0 THEN '2m'::models.lidar_resolution END
FROM {clean_table} t, {bounds_table} q;

COMMIT;
START TRANSACTION;

-- Add info about the number of buildings in the job bounds and the number
-- that have at least 1 pixel of LiDAR coverage:
WITH bb AS (
    SELECT count(*) AS count
    FROM mastermap.building b
    INNER JOIN {bounds_table} q
    ON ST_Intersects(bounds_4326, b.geom_4326)
)
UPDATE models.lidar_info SET
    buildings_in_bounds = bb.count
    FROM bb
    WHERE job_id = %(job_id)s;

UPDATE models.lidar_info ll SET
    building_coverage_pct = COALESCE((
        SELECT count(*)
        FROM
            mastermap.building b
            INNER JOIN models.lidar_info l ON ST_Intersects(l.lidar_coverage_4326, b.geom_4326)
        WHERE
            l.job_id = %(job_id)s
            AND l.resolution = ll.resolution
    ) / NULLIF(buildings_in_bounds::real, 0), 0.0)
    WHERE job_id = %(job_id)s;

DROP TABLE {temp_table};
DROP TABLE {clean_table};
DROP TABLE {bounds_table};
