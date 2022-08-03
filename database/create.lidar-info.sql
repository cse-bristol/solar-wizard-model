--
-- Add entries to the models.lidar_info table
--

-- Create the per-tile coverage polygons:

DROP TABLE IF EXISTS {per_tile_table};
CREATE TABLE {per_tile_table} (
    job_id int NOT NULL,
    x int NOT NULL,
    y int NOT NULL,
    lidar_coverage_27700 geometry(multipolygon, 27700) NOT NULL,
    resolution models.lidar_resolution NOT NULL,
    PRIMARY KEY(x, y, resolution)
);
CREATE INDEX ON {per_tile_table} USING GIST (lidar_coverage_27700);

INSERT INTO {per_tile_table} (
    job_id,
    x, y,
    lidar_coverage_27700,
    resolution)
SELECT
    %(job_id)s,
    ST_UpperLeftX(rast), ST_UpperLeftY(rast),
    st_polygon(st_clip(l.rast, jq.bounds)),
    '50cm'
FROM models.lidar_50cm l
LEFT JOIN models.job_queue jq ON st_intersects(l.rast, jq.bounds)
WHERE jq.job_id = %(job_id)s;

INSERT INTO {per_tile_table} (
    job_id,
    x, y,
    lidar_coverage_27700,
    resolution)
SELECT
    %(job_id)s,
    ST_UpperLeftX(rast), ST_UpperLeftY(rast),
    st_polygon(st_clip(l.rast, jq.bounds)),
    '1m'
FROM models.lidar_1m l
LEFT JOIN models.job_queue jq ON st_intersects(l.rast, jq.bounds)
WHERE jq.job_id = %(job_id)s;

INSERT INTO {per_tile_table} (
    job_id,
    x, y,
    lidar_coverage_27700,
    resolution)
SELECT
    %(job_id)s,
    ST_UpperLeftX(rast), ST_UpperLeftY(rast),
    st_polygon(st_clip(l.rast, jq.bounds)),
    '2m'
FROM models.lidar_2m l
LEFT JOIN models.job_queue jq ON st_intersects(l.rast, jq.bounds)
WHERE jq.job_id = %(job_id)s;

-- This relies on all 3 resolutions having the same tile-size in 27700
-- (so different pixel-tile-size) post-loading into the database:
INSERT INTO {per_tile_table} (
    job_id,
    x, y,
    lidar_coverage_27700,
    resolution)
SELECT
    %(job_id)s,
    x, y,
    st_multi(st_union(lidar_coverage_27700)),
    'all'
FROM {per_tile_table} l
WHERE l.job_id = %(job_id)s
GROUP BY x, y;

COMMIT;
START TRANSACTION;

-- whole-area coverage polygons:

DELETE FROM models.lidar_info WHERE job_id = %(job_id)s;

INSERT INTO models.lidar_info (
    job_id,
    lidar_coverage_4326,
    lidar_coverage_pct,
    buildings_in_bounds,
    building_coverage_pct,
    resolution)
SELECT
    %(job_id)s,
    st_transform(
      st_multi(
        ST_CollectionExtract(
          ST_MakeValid(st_union(lidar_coverage_27700)),
          3)),
      4326),
    0.0, 0, 0.0,
    resolution
FROM {per_tile_table} l
WHERE l.job_id = %(job_id)s
GROUP BY resolution;

-- Create a copy of the job bounds in 4326:

DROP TABLE IF EXISTS {bounds_table};
CREATE TABLE {bounds_table} AS
SELECT ST_Transform(bounds, 4326) AS bounds_4326
FROM models.job_queue
WHERE job_id = %(job_id)s;

CREATE INDEX ON {bounds_table} USING GIST (bounds_4326);

-- Add pct coverage info:

UPDATE models.lidar_info li SET
    lidar_coverage_pct = ST_Area(li.lidar_coverage_4326) / ST_Area(bt.bounds_4326)
    FROM {bounds_table} bt
    WHERE li.job_id = %(job_id)s;

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

DROP TABLE {bounds_table};
