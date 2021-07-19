--
-- Add an entry to the models.lidar_info table
--


DROP TABLE IF EXISTS {clean_table};

-- Clean and aggregate the polygons from polygonising the LiDAR coverage tiff:
CREATE TABLE {clean_table} AS
SELECT
    ST_SetSrid(
        ST_CollectionExtract(ST_MakeValid(ST_Union(ST_MakeValid(t.geom_27700)))),
        27700)::geometry(multipolygon, 27700) AS geom_27700
FROM {temp_table} t;

CREATE INDEX ON {clean_table} USING GIST (geom_27700);
COMMIT;

-- Find the intersection of the coverage polygon with the job bounds polygon:
INSERT into models.lidar_info (job_id, lidar_coverage_4326, lidar_coverage_pct)
SELECT
    %(job_id)s,
    ST_Transform(ST_Intersection(t.geom_27700, q.bounds), 4326),
    ST_Area(ST_Intersection(t.geom_27700, q.bounds)) / ST_Area(q.bounds)
FROM {clean_table} t, models.job_queue q
WHERE q.job_id = %(job_id)s;
COMMIT;

-- Add info about the number of buildings in the job bounds and the number
-- that have at least 1 pixel of LiDAR coverage:
UPDATE models.lidar_info SET
    buildings_in_bounds = (
        SELECT count(*)
        FROM mastermap.building b
        INNER JOIN models.job_queue q
        ON ST_Intersects(st_transform(q.bounds, 4326), b.geom_4326)
        WHERE q.job_id = %(job_id)s ),
    buildings_with_coverage = (
        SELECT count(*)
        FROM mastermap.building b
        INNER JOIN models.lidar_info l
        ON ST_Intersects(l.lidar_coverage_4326, b.geom_4326)
        WHERE l.job_id = %(job_id)s )
    WHERE job_id = %(job_id)s;
COMMIT;

DROP TABLE {temp_table};
DROP TABLE {clean_table};
