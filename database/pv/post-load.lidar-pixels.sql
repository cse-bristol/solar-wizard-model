
DO $$ BEGIN RAISE NOTICE 'Adding extra columns'; END; $$;

ALTER TABLE {lidar_pixels} ADD COLUMN en geometry(Point, 27700);
ALTER TABLE {lidar_pixels} ADD COLUMN pixel_id SERIAL PRIMARY KEY;
ALTER TABLE {lidar_pixels} ADD COLUMN roof_plane_id int;
ALTER TABLE {lidar_pixels} ADD COLUMN toid text;

DO $$ BEGIN RAISE NOTICE 'Setting aspect to NODATA where elevation is NODATA'; END; $$;

UPDATE {lidar_pixels} lp SET aspect = -9999 WHERE elevation = -9999;

COMMIT;
START TRANSACTION;

DO $$ BEGIN RAISE NOTICE 'Adding centroid'; END; $$;

UPDATE {lidar_pixels} p SET en = ST_Transform(ST_SetSRID(ST_MakePoint(p.easting,p.northing), {srid}), 27700);
CREATE INDEX ON {lidar_pixels} USING GIST (en);

COMMIT;
START TRANSACTION;

DO $$ BEGIN RAISE NOTICE 'Adding TOID'; END; $$;

UPDATE {lidar_pixels}
SET toid = (
    SELECT toid
    FROM (
        SELECT b.geom_27700, b.toid
        FROM {buildings} b
        ORDER BY b.geom_27700 <-> en
        LIMIT 1
    ) nearest
    WHERE ST_Distance(nearest.geom_27700, en) <= 0
);

COMMIT;
START TRANSACTION;

DO $$ BEGIN RAISE NOTICE 'Setting TOID on pixels that almost fully intersect'; END; $$;

-- Add some more pixels that almost fully intersect the buildings:
UPDATE {lidar_pixels}
SET toid = (
    SELECT toid
    FROM (
        SELECT b.geom_27700, b.toid
        FROM {buildings} b
        ORDER BY b.geom_27700 <-> en
        LIMIT 1
    ) nearest
    WHERE ST_Distance(nearest.geom_27700, en) <= {res} / 4
)
WHERE toid IS NULL;

DO $$ BEGIN RAISE NOTICE 'Creating indexes'; END; $$;

CREATE INDEX ON {lidar_pixels} (roof_plane_id);
CREATE INDEX ON {lidar_pixels} (toid);
