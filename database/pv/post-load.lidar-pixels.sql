
ALTER TABLE {lidar_pixels} ADD COLUMN en geometry(Point, 27700);
ALTER TABLE {lidar_pixels} ADD COLUMN pixel_id SERIAL PRIMARY KEY;
ALTER TABLE {lidar_pixels} ADD COLUMN roof_plane_id int;
ALTER TABLE {lidar_pixels} ADD COLUMN toid text;

UPDATE {lidar_pixels} lp SET aspect = -9999 WHERE elevation = -9999;

COMMIT;
START TRANSACTION;

UPDATE {lidar_pixels} p SET en = ST_Transform(ST_SetSRID(ST_MakePoint(p.easting,p.northing), {srid}), 27700);
CREATE INDEX ON {lidar_pixels} USING GIST (en);

COMMIT;
START TRANSACTION;

UPDATE {lidar_pixels} lp
SET toid = b.toid
FROM {buildings} b
WHERE ST_Intersects(b.geom_27700, lp.en);

COMMIT;
START TRANSACTION;

-- Add some more pixels that almost fully intersect the buildings:
UPDATE {lidar_pixels} lp
SET toid = b.toid
FROM {buildings} b
WHERE ST_DWithin(b.geom_27700, lp.en, 0.25) and lp.toid is null;

CREATE INDEX ON {lidar_pixels} (roof_plane_id);
CREATE INDEX ON {lidar_pixels} (toid);
