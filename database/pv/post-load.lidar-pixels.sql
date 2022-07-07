-- Run after 320-albion-saga-gis CSV output is copied into the the table:

ALTER TABLE {lidar_pixels} ADD COLUMN en geometry(Point, 27700);
UPDATE {lidar_pixels} p SET en = ST_Transform(ST_SetSRID(ST_MakePoint(p.easting,p.northing), {srid}), 27700);
CREATE INDEX ON {lidar_pixels} USING GIST (en);
UPDATE {lidar_pixels} SET easting = ST_X(en), northing = ST_Y(en);

ALTER TABLE {lidar_pixels} ADD COLUMN pixel_id SERIAL PRIMARY KEY;
ALTER TABLE {lidar_pixels} ADD COLUMN roof_plane_id int;
ALTER TABLE {lidar_pixels} ADD COLUMN toid text;

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

CREATE INDEX ON {lidar_pixels} (roof_plane_id);
CREATE INDEX ON {lidar_pixels} (toid);
