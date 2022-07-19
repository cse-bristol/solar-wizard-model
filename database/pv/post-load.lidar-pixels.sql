
ALTER TABLE {lidar_pixels} ADD COLUMN aspect double precision;
ALTER TABLE {lidar_pixels} ADD COLUMN slope double precision;
CREATE INDEX lidar_pixels_temp_idx ON {lidar_pixels} (easting, northing);

UPDATE {lidar_pixels} lp SET aspect = a.aspect
FROM {aspect_pixels} a
WHERE lp.easting = a.easting AND lp.northing = a.northing;

UPDATE {lidar_pixels} lp SET slope = s.slope
FROM {slope_pixels} s
WHERE lp.easting = s.easting AND lp.northing = s.northing;

DROP TABLE {aspect_pixels};
DROP TABLE {slope_pixels};
DROP INDEX {schema}.lidar_pixels_temp_idx;

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
