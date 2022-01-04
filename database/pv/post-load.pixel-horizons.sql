-- Run after 320-albion-saga-gis CSV output is copied into the the table:

ALTER TABLE {pixel_horizons} ADD COLUMN en geometry(Point, 27700);
UPDATE {pixel_horizons} p SET en = ST_Transform(ST_SetSRID(ST_MakePoint(p.easting,p.northing), {srid}), 27700);
CREATE INDEX ON {pixel_horizons} USING GIST (en);
UPDATE {pixel_horizons} SET easting = ST_X(en), northing = ST_Y(en);

ALTER TABLE {pixel_horizons} ADD COLUMN pixel_id SERIAL PRIMARY KEY;
ALTER TABLE {pixel_horizons} ADD COLUMN roof_plane_id int;
ALTER TABLE {pixel_horizons} ADD COLUMN toid text;

UPDATE {pixel_horizons}
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

CREATE INDEX ON {pixel_horizons} (roof_plane_id);
CREATE INDEX ON {pixel_horizons} (toid);
