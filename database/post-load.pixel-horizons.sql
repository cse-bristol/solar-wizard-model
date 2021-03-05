-- Run after 320-albion-saga-gis CSV output is copied into the the table:

ALTER TABLE {pixel_horizons} ADD COLUMN en geometry(Point, {srid});
UPDATE {pixel_horizons} p SET en = ST_Transform(ST_SetSRID(ST_MakePoint(p.easting,p.northing), {srid}), 27700);
CREATE INDEX ON {pixel_horizons} USING GIST (en);
UPDATE {pixel_horizons} SET easting = ST_X(en), northing = ST_Y(en);

ALTER TABLE {pixel_horizons} ADD COLUMN pixel_id SERIAL PRIMARY KEY;
ALTER TABLE {pixel_horizons} ADD COLUMN roof_plane_id int;

CREATE INDEX ON {pixel_horizons} (roof_plane_id);