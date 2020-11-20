-- Run after 320-albion-saga-gis CSV output is copied into the the table:

ALTER TABLE {pixel_horizons} ADD COLUMN en geometry(Point, 27700);
UPDATE {pixel_horizons} p SET en = ST_SetSRID(ST_MakePoint(p.easting,p.northing), 27700);
CREATE INDEX ON {pixel_horizons} USING GIST (en);
