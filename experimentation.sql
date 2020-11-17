-- experimentation

CREATE TABLE models.aspect_centroids AS
SELECT
    ogc_fid,
    wkb_geometry,
    ST_SetSRID(ST_Centroid(wkb_geometry), 27700)::geometry(point, 27700) AS centroid,
    DN AS aspect,
    ST_Area(wkb_geometry) AS area
FROM aspect_polygons
WHERE ST_Area(wkb_geometry) >= 10;

ALTER TABLE models.aspect_centroids ADD PRIMARY KEY(ogc_fid);


CREATE TABLE horizons (
    x bigint,
    y bigint,
    easting double precision,
    northing double precision,
    slope double precision,
    aspect double precision,
    sky_view_factor double precision,
    percent_visible double precision,
    angle_rad_0 double precision,
    angle_rad_45 double precision,
    angle_rad_90 double precision,
    angle_rad_135 double precision,
    angle_rad_180 double precision,
    angle_rad_225 double precision,
    angle_rad_270 double precision,
    angle_rad_315 double precision
);

COPY horizons FROM '/csv_out.csv' (FORMAT 'csv', HEADER);

SELECT AddGeometryColumn ('horizons','en',27700,'POINT',2);
UPDATE horizons p SET en = ST_SetSRID(ST_MakePoint(p.easting,p.northing), 27700);
CREATE INDEX ON horizons USING GIST (en);

CREATE TABLE h2 AS
SELECT
    ogc_fid,
    avg(h.slope) AS slope,
    avg(h.aspect) AS aspect,
    ST_X(centroid) AS easting,
    ST_Y(centroid) AS northing,
    max(angle_rad_0) AS angle_rad_0,
    max(angle_rad_45) AS angle_rad_45,
    max(angle_rad_90) AS angle_rad_90,
    max(angle_rad_135) AS angle_rad_135,
    max(angle_rad_180) AS angle_rad_180,
    max(angle_rad_225) AS angle_rad_225,
    max(angle_rad_270) AS angle_rad_270,
    max(angle_rad_315) AS angle_rad_315
FROM
    models.aspect_centroids c
    LEFT JOIN horizons h ON ST_Contains(c.wkb_geometry, h.en)
GROUP BY ogc_fid;
