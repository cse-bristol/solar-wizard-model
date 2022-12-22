
DROP TABLE IF EXISTS {lidar_pixels};

CREATE TABLE {lidar_pixels} (
    easting double precision,
    northing double precision,
    elevation double precision,
    aspect double precision,
    slope double precision
);
