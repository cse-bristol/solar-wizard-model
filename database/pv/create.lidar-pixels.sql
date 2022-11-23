
DROP TABLE IF EXISTS {aspect_pixels};
DROP TABLE IF EXISTS {slope_pixels};
DROP TABLE IF EXISTS {lidar_pixels};

CREATE TABLE {aspect_pixels} (
    easting double precision,
    northing double precision,
    aspect double precision,
    PRIMARY KEY (easting, northing)
);

CREATE TABLE {slope_pixels} (
    easting double precision,
    northing double precision,
    slope double precision,
    PRIMARY KEY (easting, northing)
);

CREATE TABLE {lidar_pixels} (
    easting double precision,
    northing double precision,
    elevation double precision
);
