
-- TODO change columns if needed - see create.lidar-pixels.sql for approach to loading
--  multiple rasters into the same table
DROP TABLE IF EXISTS {solar_pv};
CREATE TABLE {solar_pv} (
    easting double precision,
    northing double precision,
    kwh_year real,
    aoi_loss_percentage real,
    spectral_loss_percentage text,
    temp_irr_loss_percentage real,
    total_loss_percentage real,
    PRIMARY KEY (easting, northing)
);
