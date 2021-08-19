CREATE TABLE {pixel_horizons} (
    x bigint,
    y bigint,
    easting double precision,
    northing double precision,
    elevation double precision,
    slope double precision,
    aspect double precision,
    sky_view_factor double precision,
    percent_visible double precision,
    {horizon_cols}
);
