-- Treat the entire bounds of the job as the bounds of one PV installation
-- (or as many installations are there are polygons in the multipolygon)

CREATE TABLE {schema}.installations AS
SELECT
    roof_geom_27700,
    row_number() OVER () AS roof_plane_id,
    row_number() OVER () AS toid
FROM (
    SELECT
        (ST_Dump(bounds)).geom AS roof_geom_27700
    FROM models.job_queue
    WHERE job_id = %(job_id)s
) a;

ALTER TABLE {schema}.installations ADD CONSTRAINT installations_pk PRIMARY KEY (roof_plane_id);

-- Table needs to match that created by the standard solar PV model (`create.roof-horizons.sql`)
CREATE TABLE {roof_horizons} AS
SELECT
    c.roof_geom_27700,
    c.roof_plane_id,
    c.toid,
    null AS x_coef,
    null AS y_coef,
    null AS intercept,
    avg(h.slope) AS slope,
    avg(h.aspect) AS aspect,
    avg(sky_view_factor) AS sky_view_factor,
    avg(percent_visible) AS percent_visible,
    ST_Area(c.roof_geom_27700) / cos(avg(h.slope)) as area,
    ST_Area(c.roof_geom_27700) as footprint,
    null AS is_flat,
    {aggregated_horizon_cols},
    null AS usable,
    ST_X(ST_Transform(ST_Centroid(c.roof_geom_27700), 27700)) AS easting,
    ST_Y(ST_Transform(ST_Centroid(c.roof_geom_27700), 27700)) AS northing,
    null AS roof_geom_27700_3d,
    null AS horizon_sd,
    null AS southerly_horizon_sd
FROM
    {schema}.installations c
    LEFT JOIN {pixel_horizons} h ON ST_Contains(c.roof_geom_27700, h.en)
GROUP BY c.roof_plane_id;

UPDATE {roof_horizons} SET
    slope = radians(%(flat_roof_degrees)s),
    aspect = radians(180),
    area = footprint / cos(radians(%(flat_roof_degrees)s))
WHERE degrees(slope) <= 5;

DROP TABLE {schema}.installations;