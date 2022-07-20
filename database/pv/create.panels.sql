
--
-- PV panelling:
--
DROP TABLE IF EXISTS {panel_polygons};

CREATE TABLE {panel_polygons} AS
SELECT
    models.pv_grid(
        roof_geom_27700,
        %(panel_width_m)s,
        %(panel_height_m)s,
        aspect,
        slope,
        is_flat,
        %(panel_spacing_m)s
    )::geometry(MultiPolygon, 27700) AS panel_geom_27700,
    0.0::double precision AS footprint,
    0.0::double precision AS area,
    rh.*
FROM {roof_polygons} rh;

UPDATE {panel_polygons} SET
    footprint = ST_Area(panel_geom_27700),
    area = ST_Area(panel_geom_27700) / cos(radians(slope));

UPDATE {panel_polygons} SET usable = false
WHERE usable = true AND area < %(min_roof_area_m)s;

CREATE INDEX ON {panel_polygons} USING GIST (panel_geom_27700);
ALTER TABLE {panel_polygons} ADD PRIMARY KEY (roof_plane_id);

--
-- Update building_exclusion_reasons for any buildings that have roof planes but no
-- usable ones:
--
UPDATE {building_exclusion_reasons} ber
SET exclusion_reason = 'ALL_ROOF_PLANES_UNUSABLE'
WHERE
    NOT EXISTS (SELECT FROM {panel_polygons} ph WHERE ph.usable AND ph.toid = ber.toid)
    AND ber.exclusion_reason IS NULL;

--
-- Add 3D version of panels:
--

--ALTER TABLE {panel_polygons} ADD COLUMN panel_geom_27700_3d geometry(MultiPolygonZ, 27700);
--
--UPDATE {panel_polygons} SET panel_geom_27700_3d = ST_Multi(ST_Translate(
--    ST_RotateY(
--        ST_RotateX(
--            ST_Translate(
--                ST_Force3d(ST_Scale(
--                    panel_geom_27700,
--                    ST_MakePoint(sqrt((x_coef * x_coef) + 1), sqrt((y_coef * y_coef) + 1)),
--                    ST_Centroid(panel_geom_27700))),
--                -easting, -northing),
--            atan(y_coef)),
--        atan(x_coef)),
--    easting,
--    northing,
--    (easting * x_coef) + (northing * y_coef) + intercept))::geometry(MultiPolygonZ, 27700);
--
--CREATE INDEX ON {panel_polygons} USING GIST (panel_geom_27700_3d);