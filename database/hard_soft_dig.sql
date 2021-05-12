-- Model hard/soft dig - parameterised by model_hard_soft_dig.py

--
-- Get everything from highways.road_link / highways.path_link / highways.connecting_link that intersects the bbox
--
CREATE SCHEMA {temp_schema};

CREATE TABLE {temp_schema}.road (
    road_id text PRIMARY KEY,
    road_type text NOT NULL,
    geom_4326 geometry(MultiLineString,4326) NOT NULL,
    hierarchy text NOT NULL,
    form text NOT NULL,
    beis_cost_category text NOT NULL
);

INSERT INTO {temp_schema}.road SELECT
    r.toid,
    'road_link',
    ST_Force2D(r.geom_4326),
    lower(r.routehierarchy),
    lower(r.formofway),
    lower(b.beis_cost_category)
FROM highways.road_link r
LEFT JOIN models.hsd_beis_cost_category b
    ON lower(b.hierarchy) = lower(r.routehierarchy)
    AND lower(b.form) = lower(r.formofway)
WHERE ST_Intersects(ST_GeomFromText(%(bounds)s, 4326), geom_4326);

INSERT INTO {temp_schema}.road SELECT
    r.toid,
    'path_link',
    ST_Force2D(r.geom_4326),
    'path',
    'path',
    lower(b.beis_cost_category)
FROM highways.path_link r
LEFT JOIN models.hsd_beis_cost_category b
    ON lower(b.hierarchy) = 'path'
    AND lower(b.form) = 'path'
WHERE ST_Intersects(ST_GeomFromText(%(bounds)s, 4326), geom_4326);

INSERT INTO {temp_schema}.road SELECT
    r.toid,
    'path_connecting_link',
    ST_Force2D(r.geom_4326),
    'path',
    'path',
    lower(b.beis_cost_category)
FROM highways.path_connecting_link r
LEFT JOIN models.hsd_beis_cost_category b
    ON lower(b.hierarchy) = 'path'
    AND lower(b.form) = 'path'
WHERE ST_Intersects(ST_GeomFromText(%(bounds)s, 4326), geom_4326);

CREATE INDEX ON {temp_schema}.road USING GIST (geom_4326);

--
-- Get everything from the mastermap.natural_land / greenspace.greenspace that intersects the bbox,
-- buffer the shapes using ST_Buffer(), and remove overlapping areas using ST_Union().
--
CREATE TABLE {temp_schema}.raw_soft_ground (
    geom_4326 geometry(Polygon,4326) NOT NULL
);
CREATE TABLE {temp_schema}.soft_ground (
    geom_4326 geometry(MultiPolygon,4326) NOT NULL
);

INSERT INTO {temp_schema}.raw_soft_ground
SELECT geom FROM (
    SELECT (ST_Dump(ST_Transform(ST_Buffer(ST_Transform(geom_4326, 27700), %(soft_ground_buffer_metres)s), 4326))).geom AS geom
    FROM greenspace.greenspace
    WHERE ST_Intersects(ST_GeomFromText(%(bounds)s, 4326), geom_4326)
) a;

INSERT INTO {temp_schema}.raw_soft_ground
SELECT geom FROM (
    SELECT (ST_Dump(ST_Transform(ST_Buffer(ST_Transform(geom_4326, 27700), %(soft_ground_buffer_metres)s), 4326))).geom AS geom
    FROM mastermap.natural_land
    WHERE ST_Intersects(ST_GeomFromText(%(bounds)s, 4326), geom_4326)
) a;

INSERT INTO {temp_schema}.soft_ground
SELECT ST_Multi(ST_Union(geom_4326)) AS geom_4326 FROM {temp_schema}.raw_soft_ground;

--
-- Define grid and chop up the hard and soft ground based on the gridlines:
--
CREATE TABLE {temp_schema}.grid AS
WITH bbox AS (
    SELECT
    ST_GeomFromText(%(bounds)s, 4326) as bounds,
    ST_Xmin(ST_GeomFromText(%(bounds)s, 4326)) as xmin,
    ST_Ymin(ST_GeomFromText(%(bounds)s, 4326)) as ymin,
    ST_Xmax(ST_GeomFromText(%(bounds)s, 4326)) as xmax,
    ST_Ymax(ST_GeomFromText(%(bounds)s, 4326)) as ymax
),
cells AS (
    SELECT ST_SetSRID(ST_Translate(
        ST_GeomFromText('POLYGON((0 0, 0 0.01, 0.01 0.01, 0.01 0,0 0))', 4326),
        j * 0.01 + xmin,
        i * 0.01 + ymin
    ), 4326)::geometry(polygon, 4326) AS cell
    FROM bbox,
        generate_series(0, CEIL((xmax - xmin) / 0.01)::int) AS j,
        generate_series(0, CEIL((ymax - ymin) / 0.01)::int) AS i
)
SELECT cell FROM cells, bbox WHERE ST_Intersects(cell, bounds);
CREATE INDEX ON {temp_schema}.grid USING GIST (cell);

CREATE TABLE {temp_schema}.gridded_soft_ground AS
SELECT ST_SetSRID(ST_Intersection(cell, geom_4326), 4326) AS geom_4326
FROM {temp_schema}.grid, {temp_schema}.soft_ground
WHERE ST_Intersects(cell, ST_GeomFromText(%(bounds)s, 4326));

CREATE INDEX ON {temp_schema}.gridded_soft_ground USING GIST (geom_4326);

CREATE TABLE {temp_schema}.gridded_hard_ground AS
SELECT ST_SetSRID(ST_Difference(cell, geom_4326), 4326) AS geom_4326
FROM {temp_schema}.grid, {temp_schema}.soft_ground
WHERE ST_Intersects(cell, ST_GeomFromText(%(bounds)s, 4326));

CREATE INDEX ON {temp_schema}.gridded_hard_ground USING GIST (geom_4326);

--
-- Categorise ways:
--
CREATE TABLE {temp_schema}.hard_soft_dig (
    road_id text NOT NULL,
    road_type text NOT NULL,
    is_hard_dig bool NOT NULL,
    geom_4326 geometry(MultiLineString,4326) NOT NULL,
    hierarchy text NOT NULL,
    form text NOT NULL,
    beis_cost_category text NOT NULL
) ;

-- Mark as soft dig all roads that are fully within soft ground
-- and those parts of roads that cross into soft ground.
INSERT INTO {temp_schema}.hard_soft_dig
SELECT road.road_id, road.road_type, false,
    CASE
    WHEN ST_CoveredBy(road.geom_4326, land.geom_4326)
    THEN road.geom_4326
    ELSE ST_Multi(ST_Intersection(road.geom_4326,land.geom_4326))
    END AS geom_4326,
    road.hierarchy, road.form, 'soft'
FROM {temp_schema}.road road
   INNER JOIN {temp_schema}.gridded_soft_ground land
   ON ST_Intersects(road.geom_4326, land.geom_4326);

-- Mark as hard dig all roads that are fully within hard ground
-- and those parts of roads that cross into hard ground.
INSERT INTO {temp_schema}.hard_soft_dig
SELECT road.road_id, road.road_type, true,
    CASE
    WHEN ST_CoveredBy(road.geom_4326, land.geom_4326)
    THEN road.geom_4326
    ELSE ST_Multi(ST_Intersection(road.geom_4326,land.geom_4326))
    END AS geom_4326,
    road.hierarchy, road.form, road.beis_cost_category
FROM {temp_schema}.road road
   INNER JOIN {temp_schema}.gridded_hard_ground land
   ON ST_Intersects(road.geom_4326, land.geom_4326);

INSERT INTO models.hard_soft_dig (
    road_id, job_id, soft_ground_buffer_metres, road_type, is_hard_dig, geom_4326, hierarchy, form, beis_cost_category)
SELECT
    road_id, %(job_id)s, %(soft_ground_buffer_metres)s, road_type, is_hard_dig, (ST_Dump(geom_4326)).geom, hierarchy, form, beis_cost_category
FROM {temp_schema}.hard_soft_dig;

CREATE OR REPLACE VIEW models.{model_view} AS
SELECT * FROM models.hard_soft_dig WHERE job_id = %(job_id)s;

--
-- Assert that the process has worked:
--
CREATE INDEX ON {temp_schema}.hard_soft_dig USING GIST (geom_4326);

DO $$
    DECLARE
        road_overlaps int;
        hard_over_soft int;
        soft_over_hard int;
    BEGIN
        -- test that hard roads do not overlap soft roads:
        road_overlaps := (
            SELECT COUNT(*) FROM (
                SELECT ST_Length(ST_Intersection(d1.geom_4326, d2.geom_4326)) len
                FROM {temp_schema}.hard_soft_dig d1
                    INNER JOIN {temp_schema}.hard_soft_dig d2 ON ST_Intersects(d1.geom_4326, d2.geom_4326)
                WHERE d1.is_hard_dig = true AND d2.is_hard_dig = false
            ) l WHERE len > 0
        );
        ASSERT road_overlaps = 0, format('Hard/soft dig failed: found %%s overlapping roads with different classifications', road_overlaps);

        -- test that hard roads do not overlap soft ground:
        hard_over_soft := (
            SELECT COUNT(*) FROM (
                SELECT ST_Length(ST_Intersection(d.geom_4326, sg.geom_4326)) AS len
                FROM {temp_schema}.hard_soft_dig d
                    LEFT JOIN {temp_schema}.gridded_soft_ground sg ON ST_Intersects(d.geom_4326, sg.geom_4326)
                WHERE is_hard_dig = true
            ) l WHERE len > 0.00000001
        );
        ASSERT hard_over_soft = 0, format('Hard/soft dig failed: found %%s hard-dig roads overlapping soft ground', hard_over_soft);

        -- test that soft roads do not overlap hard ground:
        soft_over_hard := (
            SELECT COUNT(*) FROM (
                SELECT ST_Length(ST_Intersection(d.geom_4326, hg.geom_4326)) AS len
                FROM {temp_schema}.hard_soft_dig d
                    LEFT JOIN {temp_schema}.gridded_hard_ground hg ON ST_Intersects(d.geom_4326, hg.geom_4326)
                WHERE is_hard_dig = false
            ) l WHERE len > 0.00000001
        );
        ASSERT soft_over_hard = 0, format('Hard/soft dig failed: found %%s soft-dig roads overlapping hard ground', soft_over_hard);
    END;
$$ LANGUAGE plpgsql;

DROP SCHEMA {temp_schema} CASCADE;
