-- This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
-- Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
--
-- Chop up the bounds of the project into a 20,000m2 grid to stay under the
-- LIDAR API size limit.
--
CREATE TEMPORARY TABLE {grid_table} ON COMMIT DROP AS
WITH buffered_bounds AS (
    SELECT
        ST_Buffer(bounds, coalesce((params->>'horizon_search_radius')::int, 0)) AS bounds
    FROM models.job_queue
    WHERE job_id = %(job_id)s
),
bbox AS (
    SELECT
        bounds,
        ST_Xmin(bounds) as xmin,
        ST_Ymin(bounds) as ymin,
        ST_Xmax(bounds) as xmax,
        ST_Ymax(bounds) as ymax
    FROM buffered_bounds
),
cells AS (
    SELECT
        ST_SetSRID(ST_Translate(
            ST_GeomFromText('POLYGON((0 0, 0 20000, 20000 20000, 20000 0,0 0))', 27700),
            j * 20000 + xmin,
            i * 20000 + ymin
        ), 27700)::geometry(polygon, 27700) AS cell
    FROM bbox,
        generate_series(0, CEIL((xmax - xmin) / 20000)::int) AS j,
        generate_series(0, CEIL((ymax - ymin) / 20000)::int) AS i
)
SELECT cell FROM cells, bbox WHERE ST_Intersects(cell, bounds);

CREATE INDEX ON {grid_table} USING GIST (cell);

SELECT ST_AsText(a.geom) FROM (
    SELECT (ST_Dump(ST_Intersection(cell, ST_Simplify(ST_Buffer(ST_ConvexHull(bounds), 500), 500)))).geom
    FROM {grid_table}, models.job_queue
    WHERE
        job_id = %(job_id)s
        AND ST_Intersects(cell, bounds)) a;
