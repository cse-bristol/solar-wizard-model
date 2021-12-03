
WITH bbox AS (
    SELECT
        bounds,
        0 as xmin,
        0 as ymin,
        700000 as xmax,
        1300000 as ymax
    FROM models.job_queue
    WHERE job_id = %(job_id)s
),
cells AS (
    SELECT
        ST_SetSRID(ST_Translate(
            ST_GeomFromText('POLYGON((0 0, 0 5000, 5000 5000, 5000 0,0 0))', 27700),
            j * 5000 + xmin,
            i * 5000 + ymin
        ), 27700)::geometry(polygon, 27700) AS cell,
        j * 5000 + xmin AS easting,
        i * 5000 + ymin AS northing
    FROM bbox,
        generate_series(0, CEIL((xmax - xmin) / 5000)::int) AS j,
        generate_series(0, CEIL((ymax - ymin) / 5000)::int) AS i
)
SELECT easting, northing FROM cells, bbox WHERE ST_Intersects(cell, bounds);