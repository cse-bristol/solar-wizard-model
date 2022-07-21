
WITH bbox AS (
    SELECT
        ST_Buffer(bounds, coalesce((params->>'horizon_search_radius')::int, 0)) AS bounds,
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
            ST_GeomFromText('POLYGON((0 0, 0 ' || %(cell_size)s || ', ' ||
                            %(cell_size)s || ' ' || %(cell_size)s || ', ' ||
                            %(cell_size)s || ' 0,0 0))', 27700),
            j * %(cell_size)s + xmin,
            i * %(cell_size)s + ymin
        ), 27700)::geometry(polygon, 27700) AS cell,
        j * %(cell_size)s + xmin AS easting,
        i * %(cell_size)s + ymin AS northing
    FROM bbox,
        generate_series(0, CEIL((xmax - xmin) / %(cell_size)s)::int) AS j,
        generate_series(0, CEIL((ymax - ymin) / %(cell_size)s)::int) AS i
)
SELECT easting, northing FROM cells, bbox WHERE ST_Intersects(cell, bounds);