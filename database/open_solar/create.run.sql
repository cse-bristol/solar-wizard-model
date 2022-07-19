
INSERT INTO models.job_queue (
    project,
    bounds,
    solar_pv,
    params,
    open_solar)
WITH whole_country AS (
    SELECT ST_Transform(ST_Collect(geom_4326), 27700) AS geom_27700
    FROM boundaryline.local_authority
),
bbox AS (
    SELECT
        geom_27700,
        ST_Xmin(geom_27700) as xmin,
        ST_Ymin(geom_27700) as ymin,
        ST_Xmax(geom_27700) as xmax,
        ST_Ymax(geom_27700) as ymax
    FROM whole_country
),
cells AS (
    SELECT
        ST_SetSRID(ST_Translate(
            ST_GeomFromText('POLYGON((0 0, 0 ' || {cell_size} || ', ' ||
                            {cell_size} || ' ' || {cell_size} || ', ' ||
                            {cell_size} || ' 0,0 0))', 27700),
            j * {cell_size} + xmin,
            i * {cell_size} + ymin
        ), 27700)::geometry(polygon, 27700) AS cell
    FROM bbox,
        generate_series(0, CEIL((xmax - xmin) / {cell_size})::int) AS j,
        generate_series(0, CEIL((ymax - ymin) / {cell_size})::int) AS i
)
SELECT
    'open_solar:' || %(name)s,
    ST_Multi(cell),
    true,
    %(params)s,
    true
FROM cells, bbox WHERE ST_Intersects(cell, geom_27700)
RETURNING job_id;