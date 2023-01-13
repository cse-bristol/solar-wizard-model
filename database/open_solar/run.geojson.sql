
SELECT
    osr.os_run_id,
    osr.name AS os_run_name,
    osr.created_at AS os_run_created_at,
    q.job_id,
    q.project,
    q.created_at,
    q.started_at,
    q.finished_at,
    ST_AsGeoJSON(ST_Transform(q.bounds, 4326))::json AS geojson,
    q.status,
    q.error,
    q.params
FROM
    models.open_solar_run osr
    LEFT JOIN models.open_solar_jobs osj ON osr.os_run_id = osj.os_run_id
    LEFT JOIN models.job_queue q ON osj.job_id = q.job_id
WHERE
    q.archived = false
    AND q.open_solar
    AND osr.os_run_id = {os_run_id}
ORDER BY q.created_at DESC;
