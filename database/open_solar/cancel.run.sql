
UPDATE models.job_queue q SET status = 'CANCELLED'
FROM models.open_solar_jobs osj
WHERE
    status = 'NOT_STARTED'
    AND osj.job_id = q.job_id
    AND osj.os_run_id = {os_run_id}
