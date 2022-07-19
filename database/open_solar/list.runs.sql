
SELECT
    osr.os_run_id,
    MAX(osr.name) AS name, -- no-op agg
    MAX(osr.created_at) AS created_at, -- no-op agg
    COUNT(*) AS total,

    COUNT(*) FILTER (WHERE q.status = 'NOT_STARTED') AS not_started,
    COUNT(*) FILTER (WHERE q.status = 'IN_PROGRESS') AS in_progress,
    COUNT(*) FILTER (WHERE q.status = 'COMPLETE') AS complete,
    COUNT(*) FILTER (WHERE q.status = 'FAILED') AS failed,
    COUNT(*) FILTER (WHERE q.status = 'CANCELLED') AS cancelled,

    COUNT(*) FILTER (WHERE q.status = 'NOT_STARTED') / COUNT(*)::float AS not_started_pct,
    COUNT(*) FILTER (WHERE q.status = 'IN_PROGRESS') / COUNT(*)::float AS in_progress_pct,
    COUNT(*) FILTER (WHERE q.status = 'COMPLETE') / COUNT(*)::float AS complete_pct,
    COUNT(*) FILTER (WHERE q.status = 'FAILED') / COUNT(*)::float AS failed_pct,
    COUNT(*) FILTER (WHERE q.status = 'CANCELLED') / COUNT(*)::float AS cancelled_pct
FROM models.open_solar_run osr
LEFT JOIN models.open_solar_jobs osj ON osr.os_run_id = osj.os_run_id
LEFT JOIN models.job_queue q ON osj.job_id = q.job_id
GROUP BY osr.os_run_id;
