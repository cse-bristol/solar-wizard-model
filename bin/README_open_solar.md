# Using the open_solar shell script

Some possibly useful command lines

## Help
```commandline
cd 320-albion-models
bin/open_solar --help
```

## Export base data
```commandline
bin/open_solar extract \
--pg_uri postgresql://albion_ddl:*****@shared-pg.r.cse.org.uk:5432/cse \
--gpkg_dir /srv/projects/710-opensolar/base_info \
--extract_base_info
```

## Export job data
### To find job ids...
Use sql:
```sql
SELECT os_run_id, min(job_id) min_job_id, max(job_id) max_job_id FROM models.job_queue
JOIN models.open_solar_jobs using (job_id)
WHERE status::text = 'COMPLETE'
group by os_run_id
order by os_run_id
```

### Extract
If a job extract fails, a gpkg will remain with an "exp." prefix.

*Examples:*  
All jobs, don't replace existing data, will extract jobs 
where there is no existing gpkg output file
```commandline
nohup bin/open_solar extract \
--pg_uri postgresql://albion_ddl:*****@shared-pg.r.cse.org.uk:5432/cse \
--gpkg_dir /srv/projects/710-opensolar/job_results \
--extract_job_info \
7 > /srv/projects/710-opensolar/JobExport.log 2>&1 &
```

With range of job ids, don't replace existing data, will extract jobs 
where there is no existing gpkg output file
```commandline
nohup bin/open_solar extract \
--pg_uri postgresql://albion_ddl:*****@shared-pg.r.cse.org.uk:5432/cse \
--gpkg_dir /srv/projects/710-opensolar/job_results \
--start_job_id 210 --end_job_id 509 \
--extract_job_info \
7 > /srv/projects/710-opensolar/JobExport210509.log 2>&1 &
```

All jobs and update existing outputs with new data
```commandline
nohup bin/open_solar extract \
--pg_uri postgresql://albion_ddl:*****@shared-pg.r.cse.org.uk:5432/cse \
--gpkg_dir /srv/projects/710-opensolar/job_results \
--extract_job_info \
--regenerate \
7 > /srv/projects/710-opensolar/JobExport.log 2>&1 &
```