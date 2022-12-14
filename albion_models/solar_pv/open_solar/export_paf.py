import logging
import os

from albion_models.db_funcs import command_to_csv_gzip

_SQL_EXPORT_CMD: str = \
"""
SELECT b.toid, p.udprn, replace(p.formatted_address, E'\n', ', ') AS address, p.text_search
FROM paf.paf p
LEFT JOIN aggregates.building b 
ON (p.udprn = ANY(b.udprns))
"""

def export(pg_conn, output_fname: str, regenerate: bool):
    if regenerate or not os.path.isfile(output_fname):
        command_to_csv_gzip(pg_conn, output_fname, _SQL_EXPORT_CMD)
    else:
        logging.info(f"Not regenerating existing {output_fname}")