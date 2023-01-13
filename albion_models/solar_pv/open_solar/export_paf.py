import logging
import os

from albion_models.db_funcs import command_to_csv_gzip

# As multi-column primary keys don't work with Django ORM models, use an offset to produce a unique id and indicate an
# address in Welsh instead of English.
_WELSH_OFFSET: str = "1E9"

_SQL_EXPORT_CMD: str = \
f"""
SELECT a.toid, 
CASE 
  WHEN p.lang = 'en' THEN udprn
  ELSE udprn + {_WELSH_OFFSET}
END,
p.formatted_address AS address, p.text_search
FROM paf.paf p
LEFT JOIN addressbase.address a USING (udprn)
WHERE p.postcode not like 'BT%'
AND p.postcode not like 'GY%'
AND p.postcode not like 'JE%'
AND p.postcode not like 'IM%'
"""

def export(pg_conn, output_fname: str, regenerate: bool):
    if regenerate or not os.path.isfile(output_fname):
        command_to_csv_gzip(pg_conn, output_fname, _SQL_EXPORT_CMD)
    else:
        logging.info(f"Not regenerating existing {output_fname}")