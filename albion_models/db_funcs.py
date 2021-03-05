from os.path import join

import psycopg2
from psycopg2.sql import SQL, Identifier

from albion_models.paths import SQL_DIR

PG_NULL = "\\N"


def sql_script(pg_conn, script_name: str, **kwargs):
    """Run one of the SQL scripts in the `database` directory"""
    with pg_conn.cursor() as cursor:
        with open(join(SQL_DIR, script_name)) as schema_file:
            cursor.execute(SQL(schema_file.read()).format(**kwargs))
            pg_conn.commit()


def sql_script_with_bindings(pg_conn, script_name: str, bindings: dict, **kwargs):
    """Run one of the SQL scripts in the `database` directory, with named
    prepared-statement bindings. """
    with pg_conn.cursor() as cursor:
        with open(join(SQL_DIR, script_name)) as schema_file:
            cursor.execute(SQL(schema_file.read()).format(**kwargs), bindings)
            pg_conn.commit()


def copy_tsv(pg_conn, file_name: str, table: str, sep='\t', null=PG_NULL, encoding='utf-8'):
    """Using the postgres COPY command, load data into a table from a TSV file."""
    with pg_conn.cursor() as cursor:
        with open(file_name, encoding=encoding) as f:
            cursor.copy_from(f, table, sep=sep, null=null)
            pg_conn.commit()


def copy_csv(pg_conn, file_name: str, table: str, encoding='utf-8'):
    with pg_conn.cursor() as cursor:
        with open(file_name, encoding=encoding) as f:
            copy_sql = SQL("COPY {} FROM stdin (FORMAT 'csv', HEADER)").format(Identifier(*table.split(".")))
            cursor.copy_expert(copy_sql, f)
            pg_conn.commit()


def to_csv(pg_conn, file_name: str, table: str, encoding='utf-8'):
    with pg_conn.cursor() as cursor:
        with open(file_name, 'w', encoding=encoding) as f:
            copy_sql = SQL("COPY {} TO stdin (FORMAT 'csv', HEADER)").format(Identifier(*table.split(".")))
            cursor.copy_expert(copy_sql, f)
            pg_conn.commit()


def process_pg_uri(pg_uri: str) -> str:
    """
    Some versions of ogr2ogr attempt to add an 'application name' parameter
    to the connection string, assuming it is the 'key=value' form of postgres
    connection string. This mangles any URI-form connection strings which do not
    contain the text 'application_name'.
    """
    if 'application_name' in pg_uri:
        return pg_uri

    from urllib.parse import urlparse
    parsed = urlparse(pg_uri)
    if parsed.scheme == '':
        # Not a URI, probably the 'key=value' form of PG connection string:
        return pg_uri + ' ' + 'application_name=albion_models'

    if len(parsed.query) == 0:
        return parsed._replace(query='application_name=albion_models').geturl()
    else:
        return parsed._replace(query=f'{parsed.query}&application_name=albion_models').geturl()


def connect(pg_uri: str, **kwargs):
    return psycopg2.connect(process_pg_uri(pg_uri), **kwargs)


def count(pg_uri: str, schema: str, table: str) -> int:
    pg_conn = connect(pg_uri)
    try:
        with pg_conn.cursor() as cursor:
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = %(schema)s
                    AND table_name = %(table)s
                );
            """, {"schema": schema, "table": table})
            exists = cursor.fetchone()[0] is True
            if not exists:
                return 0
            cursor.execute(SQL("SELECT COUNT(*) FROM {table} LIMIT 1").format(
                table=Identifier(schema, table)
            ))
            return cursor.fetchone()[0]
    finally:
        pg_conn.close()
