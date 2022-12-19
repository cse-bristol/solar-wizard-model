import gzip
import os
import shlex

import subprocess

import logging

from contextlib import ExitStack, contextmanager
from os.path import join
from typing import Union, Optional

import psycopg2
from psycopg2.sql import SQL, Identifier, Composed

from albion_models.paths import SQL_DIR

PG_NULL = "\\N"


def sql_command(pg_conn, command: Union[str, Composed], bindings: dict = None, result_extractor=None, **kwargs):
    """
    Execute a SQL command, with optional named prepared-statement bindings
    (https://www.psycopg.org/docs/usage.html#query-parameters).

    The SQL will be interpolated with the keyword args in the standard psycopg2 way
    (https://www.psycopg.org/docs/sql.html#module-psycopg2.sql).
    """
    with ExitStack() as stack:
        if hasattr(pg_conn, 'cursor'):
            cursor = stack.enter_context(pg_conn.cursor())
            in_tx = False
        else:
            cursor = pg_conn
            in_tx = True

        if len(kwargs) != 0:
            if isinstance(command, str):
                command = SQL(command).format(**kwargs)
            else:
                command = command.format(**kwargs)

        if bindings is not None:
            cursor.execute(command, bindings)
        else:
            cursor.execute(command)

        if not in_tx:
            pg_conn.commit()
        if result_extractor is not None:
            return result_extractor(cursor.fetchall())


def sql_script(pg_conn, script_name: str, bindings: dict = None, result_extractor=None, **kwargs):
    """
    Run one of the SQL scripts in the `sql` directory, with optional named
    prepared-statement bindings (https://www.psycopg.org/docs/usage.html#query-parameters).

    The SQL will be interpolated with the keyword args in the standard psycopg2 way
    (https://www.psycopg.org/docs/sql.html#module-psycopg2.sql).
    """
    with open(join(SQL_DIR, script_name)) as schema_file:
        return sql_command(pg_conn, schema_file.read(), bindings, result_extractor, **kwargs)


def command_to_gpkg(pg_conn,
                    pg_uri: str,
                    filename: str,
                    table_name: str,
                    command: Union[str, SQL],
                    src_srs: int,
                    dst_srs: int,
                    overwrite: bool = False,
                    append: bool = False,
                    **kwargs) -> Optional[str]:
    logging.info(f"Loading {table_name} into {filename}")
    path = join(os.environ.get("GPKG_DIR", ""), filename)  # If env var is not set just use filename
    exists = os.path.exists(path)
    if overwrite and append:
        overwrite = False

    if len(kwargs) != 0:
        if isinstance(command, str):
            command = SQL(command).format(**kwargs).as_string(pg_conn)
        else:
            command = command.format(**kwargs).as_string(pg_conn)

    res = subprocess.run(shlex.split(
        f"""ogr2ogr
        -f GPKG {path}
        -sql "{command}"
        -gt 65536
        -nln {table_name}
        {"-update" if exists else ""}
        {"-overwrite" if overwrite else ""}
        {"-append" if append else ""}
        -s_srs EPSG:{src_srs}
        -t_srs EPSG:{dst_srs}
        "PG:{process_pg_uri(pg_uri)}"
        """),
        capture_output=True, text=True)

    # TODO consider:
    # Re message from Neil Nov 22: I wonder if some of the performance hints mentioned here:
    # https://gdal.org/drivers/vector/gpkg.html#performance-hints might be worth trying? They sound like they wouldn't
    # be compatible with concurrent writes but it sounds like that hasn't been working anyway. We saw quite good
    # performance increases in HNZP when writing geopackages with the following:
    # (.setJournalMode SQLiteConfig$JournalMode/WAL)
    # (.setPragma SQLiteConfig$Pragma/SYNCHRONOUS "OFF")
    # (.setTransactionMode SQLiteConfig$TransactionMode/DEFERRED)
    # (.setReadUncommited true)

    if res.stdout:
        logging.info(res.stdout)
    if res.returncode != 0:
        logging.error(res.stderr)
        return res.stderr

    return None


# TODO remove
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
    """Using the postgres COPY command, export a table to a CSV file."""
    with pg_conn.cursor() as cursor:
        with open(file_name, 'w', encoding=encoding) as f:
            copy_sql = SQL("COPY {} TO stdin (FORMAT 'csv', HEADER)").format(Identifier(*table.split(".")))
            cursor.copy_expert(copy_sql, f)
            pg_conn.commit()

def _command_to_csv(pg_conn, file, command: str, **kwargs):
    """Using the postgres COPY command, export the output of a SQL command to a file"""
    with pg_conn.cursor() as cursor:
        copy_sql = SQL("COPY ({}) TO stdin (FORMAT 'csv', HEADER)").format(SQL(command).format(**kwargs))
        cursor.copy_expert(copy_sql, file)
        pg_conn.commit()

def command_to_csv(pg_conn, file_name: str, command: str, encoding='utf-8', **kwargs):
    """Using the postgres COPY command, export the output of a SQL command to a CSV file."""
    with open(file_name, 'w', encoding=encoding) as f:
        _command_to_csv(pg_conn, f, command, **kwargs)

def command_to_csv_gzip(pg_conn, file_name: str, command: str, **kwargs):
    """Using the postgres COPY command, export the output of a SQL command to a gzipped CSV file."""
    with gzip.open(file_name, 'w') as f:
        _command_to_csv(pg_conn, f, command, **kwargs)


def script_to_csv(pg_conn, file_name: str, script: str, encoding='utf-8', **kwargs):
    with open(join(SQL_DIR, script)) as schema_file:
        return command_to_csv(pg_conn, file_name, schema_file.read(), encoding, **kwargs)


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


@contextmanager
def connection(pg_uri: str, **kwargs):
    pg_conn = connect(pg_uri, **kwargs)
    try:
        yield pg_conn
    finally:
        pg_conn.close()


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
