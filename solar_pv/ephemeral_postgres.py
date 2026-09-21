# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
"""A throwaway PostGIS cluster, so the CLI can run without a pre-existing database."""
import logging
from contextlib import contextmanager

try:
    import testing.postgresql
    _HAS_TESTING_POSTGRESQL = True
except ModuleNotFoundError:
    _HAS_TESTING_POSTGRESQL = False

from solar_pv.db_funcs import connection


@contextmanager
def ephemeral_postgres(keep: bool = False):
    """
    Yield a `pg_uri` for a throwaway PostGIS cluster with the postgis + raster
    extensions installed. The cluster is stopped (and its data directory removed)
    on exit, unless `keep` is set - then it is left running until the user presses
    enter, so its schema can be inspected.

    The transforms it does are only correct inside the project's nix-shell, whose
    proj carries the OSTN15 grids (see `check_proj_datumgrid`).
    """
    if not _HAS_TESTING_POSTGRESQL:
        raise RuntimeError(
            "testing.postgresql is needed to run without --pg-uri; either install "
            "it or pass --pg-uri pointing at an existing postGIS database.")

    pg = testing.postgresql.Postgresql()
    try:
        with connection(pg.url()) as pg_conn, pg_conn.cursor() as cursor:
            cursor.execute("CREATE EXTENSION postgis; CREATE EXTENSION postgis_raster;")
            pg_conn.commit()
        logging.info(f"Started ephemeral postgres at {pg.url()}")
        yield pg.url()
    finally:
        if keep:
            input(f"Ephemeral postgres left running at {pg.url()} - "
                  f"press enter to shut it down...")
        logging.info("Stopping ephemeral postgres")
        pg.stop()
