import functools
from enum import Enum

from psycopg2.sql import Identifier

from solar_pv import tables
from solar_pv.db_funcs import connection, sql_command


@functools.total_ordering
class Stage(Enum):
    """
    The model stages.
    """
    NOT_STARTED = 'NOT_STARTED'
    INIT = 'INIT'
    GENERATE_RASTERS = 'GENERATE_RASTERS'
    CHECK_LIDAR = 'CHECK_LIDAR'
    DETECT_ROOFS = 'DETECT_ROOFS'
    PVGIS = 'PVGIS'

    @classmethod
    @functools.lru_cache(None)
    def _member_list(cls):
        return list(cls)

    def __lt__(self, other):
        if self.__class__ is other.__class__:
            member_list = self.__class__._member_list()
            return member_list.index(self) < member_list.index(other)
        return NotImplemented


def set_stage(pg_uri: str, job_id: int, stage: Stage):
    """Set the most recently completed model stage."""
    with connection(pg_uri) as pg_conn:
        sql_command(
            pg_conn,
            """
            UPDATE {stage} SET stage = %(stage)s;
            """,
            {"stage": stage.name},
            stage=Identifier(tables.schema(job_id), tables.STAGE_TABLE))


def get_stage(pg_uri: str, job_id: int) -> Stage:
    """
    Get the most recently completed model stage.
    """
    with connection(pg_uri) as pg_conn:
        return sql_command(
            pg_conn,
            """
            SELECT * FROM {stage};
            """,
            stage=Identifier(tables.schema(job_id), tables.STAGE_TABLE),
            result_extractor=lambda rows: rows[0][0])
