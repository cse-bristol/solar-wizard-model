import json
from os.path import join
from typing import List

from psycopg2.extras import DictCursor

from albion_models import paths
from albion_models.db_funcs import connection
from albion_models.solar_pv.outdated_lidar.outdated_lidar_check import _load_buildings, \
    _check_building


def check_toids_lidar(pg_uri: str, job_id: int, toids: List[str], write_test_data: bool = True):
    for toid in toids:
        check_toid_lidar(pg_uri, job_id, toid, write_test_data)


def check_toid_lidar(pg_uri: str, job_id: int, toid: str, write_test_data: bool):
    with connection(pg_uri, cursor_factory=DictCursor) as pg_conn:
        buildings = _load_buildings(pg_conn, job_id, page=0, page_size=1000, toids=[toid])
    building = buildings[0]
    reason = _check_building(building, resolution_metres=1.0, debug=True)
    if reason:
        print(f"toid {toid} excluded. Reason {reason}\n")
    else:
        print(f"toid {toid} not excluded.\n")
    if write_test_data:
        _write_test_data(toid, building)


def _write_test_data(toid, building):
    """
    Write out a test data CSV that can be used for unit tests.
    See test_oudated_lidar_check.py
    """
    lidar_test_data_dir = join(paths.TEST_DATA, "outdated_lidar")
    jsonfile = join(lidar_test_data_dir, f"{toid}.json")
    with open(jsonfile, 'w') as f:
        json.dump(building, f, sort_keys=True, default=str)


if __name__ == "__main__":
    # check_toids_lidar(
    #     "postgresql://albion_webapp:ydBbE3JCnJ4@localhost:5432/albion?application_name=blah",
    #     1617,
    #     [
    #         "osgb5000005134753286",
    #         "osgb5000005134753280",
    #         "osgb5000005134753270",
    #         "osgb5000005134753276",
    #         "osgb5000005152026792",
    #         "osgb5000005152026801",
    #         "osgb5000005235848378",
    #         "osgb5000005134753282",
    #         "osgb5000005135275129",
    #         "osgb1000020005762",
    #         # should be allowed:
    #         "osgb1000019929148",
    #         "osgb1000043085584",
    #         "osgb1000019927618",
    #         "osgb1000020002707",
    #         "osgb1000020002198",
    #         "osgb1000043085181",
    #         "osgb1000020002780",
    #     ],
    #     write_test_data=False)

    # check_toids_lidar(
    #     "postgresql://albion_webapp:ydBbE3JCnJ4@localhost:5432/albion?application_name=blah",
    #     1618,
    #     [
    #         "osgb5000005262593487",
    #         "osgb5000005262593494",
    #         "osgb5000005262592293",
    #         "osgb5000005219846721",
    #         "osgb1000002085437860",
    #     ],
    #     write_test_data=False)

    check_toids_lidar(
        "postgresql://albion_webapp:ydBbE3JCnJ4@localhost:5432/albion?application_name=blah",
        1623,
        [
            "osgb1000002085437860",
        ],
        write_test_data=True)

    # check_toids_lidar(
    #     "postgresql://albion_webapp:ydBbE3JCnJ4@localhost:5432/albion?application_name=blah",
    #     1622,
    #     [
    #         # All keep:
    #         "osgb1000021445343",
    #         "osgb1000021445346",
    #         "osgb5000005150981943",
    #     ],
    #     write_test_data=True)
