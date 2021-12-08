import csv
import unittest
from os.path import join
from typing import List

from albion_models.paths import TEST_DATA
from albion_models.solar_pv.ransac.run_ransac import _ransac_building

_RANSAC_DATA = join(TEST_DATA, "ransac")


def _load_data(filename: str) -> List[dict]:
    with open(filename) as f:
        return [{k: float(v) if k != 'pixel_id' else int(v) for k, v in row.items()}
                for row in csv.DictReader(f)]


class RansacTestCase(unittest.TestCase):

    def test_ransac_end_terrace(self):
        planes = _ransac_building(_load_data(join(_RANSAC_DATA, 'end_terrace.csv')), 'toid', 1)
        assert len(planes) == 4, f"\nExpected: {4}\nActual  : {len(planes)}"

    def test_ransac_all_one_plane(self):
        planes = _ransac_building(_load_data(join(_RANSAC_DATA, 'all_one_plane.csv')), 'toid', 1)
        assert len(planes) == 1, f"\nExpected: {1}\nActual  : {len(planes)}"
