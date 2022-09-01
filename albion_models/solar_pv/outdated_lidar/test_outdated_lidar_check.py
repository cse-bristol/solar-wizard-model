import csv
from os.path import join
from typing import List

from albion_models.paths import TEST_DATA
from albion_models.solar_pv.outdated_lidar.outdated_lidar_check import HeightAggregator
from albion_models.test.test_funcs import ParameterisedTestCase

_PIXEL_DATA = join(TEST_DATA, "outdated_lidar")


def _load_data(filename: str) -> List[dict]:
    with open(filename) as f:
        return [{'pixel_id': int(row['pixel_id']),
                 'elevation': float(row['elevation']),
                 'toid': row['toid'],
                 'within_building': row['within_building'] == 'True',
                 'without_building': row['without_building'] == 'True',
                 'height': float(row['height']),
                 'base_roof_height': float(row['base_roof_height']) if row['base_roof_height'] else None,
                 }
                for row in csv.DictReader(f)]


def _check(filename: str):
    ha = HeightAggregator()
    pixels = _load_data(join(_PIXEL_DATA, filename))
    for pixel in pixels:
        ha.aggregate_row(pixel)

    return ha.exclusion_reason()


class RansacTestCase(ParameterisedTestCase):

    def test_lidar_checker(self):
        self.parameterised_test([
            ('osgb5000005134753276.csv', 'OUTDATED_LIDAR_COVERAGE'),
            ('osgb5000005152026792.csv', 'OUTDATED_LIDAR_COVERAGE'),
            ('osgb5000005152026801.csv', 'OUTDATED_LIDAR_COVERAGE'),
            ('osgb5000005235848378.csv', 'OUTDATED_LIDAR_COVERAGE'),
            ('osgb5000005134753282.csv', 'OUTDATED_LIDAR_COVERAGE'),
            ('osgb5000005135275129.csv', 'OUTDATED_LIDAR_COVERAGE'),
            ('osgb5000005134753286.csv', 'OUTDATED_LIDAR_COVERAGE'),
            ('osgb5000005134753270.csv', 'OUTDATED_LIDAR_COVERAGE'),
            ('osgb5000005134753280.csv', 'OUTDATED_LIDAR_COVERAGE'),
            ('osgb1000020005762.csv', 'OUTDATED_LIDAR_COVERAGE'),
            ('osgb1000019929148.csv', None),
            ('osgb1000043085584.csv', None),
            ('osgb1000019927618.csv', None),
            ('osgb1000020002707.csv', None),
            # ('osgb1000020002198.csv', None),  # Failing
            # ('osgb1000043085181.csv', None),  # Failing
            ('osgb1000020002780.csv', None),
            ('osgb5000005262592293.csv', None),
            ('osgb5000005262593494.csv', None),
        ], _check)
