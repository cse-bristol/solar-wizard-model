import json
import logging
import os
import shutil
from os.path import join

import unittest
from typing import List
from unittest import mock

from albion_models.lidar.get_lidar import _get_lidar, _find_best, _wkt_to_rings
from albion_models.paths import PROJECT_ROOT

_lidar_dir = join(PROJECT_ROOT, "tmp")


def mocked_requests_get(*args, **kwargs):
    class MockResponse:
        def __init__(self, json_data, status_code: int, content=None):
            self.json_data = json_data
            self.status_code = status_code
            self.content = content

        def json(self):
            return self.json_data

        def raise_for_status(self):
            pass

    url = args[0]
    job_id = 'TEST_JOB_ID'
    # Start job:
    if url == 'https://environment.data.gov.uk/arcgis/rest/services/gp/DataDownload/GPServer/DataDownload/submitJob':
        return MockResponse({"jobId": job_id}, 200)

    # Check job status:
    elif url == f'https://environment.data.gov.uk/arcgis/rest/services/gp/DataDownload/GPServer/DataDownload/jobs/{job_id}':
        return MockResponse({"jobStatus": "esriJobSucceeded"}, 200)

    # Get tile URLs:
    elif url == f'https://environment.data.gov.uk/arcgis/rest/directories/arcgisjobs/gp/datadownload_gpserver/{job_id}/scratch/results.json':
        with open(join(PROJECT_ROOT, "testdata", "lidar_urls.json")) as f:
            return MockResponse(json.load(f), 200)

    # get tile zips:
    elif url.startswith('https://environment.data.gov.uk/UserDownloads/interactive/'):
        with open(join(PROJECT_ROOT, "testdata", "test_lidar_data.zip"), 'rb') as f:
            return MockResponse(None, 200, f.read())

    return MockResponse(None, 404)


class LidarTestCase(unittest.TestCase):

    @mock.patch('requests.get', side_effect=mocked_requests_get)
    def test_create_tiffs(self, mock_get):
        tiffs = _get_lidar([[]], _lidar_dir)
        self._assert_tiffs(["tl3555_DSM_1M.tiff", "tl3556_DSM_2M.tiff"], tiffs)

    @mock.patch('requests.get', side_effect=mocked_requests_get)
    def test_dont_redownload_same_year(self, mock_get):
        os.makedirs(_lidar_dir, exist_ok=True)
        self._create_zip_file("2017-LIDAR-DSM-1M-TL35ne.zip")

        tiffs = _get_lidar([[]], _lidar_dir)

        self.assertNotIn(
            mock.call('https://environment.data.gov.uk/UserDownloads/interactive/5fe820254ea24f048900ea8d94dfdaa345872/LIDARCOMP/LIDAR-DSM-1M-TL35ne.zip'),
            mock_get.call_args_list)
        self._assert_tiffs(["tl3555_DSM_1M.tiff", "tl3556_DSM_2M.tiff"], tiffs)

    @mock.patch('requests.get', side_effect=mocked_requests_get)
    def test_dont_overwrite_newer_files(self, mock_get):
        os.makedirs(_lidar_dir, exist_ok=True)
        self._create_zip_file("2018-LIDAR-DSM-1M-TL35ne.zip")

        tiffs = _get_lidar([[]], _lidar_dir)

        self.assertNotIn(
            mock.call('https://environment.data.gov.uk/UserDownloads/interactive/5fe820254ea24f048900ea8d94dfdaa345872/LIDARCOMP/LIDAR-DSM-1M-TL35ne.zip'),
            mock_get.call_args_list)
        self._assert_tiffs(["tl3555_DSM_1M.tiff", "tl3556_DSM_2M.tiff"], tiffs)

    @mock.patch('requests.get', side_effect=mocked_requests_get)
    def test_handle_existing_tiffs_from_old_approach(self, mock_get):
        os.makedirs(_lidar_dir, exist_ok=True)
        self._create_zip_file("2018-LIDAR-DSM-1M-TL35ne.zip")
        self._create_file("2018_tl3555_DSM_1M.tiff")
        self._create_file("2018_tl3556_DSM_1M.tiff")

        tiffs = _get_lidar([[]], _lidar_dir)

        self.assertNotIn(
            mock.call('https://environment.data.gov.uk/UserDownloads/interactive/5fe820254ea24f048900ea8d94dfdaa345872/LIDARCOMP/LIDAR-DSM-1M-TL35ne.zip'),
            mock_get.call_args_list)
        self._assert_tiffs(["tl3555_DSM_1M.tiff", "tl3556_DSM_2M.tiff"], tiffs)

    @mock.patch('requests.get', side_effect=mocked_requests_get)
    def test_prefer_1m(self, mock_get):
        _get_lidar([[]], _lidar_dir)

        self.assertIn(
            mock.call('https://environment.data.gov.uk/UserDownloads/interactive/5fe820254ea24f048900ea8d94dfdaa345872/LIDARCOMP/LIDAR-DSM-1M-TL35ne.zip'),
            mock_get.call_args_list)

    # Makes real API calls:
    # def test_get_lidar(self):
    #     get_lidar(538822.036345393, 251052.546217778, 539221.042792384, 265279.552500898, _lidar_dir)
    #     tiffs = os.listdir(_lidar_dir)
    #     assert len(tiffs) == 100, f"Wanted 100 tiffs, found {len(tiffs)}:\n {tiffs}"

    def test_find_best(self):
        self._parameterised_test([
            (['2015-LIDAR-DSM-2M-TL35ne.zip'], '2015-LIDAR-DSM-2M-TL35ne.zip'),
            (['2015-LIDAR-DSM-2M-TL35ne.zip', '2015-LIDAR-DSM-2M-TL35ne.zip'], '2015-LIDAR-DSM-2M-TL35ne.zip'),
            (['2015-LIDAR-DSM-2M-TL35ne.zip', '2015-LIDAR-DSM-1M-TL35ne.zip'], '2015-LIDAR-DSM-1M-TL35ne.zip'),
            (['2014-LIDAR-DSM-2M-TL35ne.zip', '2015-LIDAR-DSM-2M-TL35ne.zip'], '2015-LIDAR-DSM-2M-TL35ne.zip'),
            (['2014-LIDAR-DSM-1M-TL35ne.zip', '2015-LIDAR-DSM-2M-TL35ne.zip'], '2014-LIDAR-DSM-1M-TL35ne.zip'),
        ], _find_best)

    def test_wkt_to_rings(self):
        self._parameterised_test([
            ('POLYGON((417649.533067673 206504.504705884,417649.533067673 226504.504705884,426447.445894151 226504.504705884,417649.533067673 206504.504705884))',
             [
                [417649.533067673, 206504.504705884],
                [417649.533067673, 226504.504705884],
                [426447.445894151, 226504.504705884],
                [417649.533067673, 206504.504705884],
             ]),
            ('POINT(0 1)', [])
        ], _wkt_to_rings)

    def _create_file(self, name: str):
        open(join(_lidar_dir, name), 'w').close()

    def _create_zip_file(self, name: str):
        from shutil import copyfile
        copyfile(join(PROJECT_ROOT, "testdata", "test_lidar_data.zip"), join(_lidar_dir, name))

    def _parameterised_test(self, mapping: List[tuple], fn):
        for tup in mapping:
            expected = tup[-1]
            actual = fn(*tup[:-1])
            assert expected == actual, f"\n{tup[:-1]}\nExpected: {expected}\nActual  : {actual}"

    def _assert_tiffs(self, expected: List[str], ret_value: List[str]):
        actual = [f for f in os.listdir(_lidar_dir) if f.endswith(".tiff")]
        assert len(actual) == len(expected) == len(ret_value), f"{expected} length != {ret_value} length != {actual} length"
        for name in expected:
            assert name in actual
            assert join(_lidar_dir, name) in ret_value

    def setUp(self):
        logging.basicConfig(level=logging.DEBUG, format='[%(asctime)s] %(levelname)s: %(message)s')

    def tearDown(self):
        try:
            shutil.rmtree(_lidar_dir)
        except FileNotFoundError:
            pass