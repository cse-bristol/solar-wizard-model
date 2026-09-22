# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
from solar_pv.lidar import lidar
from solar_pv.test_utils.test_funcs import ParameterisedTestCase


class LidarTestCase(ParameterisedTestCase):

    def test_ZippedTiles_from_file(self):
        self.parameterised_test([
            (
                "2017-LIDAR-DSM-2M-TL35ne.zip", None, "test",
                lidar.ZippedTiles(
                    zip_id="TL35ne",
                    year=2017,
                    resolution=lidar.Resolution.R_2M,
                    url=None,
                    filename="2017-LIDAR-DSM-2M-TL35ne.zip",
                    product="test")),
            (
                "2017-LIDAR-DSM-1M-TL35ne.zip", None, "test",
                lidar.ZippedTiles(
                    zip_id="TL35ne",
                    year=2017,
                    resolution=lidar.Resolution.R_1M,
                    url=None,
                    filename="2017-LIDAR-DSM-1M-TL35ne.zip",
                    product="test")),
            (
                "2017-LIDAR-DSM-50CM-TL35ne.zip", None, "test",
                lidar.ZippedTiles(
                    zip_id="TL35ne",
                    year=2017,
                    resolution=lidar.Resolution.R_50CM,
                    url=None,
                    filename="2017-LIDAR-DSM-50CM-TL35ne.zip",
                    product="test")),
        ], lidar.ZippedTiles.from_filename)

    def test_LidarTile_from_file(self):
        self.parameterised_test([
            (
                "/path/to/lidar/sp2917_DSM_2M.tiff", 2017, "test",
                lidar.LidarTile(
                    tile_id="sp2917",
                    year=2017,
                    resolution=lidar.Resolution.R_2M,
                    filename="/path/to/lidar/sp2917_DSM_2M.tiff",
                    product="test")),
            (
                "/path/to/lidar/sp2917_DSM_1M.tiff", 2017, "test",
                lidar.LidarTile(
                    tile_id="sp2917",
                    year=2017,
                    resolution=lidar.Resolution.R_1M,
                    filename="/path/to/lidar/sp2917_DSM_1M.tiff",
                    product="test")),
            (
                "/path/to/lidar/sp2917_DSM_50CM.tiff", 2017, "test",
                lidar.LidarTile(
                    tile_id="sp2917",
                    year=2017,
                    resolution=lidar.Resolution.R_50CM,
                    filename="/path/to/lidar/sp2917_DSM_50CM.tiff",
                    product="test")),
        ], lidar.LidarTile.from_filename)
