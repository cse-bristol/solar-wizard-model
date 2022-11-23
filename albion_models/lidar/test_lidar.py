from albion_models.lidar import lidar
from albion_models.test.test_funcs import ParameterisedTestCase


def _mocked_count_raster_pixels_pct(tiff: str, value, band: int = 1) -> float:
    if tiff == "/home/sp2917_DSM_50CM.tiff":
        return 0.4
    if tiff == "/home/sp2917_DSM_1M.tiff":
        return 0.2
    if tiff == "/home/sp2918_DSM_50CM.tiff":
        return 0.99
    if tiff == "/home/sp2918_DSM_1M.tiff":
        return 0.01
    else:
        raise ValueError(f"Unexpected tiff filename in mock of count_raster_pixels_pct: {tiff}")


class LidarTestCase(ParameterisedTestCase):

    def test_ZippedTiles_from_url(self):
        self.parameterised_test([
            (
                "https://example.com/LIDAR-DSM-2M-TL35ne.zip", 2017,
                lidar.ZippedTiles(
                    zip_id="TL35ne",
                    year=2017,
                    resolution=lidar.Resolution.R_2M,
                    url="https://example.com/LIDAR-DSM-2M-TL35ne.zip",
                    filename="2017-LIDAR-DSM-2M-TL35ne.zip")),
            (
                "https://example.com/LIDAR-DSM-1M-TL35ne.zip", 2017,
                lidar.ZippedTiles(
                    zip_id="TL35ne",
                    year=2017,
                    resolution=lidar.Resolution.R_1M,
                    url="https://example.com/LIDAR-DSM-1M-TL35ne.zip",
                    filename="2017-LIDAR-DSM-1M-TL35ne.zip")),
            (
                "https://example.com/LIDAR-DSM-50CM-TL35ne.zip", 2017,
                lidar.ZippedTiles(
                    zip_id="TL35ne",
                    year=2017,
                    resolution=lidar.Resolution.R_50CM,
                    url="https://example.com/LIDAR-DSM-50CM-TL35ne.zip",
                    filename="2017-LIDAR-DSM-50CM-TL35ne.zip")),
        ], lidar.ZippedTiles.from_url)

    def test_ZippedTiles_from_file(self):
        self.parameterised_test([
            (
                "2017-LIDAR-DSM-2M-TL35ne.zip",
                lidar.ZippedTiles(
                    zip_id="TL35ne",
                    year=2017,
                    resolution=lidar.Resolution.R_2M,
                    url=None,
                    filename="2017-LIDAR-DSM-2M-TL35ne.zip")),
            (
                "2017-LIDAR-DSM-1M-TL35ne.zip",
                lidar.ZippedTiles(
                    zip_id="TL35ne",
                    year=2017,
                    resolution=lidar.Resolution.R_1M,
                    url=None,
                    filename="2017-LIDAR-DSM-1M-TL35ne.zip")),
            (
                "2017-LIDAR-DSM-50CM-TL35ne.zip",
                lidar.ZippedTiles(
                    zip_id="TL35ne",
                    year=2017,
                    resolution=lidar.Resolution.R_50CM,
                    url=None,
                    filename="2017-LIDAR-DSM-50CM-TL35ne.zip")),
        ], lidar.ZippedTiles.from_filename)

    def test_LidarTile_from_file(self):
        self.parameterised_test([
            (
                "/home/neil/data/albion-models/lidar/sp2917_DSM_2M.tiff", 2017,
                lidar.LidarTile(
                    tile_id="sp2917",
                    year=2017,
                    resolution=lidar.Resolution.R_2M,
                    filename="/home/neil/data/albion-models/lidar/sp2917_DSM_2M.tiff")),
            (
                "/home/neil/data/albion-models/lidar/sp2917_DSM_1M.tiff", 2017,
                lidar.LidarTile(
                    tile_id="sp2917",
                    year=2017,
                    resolution=lidar.Resolution.R_1M,
                    filename="/home/neil/data/albion-models/lidar/sp2917_DSM_1M.tiff")),
            (
                "/home/neil/data/albion-models/lidar/sp2917_DSM_50CM.tiff", 2017,
                lidar.LidarTile(
                    tile_id="sp2917",
                    year=2017,
                    resolution=lidar.Resolution.R_50CM,
                    filename="/home/neil/data/albion-models/lidar/sp2917_DSM_50CM.tiff")),
        ], lidar.LidarTile.from_filename)
