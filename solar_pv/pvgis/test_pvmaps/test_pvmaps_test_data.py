# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
"""
Create some simulated test data for a range of aspects, slopes and horizons then compare local pv o/p values with the
values from the pvgis api for the same aspects, slopes and horizons.
"""

import math
import os
import shutil
from typing import List, Optional, Tuple

from osgeo import gdal
import numpy as np

from pvgis.test_pvmaps.test_pvmaps import TestPVMaps, TEST_DATA_DIR


# Run pytest with this to see results while running and see logging outputs
# --capture=no --log-cli-level=INFO


class TestPVMapsTestData(TestPVMaps):
    """
    Runs complete PVMaps process and then checks the data in the Grass DB - for generated range of aspects & slopes
    """
    DATA_INPUT_DIR = os.path.realpath(f"{TEST_DATA_DIR}/test_pvmaps_test_data/inputs")
    INPUT_DIR: str = os.path.realpath(
        f"{TEST_DATA_DIR}/test_pvmaps_test_data/generated_test_inputs")

    ELEVATION_RASTER_FILENAME: str = "elevation.tif"
    MASK_RASTER_FILENAME: str = "mask.tif"
    FLAT_ROOF_RASTER_FILENAME: str = "flat_roof_aspect_nan.tif"
    FORCED_SLOPE_FILENAME: Optional[str] = "slope.tif"
    FORCED_ASPECT_FILENAME: Optional[str] = "aspect.tif"
    FORCED_HORIZON_BASEFILENAME: Optional[str] = "horizon"

    ###
    # !!! MIN RASTER SIZE IS 10 by 10 - r.pv will coredump if less !!!
    XSIZE = 10
    Y_BLOCK_SIZE = 10
    ###

    USE_X_IX_MIN = 0  # inc. (Aspect) 0 = E, 1 = NE, 2 = N, 3 = NW, 4 = W, 5 = SW, 6 = S, 7 = SE
    USE_X_IX_MAX = 8  # exc. (Aspect)
    USE_Y_IX_MIN = 0  # inc. (Slope) 0 = 0, 1 = 30, 2 = 60, 3 = 90
    USE_Y_IX_MAX = 4  # exc. (Slope)

    D90 = math.pi/2.0
    D45 = math.pi/4.0
    D20 = math.pi/9.0
    D0 = 0.0
    # 0 = E, 2 = N, 4 = W, 6 = S
    TEST_HORIZONS = {
        "h_none": [D0] * 8,
        "h_south": [D90] * 5 + [0.0] * 3,
        "h_blocked-low": [D20] * 8,
        "h_obstacles": [D0, D20, D0, D20, D0, D20, D0, D20],
        "h_blocked-mid": [D45] * 8,
        "h_blocked": [D90] * 8,

        # Test horizons which showed differing results in the real data but without -ve and > 90 degrees horizons
        "t-0": [0.02333823, 0.05599336, 0.5258255, 0.696244, 0.7175032, 0.2262796, 0.0, 0.0],       # s=52.034351348877, a=322.277
        "t-1": [0.0, 0.09205787, 0.08445972, 0.2245082, 0.3286499, 0.2026049, 0.0, 0.0],            # s=72.9425430297852, a=5.67225
        "t-2": [0.01182702, 0.07072365, 0.03580371, 0.0196779, 0.0, 0.0, 0.0, 0.02229225],          # s=68.8477096557617, a=219.8705
        "t-3": [0.001764834, 0.07981813, 0.1293355, 0.4716583, 0.6639411, 0.3776636, 0.0, 0.0],     # s=38.4195709228516, a=348.0992
        "t-4": [0.4486941, 0.6878363, 0.4041844, 0.1566703, 0.0, 0.0, 0.002023996, 0.03701947],     # s=70.8203353881836, a=204.4764
        "t-5": [0.01513914, 0.0, 0.2023054, 0.5972219, 0.7398635, 0.4489109, 0.0, 0.0],             # s=43.9065246582031, a=347.1845
        "t-6": [0.4963857, 0.05127088, 0.006364813, 0.01416116, 0.01759821, 0.0, 0.0, 0.008173156], # s=66.0955276489258, a=222.897
        "t-7": [0.501586, 0.708634, 0.5882581, 0.0988518, 0.0, 0.0, 0.06992211, 0.1128646],         # s=65.1903076171875, a=216.1884
        "t-8": [1.253926, 1.252908, 1.222203, 0.0, 0.0, 0.0, 0.0, 0.4091839],                       # s=73.698844909668, a=230.7544
        "t-9": [0.0166989, 0.01109422, 0.01626312, 0.05425261, 0.06260412, 0.0, 0.0, 0.0],          # s=56.4355010986328, a=323.619
        "t-10": [0.0, 0.009862965, 0.02817614, 0.01581866, 0.0, 0.0, 0.0, 0.0],                     # s=1.7918553352356, a=298.6087
    }

    @classmethod
    def setup_class(cls):
        cls.test_locns = cls.create_test_rasters()

    def test_outputs(self):
        """
        Test either or both of pv and radiation outputs from local against API for generated "test" data.
        **Note** cached api data is stored in REAL_DATA_INPUT_DIR in api_test_pv_output.pkl - this will only be
        regenerated if the number of points changes, so deleting file is necessary if other changes are made.
        """
        self._run_pvmaps(self.FORCED_HORIZON_BASEFILENAME + ".tif")

        self._test_pv_output(self.test_locns, "api_test_pv_output", max_diff_pc_year=5.83)  # Use to assert error if gets worse and not plot results

    @classmethod
    def borrow_transform_projection(cls):
        ds: gdal.Dataset = gdal.Open(f"{cls.DATA_INPUT_DIR}/mask_27700.tif")
        gt = ds.GetGeoTransform()
        pr = ds.GetProjection()
        return gt, pr

    @staticmethod
    def create_raster(gt, pr, name: str, values: np.array, e_type) -> gdal.Dataset:
        """
        :return: list of (long, lat) tuples of the points in the raster
        """
        driver = gdal.GetDriverByName("GTiff")
        driver.Register()

        ds: gdal.Dataset = driver.Create(name, xsize=values.shape[1], ysize=values.shape[0], bands=1, eType=e_type)
        ds.SetGeoTransform(gt)
        ds.SetProjection(pr)
        ds_band: gdal.Band = ds.GetRasterBand(1)
        ds_band.WriteArray(values)
        ds_band.SetNoDataValue(np.nan)
        ds_band.FlushCache()

        return ds

    @classmethod
    def create_test_rasters(cls) -> List[Tuple[float, float]]:
        """
        :return: list of (long, lat) tuples of the points in the rasters
        """
        shutil.rmtree(cls.INPUT_DIR, ignore_errors=True)
        os.mkdir(cls.INPUT_DIR)

        gt, pr = cls.borrow_transform_projection()

        num_horizons: int = len(cls.TEST_HORIZONS)
        total_ysize: int = cls.Y_BLOCK_SIZE * num_horizons

        # Elevation
        # 158.0 is the elevation the api returns for the location being used
        elevation = np.full((total_ysize, cls.XSIZE), 158.0)
        ds = cls.create_raster(gt, pr, f"{cls.INPUT_DIR}/{cls.ELEVATION_RASTER_FILENAME}", elevation, gdal.GDT_Float32)

        ###
        test_locs: List[Tuple[float, float]] = []
        gt = ds.GetGeoTransform()
        for hor_ix in range(num_horizons):
            for y_coord_block in range(cls.USE_Y_IX_MIN, cls.USE_Y_IX_MAX):
                y_coord: int = hor_ix * cls.Y_BLOCK_SIZE + y_coord_block
                for x_coord in range(cls.USE_X_IX_MIN, cls.USE_X_IX_MAX):
                    y = gt[3] + (x_coord + 0.5) * gt[4] + (y_coord + 0.5) * gt[5]
                    x = gt[0] + (x_coord + 0.5) * gt[1] + (y_coord + 0.5) * gt[2]
                    test_locs.append((x, y))
        ###

        # Mask
        # Un-masked area needs to be > 10 by 10 too!
        mask = np.full((total_ysize, cls.XSIZE), 1)
        cls.create_raster(gt, pr, f"{cls.INPUT_DIR}/{cls.MASK_RASTER_FILENAME}", mask, gdal.GDT_Int32)

        # Flat roof aspect values
        mask = np.full((total_ysize, cls.XSIZE), math.nan)
        cls.create_raster(gt, pr, f"{cls.INPUT_DIR}/{cls.FLAT_ROOF_RASTER_FILENAME}", mask, gdal.GDT_Int32)

        # Slope
        vals = np.zeros((total_ysize, cls.XSIZE))
        slopes = []
        for hor_ix in range(num_horizons):
            for row in range(cls.Y_BLOCK_SIZE):
                if cls.USE_Y_IX_MIN <= row < cls.USE_Y_IX_MAX:
                    slopes.append(30.0 * row)
                else:
                    slopes.append(0.0)
        for col in range(cls.XSIZE):
            vals[:, col] = slopes
        cls.create_raster(gt, pr, f"{cls.INPUT_DIR}/slope.tif", vals, gdal.GDT_Float32)

        # Aspect
        # Note: https://grass.osgeo.org/grass80/manuals/r.slope.aspect.html :
        # "Zero aspect indicates flat areas with zero slope"
        vals = np.zeros((total_ysize, cls.XSIZE))
        aspects = []
        for col in range(0, cls.XSIZE):
            if cls.USE_X_IX_MIN <= col < cls.USE_X_IX_MAX:
                aspect = (45.0 * col) % 360.0
                if aspect <= 0.0:   # "Zero aspect indicates flat areas with zero slope"
                    aspect = 360.0
                aspects.append(aspect)
            else:
                aspects.append(0.0)
        for row in range(total_ysize):
            vals[row, :] = aspects
        cls.create_raster(gt, pr, f"{cls.INPUT_DIR}/aspect.tif", vals, gdal.GDT_Float32)

        # Horizon
        for ix, d in enumerate(range(0, 360, 45)):
            vals = np.zeros((total_ysize, cls.XSIZE))
            for hor_ix, (name, horizon) in enumerate(cls.TEST_HORIZONS.items()):
                y_start: int = hor_ix * cls.Y_BLOCK_SIZE
                y_end: int = y_start + cls.Y_BLOCK_SIZE
                vals[y_start:y_end, 0:cls.XSIZE] = horizon[ix]
            cls.create_raster(gt, pr,
                              f"{cls.INPUT_DIR}/{cls.FORCED_HORIZON_BASEFILENAME}_{d:03d}.tif",
                              vals, gdal.GDT_Float32)
        return test_locs
