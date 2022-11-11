import pickle
from random import sample

import math
import os
from typing import List, Tuple, Optional

import numpy as np
import pytest
from osgeo import gdal

from albion_models.solar_pv.pvgis.pvmaps import SLOPE, ASPECT_GRASS
from albion_models.solar_pv.pvgis.test_pvmaps.test_pvmaps import TestPVMaps, TEST_DATA_DIR


# Run pytest with this to see results while running and see logging outputs
# --capture=no --log-cli-level=INFO


class TestPVMapsRealData(TestPVMaps):
    """
    Runs complete PVMaps process and then checks the data in the Grass DB
    """
    INPUT_DIR = os.path.realpath(f"{TEST_DATA_DIR}/test_pvmaps_real_data/inputs")
    DATA_INPUT_DIR = os.path.realpath(f"{TEST_DATA_DIR}/test_pvmaps_real_data/inputs")
    DATA_OUTPUT_DIR = os.path.realpath(f"{TEST_DATA_DIR}/test_pvmaps_real_data/outputs")

    EXPECTED_DIR = os.path.realpath(f"{TEST_DATA_DIR}/test_pvmaps_real_data/expected")

    ELEVATION_RASTER_FILENAME: str = "elevation_4326.tif"
    MASK_RASTER_FILENAME: str = "mask_4326.tif"
    FLAT_ROOF_RASTER_FILENAME: str = "flat_roof_nan_4326.tif"

    @classmethod
    def setup_class(cls):
        super()._run_pvmaps()

    @pytest.mark.parametrize(
        "exp, day",
        [
            # Below are the values from PVMAPS script - totpv_incl.sh
            (-0.36146, 17),
            (-0.22358, 46),
            (-0.03141, 75),
            (0.17052, 105),
            (0.32864, 135),
            (0.40265, 162),
            (0.36931, 198),
            (0.23823, 228),
            (0.04695, 259),
            (-0.15219, 289),
            (-0.32062, 319),
            (-0.40125, 345)
        ],
    )
    def test_get_solar_declination(self, day, exp):
        act = self.instance._calc_solar_declination(day)
        print(f"{act} {exp} {abs(act-exp)}")
        assert abs(act-exp) < 0.005

    def test_slope_raster(self):
        self.instance._run_cmd(f"r.import --overwrite input={self.EXPECTED_DIR}/slope_4326.tif output=exp_slope")
        # Note values in slope_4326.tif have been converted to uint16 to save space
        self.instance._run_cmd(f'r.mapcalc --overwrite '
                               f'expression="slope_diff=(abs(if(isnull({SLOPE}),0,{SLOPE})-if(isnull(exp_slope),0,exp_slope)))>1"')

        stats = self._get_raster_stats("slope_diff")
        print(stats)

        assert stats["0"] >= 749772
        assert stats["1"] <= 3540
        # assert stats["*"] <= 0

    def test_aspect_raster(self):
        self.instance._run_cmd(f"r.import --overwrite input={self.EXPECTED_DIR}/aspect_4326.tif output=exp_aspect")
        # Change from compass to grass angles
        self.instance._run_cmd('r.mapcalc --overwrite "exp_aspect_grass = if(exp_aspect == 0, 0, if(exp_aspect < 90, 90 - exp_aspect, 450 - exp_aspect))"')
        # Note values in aspect_4326.tif have been converted to uint16 to save space
        # Use 0 instead of no-data value, get smallest angle between two angles in whichever direction
        self.instance._run_cmd(f'r.mapcalc --overwrite '
                               f'expression="aspect_diff=(min('
                               f'abs(if(isnull({ASPECT_GRASS}),0,{ASPECT_GRASS})-if(isnull(exp_aspect_grass),0,exp_aspect_grass)),'
                               f'360-abs(if(isnull({ASPECT_GRASS}),0,{ASPECT_GRASS})-if(isnull(exp_aspect_grass),0,exp_aspect_grass))'
                               f'))>1"')

        stats = self._get_raster_stats("aspect_diff")
        print(stats)

        assert stats["0"] >= 745856
        assert stats["1"] <= 7456
        # assert stats["*"] <= 0

    def test_pv_output(self):
        """Test the PV results for randomly sampled locations that are not masked against API results
        **Note** cached api data is stored in REAL_DATA_INPUT_DIR in api_real_pv_output.pkl - this will only be
        regenerated if the number of points changes, so deleting file is necessary if other changes are made. Also,
        the set of test points used are in loc_real_pv_sample_locns.pkl.
        """

        # Checks results for the "loc_real_pv_sample_locns.pkl" in git
        # haven't changed
        max_diff_pc_year = 4.42  # for "loc_real_pv_sample_locns.pkl" in git (6/10/22)
        max_diff_pc_day = 100.0  # To use, turn on day values in _get_local_pv_data()

        # Get some random locations (or use cached ones)
        max_x = 32
        max_y = 32
        sample_size: int = max_x * max_y

        sampled_locns = self._get_sample_locns_cached(sample_size)
        assert len(sampled_locns) == sample_size, f"Wrong number of test locs: {len(sampled_locns)}"

        test_locs: List[Tuple[float, float]] = [(lrx, lry) for _, _, lrx, lry in sampled_locns]

        # Do the tests
        self._test_pv_output(test_locs, "api_real_pv_output", max_diff_pc_year)

    def _get_sample_locns_cached(self, sample_size: int) -> List[Tuple[int, int, float, float]]:
        cached_data_filename: str = "loc_real_pv_sample_locns"
        sampled_locns: List[Tuple[int, int, float, float]]
        if os.path.exists(f"{self.DATA_INPUT_DIR}/{cached_data_filename}.pkl"):
            with open(f"{self.DATA_INPUT_DIR}/{cached_data_filename}.pkl", "rb") as pkl_in:
                sampled_locns = pickle.load(pkl_in)
        else:
            mask_ds: Optional[gdal.Dataset] = gdal.Open(f"{self.DATA_INPUT_DIR}/mask_4326.tif")
            gt = mask_ds.GetGeoTransform()
            mask_band = mask_ds.GetRasterBand(1)
            band_vals = mask_band.ReadAsArray(0, 0, mask_band.XSize, mask_band.YSize)
            mask_np = np.array(band_vals)
            unmasked_coords = np.transpose(np.nonzero(mask_np))

            rng = np.random.default_rng()
            sampled_coords = rng.choice(size=sample_size*2, a=unmasked_coords, replace=False)

            print("Getting sample locns")
            test_locs: List[Tuple[int, int, float, float]] = []
            for y_coord, x_coord in sampled_coords:
                lrx, lry = gdal.ApplyGeoTransform(gt, (x_coord + 0.5), (y_coord + 0.5))
                # Try coords with mask in GRASS mapset ... to make sure will work
                val_mask_grass = self._get_raster_val("mask", lrx, lry)
                if val_mask_grass > 0:
                    # Check there are non-nan values to work with for the locn
                    userhorizon, angle, aspect, aspect_grass = self._get_api_inputs_from_rasters(lrx, lry)
                    if userhorizon.find("nan") == -1 and not math.isnan(angle) and not math.isnan(
                            aspect_grass):
                        test_locs.append((x_coord, y_coord, lrx, lry))
                num = len(test_locs)
                pc = int(100.0 * num/sample_size)
                print(f"\r{pc}%", end="")
                if num == sample_size:
                    break
            mask_ds = None

            print(f"Available size = {len(test_locs)}, sample_size = {sample_size}")
            sampled_locns = sample(test_locs, sample_size)

            pickle.dump(sampled_locns, open(f"{self.DATA_INPUT_DIR}/{cached_data_filename}.pkl", "wb"))
        return sampled_locns
