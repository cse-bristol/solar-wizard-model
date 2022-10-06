import pickle
from random import sample

import math
import os
from typing import List, Tuple, Optional

import numpy as np
import pytest
from matplotlib import pyplot as plt
from osgeo import gdal

from albion_models.solar_pv.pvgis.pvmaps import SLOPE, ASPECT_GRASS, HORIZON090_BASENAME, PI_HALF
from albion_models.test.solar_pv.pvgis.test_pvmaps import TestPVMaps

# Run pytest with this to see results while running and see logging outputs
# --capture=no --log-cli-level=INFO


class TestPVMapsRealDataMulti(TestPVMaps):
    PV_MODEL_COEFF_FILE_DIR = os.path.realpath("./test_data/inputs")

    ELEVATION_RASTER_FILENAME: str = "elevation_4326.tif"
    MASK_RASTER_FILENAME: str = "mask_4326.tif"

    @classmethod
    def init_dirs(cls, name: str):
        cls.DATA_INPUT_DIR = os.path.realpath(f"test_data/test_pvmaps_real_data_{name}/inputs")
        cls.INPUT_DIR = cls.DATA_INPUT_DIR
        cls.DATA_OUTPUT_DIR = os.path.realpath(f"test_data/test_pvmaps_real_data_{name}/outputs")
        if os.path.exists(os.path.join(cls.DATA_INPUT_DIR, "flat_roof_aspect_4326.gif")):
            cls.FLAT_ROOF_RASTER_FILENAME = "flat_roof_aspect_4326.gif"
        else:
            cls.FLAT_ROOF_RASTER_FILENAME = None

    @pytest.mark.parametrize(
        "name, exp_max_error", [
            ("thurso", 3.91),       # Missing pvmaps solar spectral and wind data
            ("alnwick", 1.81),      # Missing pvmaps solar spectral data
            ("alnwick_flat", 1.91),  # Flat roof (& missing pvmaps solar spectral data)
        ])
    def test_pv_output(self, name: str, exp_max_error: float):
        """Test the PV results for randomly sampled locations that are not masked against API results
        **Note** cached api data is stored in REAL_DATA_INPUT_DIR in api_real_pv_output.pkl - this will only be
        regenerated if the number of points changes, so deleting file is necessary if other changes are made. Also,
        the set of test points used are in loc_real_pv_sample_locns.pkl.
        """
        print(f"Running {name}")

        self.init_dirs(name)

        super()._run_pvmaps()

        # Sample unmasked coords... (or use cached)
        sample_size: int = 100
        sampled_locns = self._get_sample_locns_cached(sample_size)
        assert len(sampled_locns) == sample_size, f"Wrong number of test locs: {len(sampled_locns)}"

        test_locs: List[Tuple[float, float]] = [(lrx, lry) for _, _, lrx, lry in sampled_locns]

        # Do the tests
        self._test_pv_output(test_locs, "api_real_pv_output", max_diff_pc_year=exp_max_error)

    def _get_sample_locns_cached(self, sample_size: int) -> List[Tuple[int, int, float, float]]:
        cached_data_filename: str = "loc_real_pv_sample_locns"
        sampled_locns: List[Tuple[int, int, float, float]]
        if os.path.exists(f"{self.DATA_INPUT_DIR}/{cached_data_filename}.pkl"):
            sampled_locns = pickle.load(open(f"{self.DATA_INPUT_DIR}/{cached_data_filename}.pkl", "rb"))
        else:
            mask_ds: Optional[gdal.Dataset] = gdal.Open(f"{self.DATA_INPUT_DIR}/mask_4326.tif")
            gt = mask_ds.GetGeoTransform()
            mask_band = mask_ds.GetRasterBand(1)
            band_vals = mask_band.ReadAsArray(0, 0, mask_band.XSize, mask_band.YSize)
            mask_np = np.array(band_vals)
            unmasked_coords = np.transpose(np.nonzero(mask_np))

            print("Getting sample locns")
            test_locs: List[Tuple[int, int, float, float]] = []
            for y_coord, x_coord in unmasked_coords:
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

            print(f"\nAvailable size = {len(test_locs)}, sample_size = {sample_size}")
            sampled_locns = sample(test_locs, sample_size)

            pickle.dump(sampled_locns, open(f"{self.DATA_INPUT_DIR}/{cached_data_filename}.pkl", "wb"))
        return sampled_locns

