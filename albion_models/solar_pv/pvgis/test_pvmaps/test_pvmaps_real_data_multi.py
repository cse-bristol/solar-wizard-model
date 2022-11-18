import pickle
from math import isnan
from random import sample

import math
import os
from typing import List, Tuple, Optional

import numpy as np
import pytest
from osgeo import gdal

from albion_models.solar_pv.pvgis.pvmaps import SLOPE, \
    FLAT_ROOF_ASPECT_COMPASS, ASPECT_GRASS_ADJUSTED
from albion_models.solar_pv.pvgis.test_pvmaps.test_pvmaps import TestPVMaps, TEST_DATA_DIR


# Run pytest with this to see results while running and see logging outputs
# --capture=no --log-cli-level=INFO


class TestPVMapsRealDataMulti(TestPVMaps):
    """
    Runs complete PVMaps process and then checks the data in the Grass DB - for multiple locations
    """
    ELEVATION_RASTER_FILENAME: str = "elevation_4326.tif"
    MASK_RASTER_FILENAME: str = "mask_4326.tif"

    # flat_roof_degrees=10.0 = default from web interface; flat_roof_degrees_threshold=5.0 = value from pvgis.py
    FLAT_ROOF_DEGREES: float = 10.0
    FLAT_ROOF_DEGREES_THRESHOLD: float = 5.0

    @classmethod
    def init_dirs(cls, name: str):
        cls.DATA_INPUT_DIR = os.path.realpath(f"{TEST_DATA_DIR}/test_pvmaps_real_data_{name}/inputs")
        cls.INPUT_DIR = cls.DATA_INPUT_DIR
        cls.DATA_OUTPUT_DIR = os.path.realpath(f"{TEST_DATA_DIR}/test_pvmaps_real_data_{name}/outputs")
        if os.path.exists(os.path.join(cls.DATA_INPUT_DIR, "flat_roof_aspect_4326.tif")):
            cls.FLAT_ROOF_RASTER_FILENAME = "flat_roof_aspect_4326.tif"
        else:
            cls.FLAT_ROOF_RASTER_FILENAME = None

    @classmethod
    def is_flat_roof_aspects(cls):
        return cls.FLAT_ROOF_RASTER_FILENAME is not None

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

        super()._run_pvmaps(flat_roof_degrees=self.FLAT_ROOF_DEGREES, flat_roof_degrees_threshold=self.FLAT_ROOF_DEGREES_THRESHOLD)

        # Sample unmasked coords... (or use cached)
        sample_size: int = 20
        sampled_locns = self._get_sample_locns_cached(sample_size)
        assert len(sampled_locns) == sample_size, f"Wrong number of test locs: {len(sampled_locns)}"

        test_locs: List[Tuple[float, float]] = [(lrx, lry) for _, _, lrx, lry in sampled_locns]

        # Do the tests
        self._test_pv_output(test_locs, "api_real_pv_output", max_diff_pc_year=exp_max_error)

        if self.is_flat_roof_aspects():
            self._test_aspects_used(test_locs)

    def _get_sample_locns_cached(self, sample_size: int) -> List[Tuple[int, int, float, float]]:
        cached_data_filename: str = "loc_real_pv_sample_locns"
        sampled_locns: List[Tuple[int, int, float, float]]
        if os.path.exists(f"{self.DATA_INPUT_DIR}/{cached_data_filename}.pkl"):
            with open(f"{self.DATA_INPUT_DIR}/{cached_data_filename}.pkl", "rb") as pkl_in:
                sampled_locns = pickle.load(pkl_in)
        else:
            # Get available test points from either flat roof mask (if there is one) or full mask - flat roof area is
            # always inside the full mask area
            if self.is_flat_roof_aspects():
                raster_ds: Optional[gdal.Dataset] = gdal.Open(f"{self.DATA_INPUT_DIR}/{self.FLAT_ROOF_RASTER_FILENAME}")
                gt = raster_ds.GetGeoTransform()
                raster_band = raster_ds.GetRasterBand(1)
                band_vals = raster_band.ReadAsArray(0, 0, raster_band.XSize, raster_band.YSize)
                raster_np = np.array(band_vals)
                unmasked_coords = np.transpose(np.nonzero(np.isfinite(raster_np)))
            else:
                raster_ds: Optional[gdal.Dataset] = gdal.Open(f"{self.DATA_INPUT_DIR}/mask_4326.tif")
                gt = raster_ds.GetGeoTransform()
                raster_band = raster_ds.GetRasterBand(1)
                band_vals = raster_band.ReadAsArray(0, 0, raster_band.XSize, raster_band.YSize)
                raster_np = np.array(band_vals)
                unmasked_coords = np.transpose(np.nonzero(raster_np))

            print("Getting sample locns")
            test_locs: List[Tuple[int, int, float, float]] = []
            for y_coord, x_coord in unmasked_coords:
                lrx, lry = gdal.ApplyGeoTransform(gt, (x_coord + 0.5), (y_coord + 0.5))
                # Try coords with mask in GRASS mapset ... to make sure will work
                val_mask_grass = self._get_raster_val("mask", lrx, lry)
                if val_mask_grass > 0:
                    # Check there are non-nan values to work with for the locn
                    userhorizon, angle, aspect, aspect_grass = self._get_api_inputs_from_rasters(lrx, lry)

                    flat_roof_ok: bool = True
                    if self.is_flat_roof_aspects():
                        # Select test points that also allow checking flat roof aspects have been transferred
                        flat_roof_ok = \
                            not isnan(self._get_raster_val(FLAT_ROOF_ASPECT_COMPASS, lrx, lry)) and \
                            self._get_raster_val(SLOPE, lrx, lry) < self.FLAT_ROOF_DEGREES_THRESHOLD

                    if userhorizon.find("nan") == -1 and \
                            not math.isnan(angle) and \
                            not math.isnan(aspect_grass) and \
                            flat_roof_ok:
                        test_locs.append((x_coord, y_coord, lrx, lry))
                num = len(test_locs)
                pc = int(100.0 * num/sample_size)
                print(f"\r{pc}%", end="")
                if num == sample_size:
                    break
            raster_ds = None

            print(f"\nAvailable size = {len(test_locs)}, sample_size = {sample_size}")
            sampled_locns = sample(test_locs, sample_size)

            pickle.dump(sampled_locns, open(f"{self.DATA_INPUT_DIR}/{cached_data_filename}.pkl", "wb"))
        return sampled_locns

    def _test_aspects_used(self, test_locns: List[Tuple[float, float]]):
        print("_test_aspects_used")
        aspects_grass = self._read_aspect_data(ASPECT_GRASS_ADJUSTED, test_locns)  # Adjustments include applying the flat roof aspects
        flat_roof_aspects_compass = self._read_aspect_data(FLAT_ROOF_ASPECT_COMPASS, test_locns)

        print("Checking results...")
        for aspect_grass, flat_roof_aspect_compass in zip(aspects_grass, flat_roof_aspects_compass):
            if isnan(flat_roof_aspect_compass):
                assert not isnan(aspect_grass)
                assert 0 <= aspect_grass <= 360
            else:
                assert 0 <= flat_roof_aspect_compass <= 360
                assert 0 <= aspect_grass <= 360
                flat_roof_aspect_grass: float = self._switch_grass_compass(flat_roof_aspect_compass)
                assert abs(flat_roof_aspect_grass - aspect_grass) < 0.001

    def _read_aspect_data(self, raster_name: str, test_locns: List[Tuple[float, float]]):
        print("_read_aspect_data")
        loc_results = []
        for lon_east, lat_north in test_locns:
            value = self._get_raster_val(raster_name, lon_east, lat_north)
            loc_results.append(value)
            print(f"\r{100*len(loc_results)/len(test_locns):.0f}%", end="")
        print()
        return loc_results
