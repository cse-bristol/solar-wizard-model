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


class TestPVMapsRealData(TestPVMaps):
    INPUT_DIR = os.path.realpath("./test_data/inputs")
    EXPECTED_DIR = os.path.realpath("./test_data/expected")
    REAL_DATA_INPUT_DIR = os.path.realpath("./test_data/inputs")
    REAL_DATA_OUTPUT_DIR = os.path.realpath("./test_data/outputs")

    ELEVATION_RASTER_FILENAME: str = "elevation_4326.tif"
    MASK_RASTER_FILENAME: str = "mask_4326.tif"

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
        self.instance._run_cmd(f'r.mapcalc --overwrite expression="slope_diff=(abs(({SLOPE}/exp_slope))-1)>0.01"')

        stats = self._get_raster_stats("slope_diff")
        print(stats)

        assert stats["0"] >= 2149179
        assert stats["1"] <= 3606
        assert stats["*"] <= 2799

    def test_aspect_raster(self):
        self.instance._run_cmd(f"r.import --overwrite input={self.EXPECTED_DIR}/aspect_4326.tif output=exp_aspect")
        # Change from compass to grass angles
        self.instance._run_cmd('r.mapcalc --overwrite "exp_aspect_grass = if(exp_aspect == 0, 0, if(exp_aspect < 90, 90 - exp_aspect, 450 - exp_aspect))"')
        self.instance._run_cmd(f'r.mapcalc --overwrite expression="aspect_diff=abs(({ASPECT_GRASS}/exp_aspect_grass)-1)>0.01"')

        stats = self._get_raster_stats("aspect_diff")
        print(stats)

        assert stats["0"] >= 2138288
        assert stats["1"] <= 9442
        assert stats["*"] <= 7854

    def test_pv_output(self):
        """Test the PV results for randomly sampled locations that are not masked against API results
        **Note** cached api data is stored in REAL_DATA_INPUT_DIR in api_real_pv_output.pkl - this will only be
        regenerated if the number of points changes, so deleting file is necessary if other changes are made. Also,
        the set of test points used are in loc_real_pv_sample_locns.pkl.
        """

        # Use to not assert and plot results
        # max_diff_pc_year = None
        # max_diff_pc_day = None
        # Use to assert and not plot results - checks results for the "loc_real_pv_sample_locns.pkl" in git
        # haven't changed
        max_diff_pc_year = 3.73  # for "loc_real_pv_sample_locns.pkl" in git, max: 3.7239958592930784
        max_diff_pc_day = 100.0  # To use, turn on day values in _get_local_pv_data()

        # Get some random locations
        max_x = 32
        max_y = 32
        sample_size: int = max_x * max_y

        sampled_locns = self._get_sample_locns_cached(sample_size)
        assert len(sampled_locns) == sample_size, f"Wrong number of test locs: {len(sampled_locns)}"

        test_locs: List[Tuple[float, float]] = [(lrx, lry) for _, _, lrx, lry in sampled_locns]

        # Do the tests
        api_results, loc_results, diff_results = \
            self._test_pv_output(test_locs, "api_real_pv_output", max_diff_pc_year, max_diff_pc_day)

        if max_diff_pc_year is None:
            # This will print out edge cases that can then be used with test_get_elevations_for()
            self._get_stats_and_find_edge_cases(diff_results, sampled_locns)

            # Plot
            self._plot_results(api_results, loc_results, diff_results, max_x, max_y)

            # Save raster for sharing etc
            self._save_as_raster(diff_results, sampled_locns)

    def _save_as_raster(self, diff_results, sampled_locns):
        ds: gdal.Dataset = gdal.Open(f"{self.REAL_DATA_INPUT_DIR}/mask_4326.tif")
        gt = ds.GetGeoTransform()
        pr = ds.GetProjection()
        band = ds.GetRasterBand(1)

        values = np.full((band.YSize, band.XSize), np.nan)
        for (x_coord, y_coord, _, _), (_, diff_year) in zip(sampled_locns, diff_results):
            values[y_coord, x_coord] = abs(diff_year)

        driver = gdal.GetDriverByName("GTiff")
        driver.Register()
        ds: gdal.Dataset = driver.Create(f"{self.REAL_DATA_OUTPUT_DIR}/diffs.tif", xsize=band.XSize, ysize=band.YSize,
                                         bands=1, eType=gdal.GDT_Float32)
        ds.SetGeoTransform(gt)
        ds.SetProjection(pr)
        ds_band: gdal.Band = ds.GetRasterBand(1)
        ds_band.WriteArray(values)
        ds_band.SetNoDataValue(np.nan)
        ds_band.FlushCache()

    @staticmethod
    def _plot_results(api_results, loc_results, diff_results, max_x, max_y):
        api_result = np.zeros((max_y, max_x))
        loc_result = np.zeros((max_y, max_x))
        diff_result = np.zeros((max_y, max_x))
        x = 0
        y = 0
        for (local_e_day, local_e_year), (api_e_day, api_e_year), (diff_day, diff_year) in zip(loc_results, api_results,
                                                                                               diff_results):
            api_result[y, x] = api_e_year
            loc_result[y, x] = local_e_year
            diff_result[y, x] = abs(diff_year)
            x += 1
            if x == max_x:
                x = 0
                y += 1

        mean_diff = np.mean(diff_result)
        std_diff = np.std(diff_result)
        max_diff = np.amax(diff_result)
        min_diff = np.amin(diff_result)
        print(f"Abs diff % stats: Max: {max_diff} / Mean: {mean_diff} / Min: {min_diff} / SD: {std_diff}")

        fig, axs = plt.subplots(ncols=3)
        axs[0].set_title('API')
        axs[0].imshow(api_result)
        axs[1].set_title('LOC')
        axs[1].imshow(loc_result)
        axs[2].set_title('ABS DIFF (%)')
        axs[2].imshow(diff_result)
        plt.show()

    def _get_sample_locns_cached(self, sample_size: int) -> List[Tuple[int, int, float, float]]:
        cached_data_filename: str = "loc_real_pv_sample_locns"
        sampled_locns: List[Tuple[int, int, float, float]]
        if os.path.exists(f"{self.REAL_DATA_INPUT_DIR}/{cached_data_filename}.pkl"):
            sampled_locns = pickle.load(open(f"{self.REAL_DATA_INPUT_DIR}/{cached_data_filename}.pkl", "rb"))
        else:
            mask_ds: Optional[gdal.Dataset] = gdal.Open(f"{self.REAL_DATA_INPUT_DIR}/mask_4326.tif")
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

            pickle.dump(sampled_locns, open(f"{self.REAL_DATA_INPUT_DIR}/{cached_data_filename}.pkl", "wb"))
        return sampled_locns

    @staticmethod
    def _get_stats_and_find_edge_cases(diff_results, sampled_locns):
        diff_result = np.array([abs(r_year) for _, r_year in diff_results])

        mean_diff = np.mean(diff_result)
        std_diff = np.std(diff_result)
        max_diff = np.amax(diff_result)
        min_diff = np.amin(diff_result)
        print(f"Abs diff % stats: Max: {max_diff} / Mean: {mean_diff} / Min: {min_diff} / SD: {std_diff}")

        std_mult = 3.0
        edge_case_above = mean_diff + std_mult * std_diff

        print(f"Cases greater the {std_mult} * SD:")
        for (_, r_year), (x_coord, y_coord, lrx, lry) in zip(diff_results, sampled_locns):
            if abs(r_year) > edge_case_above:
                print(f"({lrx}, {lry}),")

    @pytest.mark.skip(reason="Not a test, use to see horizons at some test locations")
    def test_get_elevations_for(self):
        """Not a test, use to see horizons at some test locations"""
        # > 3 * SD:
        test_locns = (
            (-1.980795210965064, 51.70583290172531),
            (-1.9660424731956845, 51.702064291177415),
            (-1.9768283315256037, 51.70478056894702),
            (-1.97758116995937, 51.70879202552067),
            (-1.9769441528231062, 51.70389013351924),
            (-1.9771323624315478, 51.70897191146568),
            (-1.9704292048385912, 51.70343142435947),
            (-1.98108476420882, 51.70770371555338),
            (-1.9838644753488799, 51.70916978600519),
            (-1.9767993762012281, 51.70214523985267),
            (-1.9810413312222568, 51.70632758807408),
        )
        for ix, (lon_east, lat_north) in enumerate(test_locns):
            userhorizon: str = ""
            for grass_angle in range(0, 360, 45):
                horizon_raster = f"{HORIZON090_BASENAME}_{grass_angle:03d}"
                horizon: float = self._get_raster_val(horizon_raster, lon_east, lat_north)
                if horizon < 0.0:
                    raise Exception(f"Unexpected horizon value {horizon}")
                if horizon > PI_HALF:
                    raise Exception(f"Unexpected horizon value {horizon}")
                if userhorizon:
                    userhorizon += ", "
                userhorizon += f"{horizon}"
            angle, aspect, aspect_grass = self._get_aspect_slope_from_rasters(lon_east, lat_north)

            print(f'"t-{ix}": [{userhorizon}],  # s={angle}, a={aspect_grass}')
