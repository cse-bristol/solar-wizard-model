import pickle

import requests as requests
import logging
import os
import shlex
import shutil
import subprocess
from typing import List, Dict, Optional, Tuple

import math

from albion_models.solar_pv.pvgis.pvmaps import PVMaps, CSI, OUT_DIRECT_RAD_BASENAME, OUT_DIFFUSE_RAD_BASENAME, \
    HORIZON090_BASENAME, SLOPE_ADJUSTED, ASPECT_GRASS_ADJUSTED, OUT_PV_POWER_WIND_SPECTRAL_BASENAME


class TestPVMaps:
    instance: PVMaps
    INPUT_DIR: str
    REAL_DATA_INPUT_DIR: str

    ELEVATION_RASTER_FILENAME: str
    MASK_RASTER_FILENAME: str

    FORCED_SLOPE_FILENAME: Optional[str] = None
    FORCED_ASPECT_FILENAME: Optional[str] = None

    Y_BLOCK_SIZE: int = 1

    @classmethod
    def _run_pvmaps(cls, forced_horizon_basename: Optional[str] = None):
        logging.basicConfig(format='%(asctime)s: %(levelname)s: %(message)s',
                            level=logging.DEBUG, datefmt="%d/%m/%Y %H:%M:%S")

        print("Set up (first time this takes a couple of minutes)")
        cls.instance: PVMaps = PVMaps(
            grass_dbase_dir=os.path.realpath("./test_data/grass_dbase"),
            input_dir=cls.INPUT_DIR,
            output_dir=os.path.realpath("./test_data/outputs"),
            pvgis_data_tar_file=os.path.realpath("./test_data/pvgis_data_tar/pvgis_data.tar"),
            pv_model_coeff_file_dir=cls.INPUT_DIR,
            keep_temp_mapset=True,
            num_processes=os.cpu_count(),
            output_direct_diffuse=False,
            horizon_step_degrees=45,
            horizon_search_distance=1000,
            flat_roof_degrees=0.0,
            flat_roof_degrees_threshold=0.0,
            panel_type=CSI,
            num_pv_calcs_per_year=None
        )

        # Create test mapset or use existing during test dev etc - comment out clean up in teardown_class() for this

        cls._clean_test_mapsets()  # Comment this out to use the same mapset during test dev

        test_mapset = None
        num_test_mapsets = 0
        mapsets = os.listdir("./test_data/grass_dbase/grassdata")
        for mapset in mapsets:
            if mapset.startswith("pvmaps."):
                test_mapset = mapset
                num_test_mapsets += 1
        if num_test_mapsets == 1:
            cls.instance._update_mapset(test_mapset)
        else:
            if num_test_mapsets > 1:
                cls._clean_test_mapsets()
            print("Running create_pvmap for tests")
            cls.instance.create_pvmap(cls.ELEVATION_RASTER_FILENAME, cls.MASK_RASTER_FILENAME,
                                      cls.FORCED_SLOPE_FILENAME, cls.FORCED_ASPECT_FILENAME, forced_horizon_basename)

        # Disable mask if it's enabled
        try:
            cls.instance._run_cmd(f"g.rename raster=MASK,MASK_")
        except:
            pass

    def teardown_class(self):
        pass

    @staticmethod
    def _clean_test_mapsets():
        mapsets = os.listdir("./test_data/grass_dbase/grassdata")
        for mapset in mapsets:
            if mapset.startswith("pvmaps."):
                shutil.rmtree(f"./test_data/grass_dbase/grassdata/{mapset}")

    def _get_raster_val(self, raster: str, lon_east: float, lat_north: float):
        val: float = 0.0
        stats_cmd = f"r.what map={raster} coordinates={lon_east},{lat_north}"
        args: List[str] = shlex.split(stats_cmd)
        process = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, env=self.instance._grass_env)
        with process.stdout:
            line = process.stdout.readline()
            vals = line.decode("utf-8").split("|")
            try:
                val_str = vals[-1].strip()
                if val_str == "*":
                    val = math.nan
                else:
                    val = float(vals[-1])
            except ValueError as e:
                print(f"Error - {line}")
                raise e
        process.wait()
        return val

    def _get_raster_stats(self, name) -> Dict[str, int]:
        stats: Dict[str, int] = {}
        stats_cmd = f"r.stats input={name} -c"
        args: List[str] = shlex.split(stats_cmd)
        process = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, env=self.instance._grass_env)
        with process.stdout:
            while True:
                line = process.stdout.readline()
                if not line:
                    break
                vals = line.decode("utf-8").split()
                if len(vals) >= 2:
                    stats[vals[0]] = int(vals[1])
        process.wait()
        return stats

    @staticmethod
    def _switch_grass_compass(angle: float) -> float:
        """This works both ways :)"""
        return (450-angle) % 360

    def _get_api_inputs_from_rasters(self, lon_east: float, lat_north: float):
        ###
        # Get api parameters
        # userhorizon
        userhorizon: str = ""
        horizon_heights_in_radians = True
        for compass_angle in range(0, 360, 45):
            grass_angle = self._switch_grass_compass(compass_angle)
            horizon_raster = f"{HORIZON090_BASENAME}_{grass_angle:03d}"
            horizon: float = self._get_raster_val(horizon_raster, lon_east, lat_north)
            if horizon_heights_in_radians:
                horizon = 180 * horizon / math.pi  # Convert to degrees as api expects degrees
            if userhorizon:
                userhorizon += ","
            if horizon < 0.0:
                raise Exception(f"Unexpected horizon value {horizon}")
            if horizon > 90.0:
                raise Exception(f"Unexpected horizon value {horizon}")
            userhorizon += f"{horizon:.3f}"

        angle, aspect, aspect_grass = self._get_aspect_slope_from_rasters(lon_east, lat_north)

        return userhorizon, angle, aspect, aspect_grass

    def _get_aspect_slope_from_rasters(self, lon_east: float, lat_north: float):
        # angle (0 to 90 degrees)
        angle = self._get_raster_val(SLOPE_ADJUSTED, lon_east, lat_north)
        if angle < 0.0:
            angle = 0.0
        if angle > 90.0:
            angle = 90.0

        # aspect (-180 to 180 degress)
        aspect_grass = self._get_raster_val(ASPECT_GRASS_ADJUSTED, lon_east, lat_north)
        aspect = self._switch_grass_compass(aspect_grass)
        aspect = aspect - 180  # Change to "Orientation (azimuth) angle of the (fixed) PV system, 0=south, 90=west, -90=east."
        if aspect > 180:
            aspect -= 360
        ###

        return angle, aspect, aspect_grass

    @staticmethod
    def _pc_diff(loc: float, api: float) -> float:
        if api == 0.0 and loc == 0.0:
            return 0.0
        if api == 0.0:
            return math.nan
        return 100.0 * (loc - api) / api

    #############
    def _test_pv_output(self, test_locns: List[Tuple[float, float]], cached_data_filename: str,
                        max_diff_pc_year: Optional[float] = None, max_diff_pc_day: Optional[float] = None):
        api_results = self._read_api_pv_data_with_cache(test_locns, cached_data_filename)
        loc_results = self._read_local_pv_data(test_locns)

        do_assert: bool = max_diff_pc_year is not None and max_diff_pc_day is not None

        diff_results = []
        for ix, ((local_days, loc_year), (api_days, api_year), (lon_east, lat_north)) in \
                enumerate(zip(loc_results, api_results, test_locns)):
            diff_year = self._pc_diff(loc_year, api_year)
            diff_days = []
            for local, api in zip(local_days, api_days):
                diff_day = self._pc_diff(local/1000, api)
                diff_days.append(diff_day)
            diff_results.append((diff_days, diff_year))

            if not do_assert:
                angle, aspect, _ = self._get_aspect_slope_from_rasters(lon_east, lat_north)
                hor_ix = math.floor(ix / self.Y_BLOCK_SIZE)
                print(f"Cell values:"
                      f"\nhor_ix: {hor_ix}"
                      f"\nangle: {angle}"
                      f"\naspect: {aspect}"
                      f"\napi:\t{api_year}\t{api_days}"
                      f"\nlocal:\t {loc_year}\t{[round(l/1000, 2) for l in local_days]}"
                      f"\ndiff:\t{diff_year}\t{diff_days}")

        if do_assert:
            for ix, ((diff_days, diff_year), (loc_days, loc_year), (api_days, api_year), (lon_east, lat_north)) \
                    in enumerate(zip(diff_results, loc_results, api_results, test_locns)):
                angle, aspect, _ = self._get_aspect_slope_from_rasters(lon_east, lat_north)
                hor_ix = math.floor(ix/self.Y_BLOCK_SIZE)
                if abs(diff_year) >= max_diff_pc_year:
                    print(f"Year value:"
                          f"\nhor_ix: {hor_ix}"
                          f"\nangle: {angle}"
                          f"\naspect: {aspect}"
                          f"\napi: {api_year}"
                          f"\nlocal: {loc_year}"
                          f"\ndiff: {diff_year}")
                    assert abs(diff_year) < max_diff_pc_year
                for diff_day, loc_day, api_day in zip(diff_days, loc_days, api_days):
                    if abs(diff_day) >= max_diff_pc_day:
                        print(f"Month value:"
                              f"\nhor_ix: {hor_ix}"
                              f"\nangle: {angle}"
                              f"\naspect: {aspect}"
                              f"\napi: {api_day}"
                              f"\nlocal: {(loc_day/1000.0):.2f}"
                              f"\ndiff: {diff_day}")
                        assert abs(diff_day) < max_diff_pc_day

        return api_results, loc_results, diff_results

    def _read_api_pv_data_with_cache(self, test_locns: List[Tuple[float, float]], cached_data_filename: str):
        api_results = None

        if os.path.exists(f"{self.REAL_DATA_INPUT_DIR}/{cached_data_filename}.pkl"):
            api_results = pickle.load(open(f"{self.REAL_DATA_INPUT_DIR}/{cached_data_filename}.pkl", "rb"))
            if len(api_results) != len(test_locns):
                api_results = None

        if not api_results:
            api_results = self._create_cached_api_pv_data(test_locns, cached_data_filename)
        else:
            print("Using cached API data")

        return api_results

    def _create_cached_api_pv_data(self, test_locns: List[Tuple[float, float]], cached_data_filename: str):
        print("Re-fetching data from API")
        api_results = [self._get_api_pv_data(lon_east, lat_north) for lon_east, lat_north in test_locns]
        pickle.dump(api_results, open(f"{self.REAL_DATA_INPUT_DIR}/{cached_data_filename}.pkl", "wb"))
        return api_results

    def _get_api_pv_data(self, lon_east: float, lat_north: float):
        print(f"\n_get_api_pv_data(): {lon_east},{lat_north}")

        ###
        # Get api parameters
        userhorizon, angle, aspect, aspect_grass = self._get_api_inputs_from_rasters(lon_east, lat_north)

        # peakpower
        peakpower = 1.0

        # loss
        loss = 0.0
        ###

        ###
        # Get API values
        query_get = \
            f"https://re.jrc.ec.europa.eu/api/v5_2/PVcalc?lat={lat_north}&lon={lon_east}&userhorizon={userhorizon}&peakpower={peakpower}&" \
            f"loss={loss}&angle={angle}&aspect={aspect}&outputformat=json"
        query_get += "&raddatabase=PVGIS-SARAH2"
        # Don't use horizon
        #query_get += "&usehorizon=0"

        print(f"query = {query_get}")
        response = requests.get(query_get)
        # print(f"response = {response.json()}")
        api_e_day = []
        for mon_ix in range(12):
            api_e_day.append(response.json()["outputs"]["monthly"]["fixed"][mon_ix]["E_d"])
        api_e_year = response.json()["outputs"]["totals"]["fixed"]["E_y"]
        ###

        return api_e_day, api_e_year

    def _read_local_pv_data(self, test_locns: List[Tuple[float, float]]):
        print("_read_local_pv_data")
        loc_results = []
        for lon_east, lat_north in test_locns:
            result = self._get_local_pv_data(lon_east, lat_north)
            loc_results.append(result)
            print(f"\r{100*len(loc_results)/len(test_locns):.0f}%", end="")
        print()
        return loc_results

    def _get_local_pv_data(self, lon_east: float, lat_north: float):
        """This is very slow ... """
        ###
        # locally calced day Wh values
        local_e_day = []
        # for _, day, _, _ in self.instance._pv_time_steps:
        #     local_e_day.append(self._get_raster_val(f"{OUT_PV_POWER_WIND_SPECTRAL_BASENAME}{day}", lon_east, lat_north))

        # locally calced annual kWh value
        local_e_year: float = self._get_raster_val(f"{OUT_PV_POWER_WIND_SPECTRAL_BASENAME}year", lon_east, lat_north)
        ###

        return local_e_day, local_e_year

    #############
    def _test_radiation_outputs(self, test_locns: List[Tuple[float, float]], cached_data_filename: str,
                                max_diff_pc_beam: Optional[float] = None, max_diff_pc_diffuse: Optional[float] = None):
        api_results = self._read_api_radiation_data_with_cache(test_locns, cached_data_filename)
        loc_results = self._read_local_radiation_data(test_locns)

        do_assert: bool = max_diff_pc_beam is not None and max_diff_pc_diffuse is not None

        diff_results = []
        for local_rad_month, api_rad_month, (lon_east, lat_north) in zip(loc_results, api_results, test_locns):
            # Diffs
            diff_rad_month = [(self._pc_diff(l_gb, a_gb), self._pc_diff(l_gd, a_gd)) for (l_gb, l_gd), (a_gb, a_gd) in zip(local_rad_month, api_rad_month)]
            diff_results.append(diff_rad_month)

            if not do_assert:
                angle, aspect, _ = self._get_aspect_slope_from_rasters(lon_east, lat_north)
                print(f"angle: {angle}")
                print(f"aspect: API:{aspect}")
                print(f"local: one day each month:{local_rad_month}")
                print(f"api  : one day each month:{api_rad_month}")
                print(f"diffs: one day each month:{diff_rad_month}")

        if do_assert:
            for local_rad_month, api_rad_month, diff_rad_month, (lon_east, lat_north) in zip(loc_results, api_results, diff_results, test_locns):
                for local_rad, api_rad, diff_rad in zip(local_rad_month, api_rad_month, diff_rad_month):
                    if abs(diff_rad[0]) >= max_diff_pc_beam or abs(diff_rad[1]) >= max_diff_pc_diffuse:
                        angle, aspect, _ = self._get_aspect_slope_from_rasters(lon_east, lat_north)
                        print(f"angle: {angle}")
                        print(f"aspect: API:{aspect}")
                        print(f"local: one day each month:{local_rad_month}")
                        print(f"api  : one day each month:{api_rad_month}")
                        print(f"diffs: one day each month:{diff_rad}")
                        assert abs(diff_rad[0]) < max_diff_pc_beam
                        assert abs(diff_rad[1]) < max_diff_pc_diffuse

        return api_results, loc_results, diff_results

    def _read_api_radiation_data_with_cache(self, test_locns: List[Tuple[float, float]], cached_data_filename: str):
        api_results = None

        if os.path.exists(f"{self.REAL_DATA_INPUT_DIR}/{cached_data_filename}.pkl"):
            api_results = pickle.load(open(f"{self.REAL_DATA_INPUT_DIR}/{cached_data_filename}.pkl", "rb"))
            if len(api_results) != len(test_locns):
                api_results = None

        if not api_results:
            api_results = self._create_cached_api_radiation_data(test_locns, cached_data_filename)
        else:
            print("Using cached API data")

        return api_results

    def _create_cached_api_radiation_data(self, test_locns: List[Tuple[float, float]], cached_data_filename: str):
        print("Re-fetching data from API")
        api_results = [self._get_api_radiation_data(lat_north, lon_east) for lon_east, lat_north in test_locns]
        pickle.dump(api_results, open(f"{self.REAL_DATA_INPUT_DIR}/{cached_data_filename}.pkl", "wb"))
        return api_results

    def _get_api_radiation_data(self, lon_east: float, lat_north: float):
        print(f"\n_get_api_radiation_data(): {lon_east},{lat_north}")

        ###
        # Get api parameters
        userhorizon, angle, aspect, aspect_grass = self._get_api_inputs_from_rasters(lon_east, lat_north)
        ###

        ###
        # Get API values
        query_get = \
            f"https://re.jrc.ec.europa.eu/api/v5_2/DRcalc?lat={lat_north}&lon={lon_east}&userhorizon={userhorizon}" \
            f"&angle={angle}&aspect={aspect}&global=1&month=0" \
            f"&outputformat=json"
        query_get += "&raddatabase=PVGIS-SARAH2"

        print(f"query = {query_get}")
        response = requests.get(query_get)
        # print(f"response = {response.json()}")
        api_rad_month = []
        for mon_ix in range(12):
            #g_i = 0.0
            gb_i = 0.0
            gd_i = 0.0
            for hour_ix in range(24):
                out_ix = 24 * mon_ix + hour_ix
                j_response = response.json()
                #g_i += float(j_response["outputs"]["daily_profile"][out_ix]["G(i)"])
                gb_i += float(j_response["outputs"]["daily_profile"][out_ix]["Gb(i)"])
                gd_i += float(j_response["outputs"]["daily_profile"][out_ix]["Gd(i)"])
            # print(g_i, gb_i, gd_i, sep=",")
            api_rad_month.append((gb_i, gd_i))
        ###

        return api_rad_month

    def _read_local_radiation_data(self, test_locns: List[Tuple[float, float]]):
        loc_results = []
        for lon_east, lat_north in test_locns:
            result = self._get_local_radiation_data(lon_east, lat_north)
            loc_results.append(result)
        return loc_results

    def _get_local_radiation_data(self, lon_east: float, lat_north: float):
        print(f"\n_get_local_radiation_data(): {lon_east},{lat_north}")
        ###
        # locally calced day Wh values
        local_rad_month = []
        for _, day, _, _ in self.instance._pv_time_steps:
            gb_i = self._get_raster_val(f"{OUT_DIRECT_RAD_BASENAME}{day}", lon_east, lat_north)
            gd_i = self._get_raster_val(f"{OUT_DIFFUSE_RAD_BASENAME}{day}", lon_east, lat_north)
            local_rad_month.append((gb_i, gd_i))
        ###

        return local_rad_month

