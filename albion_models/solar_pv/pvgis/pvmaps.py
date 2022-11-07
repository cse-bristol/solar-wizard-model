"""
Developed in https://github.com/cse-bristol/710-pvmaps-nix, see there for more details
"""
from os.path import join

import argparse
import logging
import math
import os
import re
import shlex
import shutil
import subprocess
import sys
import tarfile
import tempfile
from concurrent.futures import ThreadPoolExecutor, Future
from datetime import date
from subprocess import Popen
from typing import List, Tuple, Optional

##
# Raster names used in grass db
ELEVATION = "elevation"
ELEVATION_OVERRIDE = "elevation_override"
ELEVATION_PATCHED = "elevation_patched"
MASK = "mask"
FLAT_ROOF_ASPECT_COMPASS = "flat_roof_aspect_compass"
FLAT_ROOF_ASPECT_GRASS = "flat_roof_aspect_grass"
SLOPE = "slope"
ASPECT_GRASS = "aspect_grass"
ASPECT_COMPASS = "aspect_compass"
SLOPE_ADJUSTED = "slope_adjusted"
ASPECT_GRASS_ADJUSTED = "aspect_adjusted"
HORIZON_BASENAME = "horizon"
HORIZON090_BASENAME = "horizon090"  # horizon between 0 and 90 degrees
TEMP_BASENAME = "t2m_avg_"
LINKE_TURBIDITY_BASENAME = "tl_0m_"
BEAM_RADIATION_BASENAME = "kcb_"
DIFFUSE_RADIATION_BASENAME = "kcd_"

OUT_DIRECT_RAD_BASENAME = "bha_"
OUT_DIFFUSE_RAD_BASENAME = "dha_"
OUT_REFLECTED_RAD_BASENAME = "rha_"
OUT_PV_POWER_BASENAME = "hpv_"
OUT_PV_POWER_WIND_BASENAME = "hpv_wind_"
OUT_PV_POWER_WIND_SPECTRAL_BASENAME = "hpv_wind_spectral_"
##

PERMANENT_MAPSET = "PERMANENT"

CSI = "cSi"  # Case matches spectraleffect_ rasters!
CDTE = "CdTe"  # Case matches spectraleffect_ rasters!

PI_HALF: float = math.pi / 2.0


class PVMaps:
    """Class that sets up a grass db for PV Maps and runs the PV Maps steps"""
    def __init__(self,
                 grass_dbase_dir: str,
                 input_dir: str,
                 output_dir: str,
                 pvgis_data_tar_file: str,
                 pv_model_coeff_file_dir: str,
                 keep_temp_mapset: bool,
                 num_processes: int,
                 output_direct_diffuse: bool,
                 horizon_step_degrees: int,
                 horizon_search_distance: float,
                 flat_roof_degrees: float,
                 flat_roof_degrees_threshold: float,
                 panel_type: str,
                 num_pv_calcs_per_year: Optional[int] = None):
        """
        Create pv maps object with settings shared by all elevation / mask raster runs
        :param grass_dbase_dir: Dir where grass database is to be created or has been created
        :param input_dir: Dir where input raster files are
        :param output_dir: Dir where output raster files will be written
        :param pvgis_data_tar_file: Path and filename of the pvgis data file (pvgis_data.tar)
        :param pv_model_coeff_file_dir: Dir where the model coefficient files are located (csi.coeffs & cdte.coeffs)
        :param keep_temp_mapset: True to not cleanup temp mapset containing intermediate data
        :param num_processes: num processes to use for the multiprocess parts
        :param output_direct_diffuse: If True, output direct (beam) & diffuse irradiance/irradiation rasters
        :param horizon_step_degrees: Step to use when calculating horizons
        :param horizon_search_distance: Distance in metres to search when calculating horizons
        :param flat_roof_degrees: Degrees at which to mount panels on a flat roof
        :param flat_roof_degrees_threshold: threshold beneath which a slope value counts as flat
        :param panel_type: Use constants CSI & CDTE
        :param num_pv_calcs_per_year: None to use monthly PV calcs, or number to do per year, divides year into
        approx equal buckets of days
        """
        self._executor: Optional[ThreadPoolExecutor] = None
        self._gisrc_filename: Optional[str] = None
        self._grass_env = None
        self._keep_temp_mapset = keep_temp_mapset

        if os.path.exists(grass_dbase_dir):
            if not os.path.isdir(grass_dbase_dir):
                raise ValueError(f"Grass dbase directory ({grass_dbase_dir}) must be a directory")
        else:
            os.makedirs(grass_dbase_dir)
        self._g_dbase: str = grass_dbase_dir

        if not os.path.isdir(input_dir):
            raise ValueError(f"Input directory ({input_dir}) must exist and be a directory")
        self._input_dir: str = input_dir

        if os.path.exists(output_dir):
            if not os.path.isdir(output_dir):
                raise ValueError(f"Output directory ({output_dir}) must be a directory")
        else:
            os.makedirs(output_dir)
        self._output_dir: str = output_dir

        self._pvgis_data_tar: str = pvgis_data_tar_file

        num_cpus = os.cpu_count()
        if num_cpus is None:
            num_cpus = 1
        if not (1 <= num_processes <= num_cpus):
            raise ValueError(f"Num processes must be 1 to {num_cpus}")
        self._num_procs: int = num_processes

        self._output_direct_diffuse = output_direct_diffuse

        if not (1 <= horizon_step_degrees <= 360):
            raise ValueError(f"Horizon step must be 1 to 360")
        self._horizon_step: int = int(horizon_step_degrees)

        if not (horizon_search_distance > 0):
            raise ValueError(f"Horizon search distance must be positive")
        self._horizon_search_distance = horizon_search_distance

        if not (0 <= flat_roof_degrees <= 90):
            raise ValueError(f"Flat roof degrees must be 0 to 90")
        self._flat_roof_degrees = flat_roof_degrees

        if not (0 <= flat_roof_degrees_threshold <= 90):
            raise ValueError(f"Flat roof degrees must be 0 to 90")
        self._flat_roof_degrees_threshold = flat_roof_degrees_threshold

        if panel_type not in (CSI, CDTE):
            raise ValueError(f"Panel type must be {CSI} or {CDTE}")
        self._pv_model_coeff_file: str = join(pv_model_coeff_file_dir, f"{panel_type.lower()}.coeffs")
        if not os.path.isfile(self._pv_model_coeff_file):
            raise ValueError(f"Model coefficient file ({self._pv_model_coeff_file}) not found")
        self._r_spectral: str = f"spectraleffect_{panel_type}_"

        self._pv_time_steps: List[Tuple[int, int, int, int]]
        if num_pv_calcs_per_year is None:
            self._pv_time_steps = self._monthly_pv_time_steps()
        else:
            if not (1 <= num_pv_calcs_per_year <= 365):
                raise ValueError(f"Num PV calcs per year must be between 1 and 365")
            else:
                self._pv_time_steps = self._calc_pv_time_steps(num_pv_calcs_per_year)
        ###

        self._g_location: str = "grassdata"

        self._g_temp_mapset: str = ""

        self._gisrc_filename = join(tempfile.gettempdir(), f"pvmaps.{os.getpid()}.rc")

        self.solar_decl: List[float] = self._calc_solar_declinations()

        self._executor = ThreadPoolExecutor(max_workers=self._num_procs)

        self._setup_grass_env()

        self._init_grass_db_pvmaps_data()

        os.makedirs(self._output_dir, exist_ok=True)

        self.yearly_kwh_raster = None
        self.monthly_kwh_rasters = None
        self.horizons = []

        self.elevation = ELEVATION

    def __del__(self):
        if self._executor:
            self._executor.shutdown()
        if self._gisrc_filename is not None and os.path.exists(self._gisrc_filename):
            os.remove(self._gisrc_filename)

    def create_pvmap(self, elevation_filename: str, mask_filename: str,
                     flat_roof_aspect_filename: Optional[str],
                     elevation_override_filename: Optional[str],
                     forced_slope_filename: Optional[str] = None,
                     forced_aspect_filename: Optional[str] = None,
                     forced_horizon_basefilename: Optional[str] = None) -> None:
        """
        Run PVMaps steps against the input elevation and mask. Raises exception if something goes wrong.
        Creates output rasters in the dir setup in the constructor.
        :param flat_roof_aspect_filename: Values to use for the aspects of arrays installed on flat roofs, None if
        there are no flat roofs
        :param elevation_override_filename: Heights from different sources to the LiDAR that should be used instead of
        the heights in the elevation raster
        :param elevation_filename: Just the filename of the elevation raster
        :param mask_filename:  Just the filename of the mask raster
        :param forced_slope_filename: (mainly for testing) None - calculate slope from elevation raster; filename - use
        instead of calculated slope raster (points in degrees, 0 = flat, 90 = vertical). Must set slope and aspect.
        :param forced_aspect_filename: (mainly for testing) None - calculate slope from elevation raster; filename - use
        instead of calculated aspect raster (points in degrees, east = 0, CCW). Must set slope and aspect.
        :param forced_horizon_basefilename: (mainly for testing) None - calculate horizon from elevation raster;
        filename - will have _<angle> appended before extension, angle = steps using horizon_step_degrees, (points in
        degrees, east = 0, CCW) - so e.g. horizon.tif becomes horizon_045.tif etc
        """
        use_flat_roof_aspects: bool = flat_roof_aspect_filename is not None
        use_elevation_override: bool = elevation_override_filename is not None
        self.yearly_kwh_raster = None
        self.monthly_kwh_rasters = None
        self.horizons = []
        self.elevation = ELEVATION

        self._create_temp_mapset()
        self._import_rasters(elevation_filename, mask_filename, flat_roof_aspect_filename, elevation_override_filename,
                             forced_slope_filename, forced_aspect_filename, forced_horizon_basefilename)

        if use_elevation_override and not forced_horizon_basefilename:
            self._set_region_to_and_zoom(ELEVATION)
            self.elevation = self._create_patched_elevation()

        self._mask_fix_nulls()
        self._set_region_to_and_zoom(MASK)

        if not forced_horizon_basefilename:
            self._calc_horizons_p()
        self._ensure_horizon_ranges_p()

        self._set_mask()
        if not (forced_slope_filename and forced_aspect_filename):
            self._calc_slope_aspect()
        self._conv_flat_panel_aspects(use_flat_roof_aspects)
        self._flat_panel_correction(use_flat_roof_aspects)

        self._pv_calcs_p()
        self._apply_wind_corrections_p()
        self._apply_spectral_corrections_p()
        self._get_annual_rasters_p()

        self._export_rasters()

        self._remove_temp_mapset_if_reqd()

    def _calc_solar_declinations(self):
        return [self._calc_solar_declination(day) for _, day, _, _ in self._pv_time_steps]

    @staticmethod
    def _calc_solar_declination(day: int):
        # As in r.pv main.c ...
        # double com_declin(int no_of_day)
        # {
        #     double d1, decl;
        #
        #     d1 = pi2 * no_of_day / 365.25;
        #     decl = -asin(0.3978 * sin(d1 - 1.4 + 0.0355 * sin(d1 - 0.0489)));
        #
        #     return (decl);
        # }
        # See also http://www.reuk.co.uk/wordpress/solar/solar-declination/

        d1 = 2.0 * math.pi * day / 365.25
        dec = math.asin(0.3978 * math.sin(d1 - 1.4 + 0.0355 * math.sin(d1 - 0.0489)))

        return dec

    @staticmethod
    def _monthly_pv_time_steps() -> List[Tuple[int, int, int, int]]:
        """These match totpv_incl.sh"""
        return [
            (0, 17, 1, 31),
            (1, 46, 2, 28),
            (2, 75, 3, 31),
            (3, 103, 4, 30),
            (4, 135, 5, 31),
            (5, 162, 6, 30),
            (6, 198, 7, 31),
            (7, 228, 8, 31),
            (8, 259, 9, 30),
            (9, 289, 10, 31),
            (10, 319, 11, 30),
            (11, 345, 12, 31),
        ]

    @staticmethod
    def _calc_pv_time_steps(num_pv_calcs_per_year: int) -> List[Tuple[int, int, int, int]]:
        step: float = 365.25 / num_pv_calcs_per_year
        rtn_val: List[Tuple[int, int, int, int]] = []
        prev_end: int = 0
        prev_end_f = 1.0
        for ix in range(num_pv_calcs_per_year):
            end_f: float = prev_end_f + step
            start: int = prev_end + 1
            end = int(round(end_f))
            if end > 365:
                end = 365
            mid: int = int(round((start + end) / 2.0))
            rtn_val.append((ix, mid, date.fromordinal(date(2010, 1, 1).toordinal() + mid - 1).month, end - start))
            prev_end = end
            prev_end_f = end_f
        return rtn_val

    def _setup_grass_env(self):
        """See https://grasswiki.osgeo.org/wiki/Working_with_GRASS_without_starting_it_explicitly
        """
        # write initial gisrc file
        self._update_mapset(PERMANENT_MAPSET)

        self._find_grass_locn()
        self._check_reqd_grass_paths()

        # Setup env vars
        grass_env = os.environ.copy()
        grass_env["GISBASE"] = self._grass_install_dir
        grass_env["GISRC"] = self._gisrc_filename

        grass_bin = join(self._grass_install_dir, 'bin')
        grass_scripts = join(self._grass_install_dir, 'scripts')
        grass_env["PATH"] = f"{grass_bin}{os.pathsep}" \
                            f"{grass_scripts}{os.pathsep}" \
                            f"{grass_env.get('PATH', '')}"

        grass_lib = join(self._grass_install_dir, 'lib')
        grass_env["LD_LIBRARY_PATH"] = f"{grass_lib}{os.pathsep}" \
                                       f"{grass_env.get('LD_LIBRARY_PATH', '')}"

        # Ref https://grasswiki.osgeo.org/wiki/Working_with_GRASS_without_starting_it_explicitly#Python:_GRASS_GIS_8_with_existing_location
        python_path: str = join(self._grass_install_dir, "etc", "python")
        grass_env["PYTHONPATH"] = f"{python_path}{os.pathsep}{grass_env.get('PYTHONPATH', '')}"  # for sub-processes
        self._grass_env = grass_env

    def _check_reqd_grass_paths(self):
        grass_bin = join(self._grass_install_dir, 'bin')
        grass_scripts = join(self._grass_install_dir, 'scripts')
        grass_lib = join(self._grass_install_dir, 'lib')
        grass_python = join(self._grass_install_dir, 'etc', 'python')
        if not os.path.exists(self._grass_install_dir):
            raise FileNotFoundError(f"Path {self._grass_install_dir} not found")
        if not os.path.exists(self._gisrc_filename):
            raise FileNotFoundError(f"Path {self._gisrc_filename} not found")
        if not os.path.exists(grass_bin):
            raise FileNotFoundError(f"Path {grass_bin} not found")
        if not os.path.exists(grass_scripts):
            raise FileNotFoundError(f"Path {grass_scripts} not found")
        if not os.path.exists(grass_lib):
            raise FileNotFoundError(f"Path {grass_lib} not found")
        if not os.path.exists(grass_python):
            raise FileNotFoundError(f"Path {grass_python} not found")

    def _update_mapset(self, mapset: str):
        with open(self._gisrc_filename, "w") as rcfile:
            rcfile.write(f"GISDBASE: {self._g_dbase}\n")
            rcfile.write(f"LOCATION_NAME: {self._g_location}\n")
            rcfile.write(f"MAPSET: {mapset}\n")
            rcfile.write("\n")

    @staticmethod
    def _run_cmd_via_method(method, args_list: List[Tuple]) -> None:
        for args in args_list:
            method(*args)

    def _run_cmd_via_method_p(self, method, args_list: List[Tuple]) -> None:
        """Ok to use threads as the cmd exe blocks. This does what ThreadPoolExecutor.map()
        does but also collects multiple exceptions"""
        futures: List[Tuple[str, Future]] = []
        for args in args_list:
            future = self._executor.submit(
                lambda p: method(*p), args)
            id_s = f"{method.__name__}{args}"
            futures.append((id_s, future))

        exc_str: str = ""
        for id_s, future in futures:
            try:
                future.result()
            except Exception as ex:
                if exc_str:
                    exc_str += "\n"
                exc_str += f"{id_s} raised '{str(ex)}'"
        if exc_str:
            raise Exception(f"Exception(s) raised\n{exc_str}")

    def _run_cmd(self, cmd_line: str, exp_returncode: int = 0) -> None:
        args: List[str] = shlex.split(cmd_line)

        process = Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=self._grass_env)
        process_output: str = ""
        with process.stdout:
            line_s = process.stdout.read().decode("utf-8").replace("\n", " ").strip()
            line_s = re.sub(r"[\x00-\x1f\x7f-\x9f]", "", line_s)
            if len(line_s) > 0:
                log_msg: str = f"{process.pid}: {line_s}"
                logging.debug(log_msg)
                process_output += log_msg
        rtn_code: int = process.wait()

        if rtn_code != exp_returncode:
            logging.error(process_output)
            raise Exception(f"Command {cmd_line} returned error code {rtn_code}")

    def _init_grass_db_pvmaps_data(self):
        location = join(self._g_dbase, self._g_location)
        permanent_mapset = join(self._g_dbase, self._g_location, PERMANENT_MAPSET)

        if not os.path.exists(location):
            if not os.path.isfile(self._pvgis_data_tar):
                raise FileNotFoundError(f"File {self._pvgis_data_tar} not found")

            logging.info("_init_grass_db_pvmaps_data")

            # Create a new grass gis database
            self._run_cmd(f"grass -c EPSG:4326 -e {location}")
            # Extract the pvgis data into it's permanent mapset
            with tarfile.open(self._pvgis_data_tar, "r") as tar:
                tar.extractall(path=permanent_mapset)

        elif not os.path.exists(permanent_mapset):
            raise FileNotFoundError(f"Grass DB path ({location}) exists but {PERMANENT_MAPSET} is missing!")

    def _remove_temp_mapset_if_reqd(self):
        if not self._keep_temp_mapset:
            logging.info("_remove_temp_mapset_if_reqd")
            if self._g_temp_mapset and self._g_temp_mapset != PERMANENT_MAPSET:
                shutil.rmtree(join(self._g_dbase, self._g_location, self._g_temp_mapset))

    def _create_temp_mapset(self):
        """Set & create mapset - used for temp rasters etc"""
        # do equivalent of e.g "g.mapset -c mapset=nsd_4326"
        self._g_temp_mapset = f"pvmaps.{os.getpid()}"
        logging.info(f"_create_temp_mapset {self._g_temp_mapset}")

        # Create temp mapset
        # New mapset has same CRS as self._g_location
        cmd = f"grass -c -e {self._g_dbase}/{self._g_location}/{self._g_temp_mapset}"
        self._run_cmd(cmd)

        self._update_mapset(self._g_temp_mapset)

    def _import_raster(self, filename: str, name: str) -> None:
        cmd_line: str = f"r.import input={self._input_dir}{os.sep}{filename} output={name}"
        self._run_cmd(cmd_line)

    def _import_rasters_p(self, rasters_to_import: List[Tuple[str, str]]):
        logging.info("_import_rasters")

        for filename, _ in rasters_to_import:
            ip: str = join(self._input_dir, filename)
            if not os.path.isfile(ip):
                raise FileNotFoundError(f"Input raster file ({ip}) not found")

        self._run_cmd_via_method_p(self._import_raster, rasters_to_import)

    def _import_rasters(self, elevation_filename: str, mask_filename: str,
                        flat_roof_aspect_filename: Optional[str],
                        elevation_override_filename: Optional[str],
                        forced_slope_filename: Optional[str],
                        forced_aspect_filename: Optional[str],
                        forced_horizon_basefilename: Optional[str]):
        rasters_to_import: List[Tuple[str, str]] = [
            (elevation_filename, ELEVATION),
            (mask_filename, MASK),
        ]

        if flat_roof_aspect_filename:
            rasters_to_import.append((flat_roof_aspect_filename, FLAT_ROOF_ASPECT_COMPASS))

        if elevation_override_filename:
            rasters_to_import.append((elevation_override_filename, ELEVATION_OVERRIDE))

        ###
        if forced_slope_filename and forced_aspect_filename:
            rasters_to_import.append((forced_slope_filename, SLOPE))
            rasters_to_import.append((forced_aspect_filename, ASPECT_GRASS))
        if forced_horizon_basefilename:
            for grass_angle in range(0, 360, self._horizon_step):
                h_parts: Tuple[str, str] = os.path.splitext(forced_horizon_basefilename)
                horizon_raster = f"{h_parts[0]}_{grass_angle:03d}{h_parts[1]}"
                horizon_name = f"{HORIZON_BASENAME}_{grass_angle:03d}"
                rasters_to_import.append((horizon_raster, horizon_name))
        ###

        self._import_rasters_p(rasters_to_import)

    def _mask_fix_nulls(self):
        """Replaces 0s with nulls in the mask"""
        logging.info("_mask_fix_nulls")
        self._run_cmd(f"r.null map={MASK} setnull=0")

    def _set_mask(self):
        """Masks where value is null, so use _mask_fix_nulls() before this"""
        logging.info("_set_mask")
        self._run_cmd(f"r.mask raster={MASK}")

    def _set_region_to_and_zoom(self, raster_name: str):
        """Zooms to the non-null central rectangle part of raster_name"""
        logging.info("_set_region_to_and_zoom")
        self._run_cmd(f"g.region raster={raster_name} zoom={raster_name}")

    def _horizon_directions(self) -> List[Tuple[int]]:
        return [(a,) for a in range(0, 360, self._horizon_step)]

    def _calc_horizon(self, direction: int):
        """Calcs for all points within the region, but uses data outside the region"""
        # Horizons for pvmaps are counterclockwise from east - see pvmaps.pdf p13 3rd bullet - as generated by r.horizon
        # Their heights are in radians unless "-d" is used
        # Looks like should be the default (radians) going by the North Carolina example
        # in https://grass.osgeo.org/grass80/manuals/r.sun.html
        self._run_cmd(f"r.horizonmask elevation={self.elevation} mask={MASK} direction={direction:03d} "
                      f"output={HORIZON_BASENAME} maxdistance={self._horizon_search_distance}")

    def _calc_horizons_p(self):
        directions: List[Tuple[int]] = self._horizon_directions()
        logging.info(f"_calc_horizons {directions}")
        self._run_cmd_via_method_p(self._calc_horizon, directions)

    def _ensure_horizon_ranges(self, direction):
        """Ensure horizon all horizons are 0-90 degrees"""
        horizon: str = f"{HORIZON_BASENAME}_{direction:03d}"
        horizon090: str = f"{HORIZON090_BASENAME}_{direction:03d}"
        self._run_cmd(
            f'r.mapcalc "{horizon090} = if({horizon} >= 0.0, if({horizon} < {PI_HALF}, {horizon}, {PI_HALF}), 0.0)"')

    def _ensure_horizon_ranges_p(self):
        logging.info("_ensure_horizon_ranges")
        directions: List[Tuple[float]] = self._horizon_directions()
        self._run_cmd_via_method_p(self._ensure_horizon_ranges, directions)

    def _calc_slope_aspect(self):
        logging.info("_calc_slope_aspect")
        self._run_cmd(f"r.slope.aspect elevation={self.elevation} slope={SLOPE} aspect={ASPECT_GRASS}")

    def _conv_flat_panel_aspects(self, use_flat_roof_aspects: bool):
        if use_flat_roof_aspects:
            self._run_cmd(f'r.mapcalc '
                          f'expression="{FLAT_ROOF_ASPECT_GRASS} = if({FLAT_ROOF_ASPECT_COMPASS} == 0, 0, if({FLAT_ROOF_ASPECT_COMPASS} < 90, 90 - {FLAT_ROOF_ASPECT_COMPASS}, 450 - {FLAT_ROOF_ASPECT_COMPASS}))"')

    def _flat_panel_correction(self, use_flat_roof_aspects: bool):
        logging.info("_flat_panel_correction")

        # If slope shallower than _flat_roof_degrees_threshold, set aspect = value from flat_roof_aspect if
        # available, otherwise south
        aspect_cmd: str
        if use_flat_roof_aspects:
            aspect_cmd = \
                f'r.mapcalc '\
                f'expression="{ASPECT_GRASS_ADJUSTED} = '\
                f'if({SLOPE} < {self._flat_roof_degrees_threshold}, if (isnull({FLAT_ROOF_ASPECT_GRASS}), 270.0, {FLAT_ROOF_ASPECT_GRASS}), {ASPECT_GRASS})"'
        else:
            aspect_cmd = \
                f'r.mapcalc '\
                f'expression="{ASPECT_GRASS_ADJUSTED} = '\
                f'if({SLOPE} < {self._flat_roof_degrees_threshold}, 270.0, {ASPECT_GRASS})"'

        slope_cmd: str = \
            f'r.mapcalc ' \
            f'expression="{SLOPE_ADJUSTED} = if({SLOPE} < {self._flat_roof_degrees_threshold}, {self._flat_roof_degrees}, {SLOPE})"'

        self._run_cmd_via_method_p(self._run_cmd, [
            (slope_cmd,),
            (aspect_cmd,)
        ])

    def _create_patched_elevation(self) -> str:
        self._run_cmd(f'r.mapcalc '
                      f'expression='
                      f'"{ELEVATION_PATCHED}='
                      f'if (isnull({ELEVATION_OVERRIDE}), {ELEVATION}, '
                      f'if (isnull({ELEVATION}), {ELEVATION_OVERRIDE}, '
                      f'max({ELEVATION_OVERRIDE}, {ELEVATION})))"')
        return ELEVATION_PATCHED

    def _pv_calc(self, ix: int, day: int, mon: int, _: int):
        """Settings based on totpv_incl.sh"""
        mon_str: str = f"{mon:02d}"
        # -a Do you want to include the effect of shallow angle reflectivity (y/n)
        # -s Do you want to incorporate the shadowing effect of terrain (y/n) => include to use the horizon data
        # -m Do you want to use the low-memory version of the program (y/n)
        # -i Do you want to use clear-sky irradiance for calculating efficiency (y/n) ... i.e. ignore clouds etc
        # Albedo value of 0.2 confirmed as being value used by pvgis api - see email from JRC-PVGIS@ec.europa.eu 6/9/22
        cmd: str = f"r.pv -a -s --quiet " \
                   f"elevation={self.elevation} " \
                   f"aspect={ASPECT_GRASS_ADJUSTED} " \
                   f"slope={SLOPE_ADJUSTED} " \
                   f"horizon_basename={HORIZON090_BASENAME} horizon_step={self._horizon_step} " \
                   f"albedo_value=0.2 " \
                   f"linke={LINKE_TURBIDITY_BASENAME}{mon_str} " \
                   f"coefbh={BEAM_RADIATION_BASENAME}{mon_str} " \
                   f"coefdh={DIFFUSE_RADIATION_BASENAME}{mon_str} " \
                   f"temperatures={TEMP_BASENAME}{mon_str}_00,{TEMP_BASENAME}{mon_str}_03,{TEMP_BASENAME}{mon_str}_06," \
                        f"{TEMP_BASENAME}{mon_str}_09,{TEMP_BASENAME}{mon_str}_12,{TEMP_BASENAME}{mon_str}_15," \
                        f"{TEMP_BASENAME}{mon_str}_18,{TEMP_BASENAME}{mon_str}_21 " \
                   f"declin={self.solar_decl[ix]} " \
                   f"civiltime=0 " \
                   f"modelparameters={self._pv_model_coeff_file} " \
                   f"day={day} " \
                   f"step=0.25 " \
                   f"beam_rad={OUT_DIRECT_RAD_BASENAME}{day} " \
                   f"diff_rad={OUT_DIFFUSE_RAD_BASENAME}{day} " \
                   f"refl_rad={OUT_REFLECTED_RAD_BASENAME}{day} " \
                   f"glob_pow={OUT_PV_POWER_BASENAME}{day}"
        self._run_cmd(cmd, exp_returncode=1)  # r.pv returns 1 if it fails and if it succeeds ... if there are no o/ps, next steps will fail

    def _pv_calcs_p(self):
        logging.info("_pv_calcs")
        self._run_cmd_via_method_p(self._pv_calc, self._pv_time_steps)

    def _apply_wind_correction(self, ix: int, day: int, mon: int, num_days: int):
        """The windeffect data rasters have missing data in Northern Scotland, so default to 1.0"""
        self._run_cmd(f"r.mapcalc --quiet "
                      f"expression={OUT_PV_POWER_WIND_BASENAME}{day}="
                      f"if(isnull(windeffect_{mon:02d}),1.0,windeffect_{mon:02d})*{OUT_PV_POWER_BASENAME}{day}")

    def _apply_wind_corrections_p(self):
        logging.info("_apply_wind_corrections")
        self._run_cmd_via_method_p(self._apply_wind_correction, self._pv_time_steps)

    def _apply_spectral_correction(self, ix: int, day: int, mon: int, num_days: int):
        """The spectral data rasters have missing data in Northern England and Scotland, so default to 1.0"""
        self._run_cmd(f"r.mapcalc --quiet "
                      f"expression={OUT_PV_POWER_WIND_SPECTRAL_BASENAME}{day}="
                      f"if(isnull({self._r_spectral}{mon:02d}),1.0,{self._r_spectral}{mon:02d})*{OUT_PV_POWER_WIND_BASENAME}{day}")

    def _apply_spectral_corrections_p(self):
        logging.info("_apply_spectral_corrections")
        self._run_cmd_via_method_p(self._apply_spectral_correction, self._pv_time_steps)

    def _get_annual_rasters_p(self):
        logging.info("_get_annual_rasters")
        bha_cmd: str = ""
        dha_cmd: str = ""
        hpv_cmd: str = ""
        for ix, day, _, num_days in self._pv_time_steps:
            if ix:
                bha_cmd += "+"
                dha_cmd += "+"
                hpv_cmd += "+"
            bha_cmd += f"{OUT_DIRECT_RAD_BASENAME}{day}*{num_days}"
            dha_cmd += f"{OUT_DIFFUSE_RAD_BASENAME}{day}*{num_days}"
            hpv_cmd += f"{OUT_PV_POWER_WIND_SPECTRAL_BASENAME}{day}*{num_days}"
        bha_cmd = f"r.mapcalc expression={OUT_DIRECT_RAD_BASENAME}year=({bha_cmd})*0.001"
        dha_cmd = f"r.mapcalc expression={OUT_DIFFUSE_RAD_BASENAME}year=({dha_cmd})*0.001"
        hpv_cmd = f"r.mapcalc expression={OUT_PV_POWER_WIND_SPECTRAL_BASENAME}year=({hpv_cmd})*0.001"
        self._run_cmd_via_method_p(self._run_cmd, [(bha_cmd,), (dha_cmd,), (hpv_cmd,)])

    def _export_raster(self, in_raster: str, out_raster_file: str):
        self._run_cmd(f"r.out.gdal --overwrite input={in_raster} output='{out_raster_file}' "
                      f"format=GTiff type=Float64 -c createopt=\"COMPRESS=PACKBITS,TILED=YES\"")

    def _export_rasters(self):
        logging.info("_export_rasters")
        exports: List[(str, str)] = []
        monthly_kwh_rasters = []
        for _, day, month, _ in self._pv_time_steps:
            if self._output_direct_diffuse:
                exports.append((f"{OUT_DIRECT_RAD_BASENAME}{day}",
                                join(self._output_dir, f"{OUT_DIRECT_RAD_BASENAME}{day}_{month}.tif")))
                exports.append((f"{OUT_DIFFUSE_RAD_BASENAME}{day}",
                                join(self._output_dir, f"{OUT_DIFFUSE_RAD_BASENAME}{day}_{month}.tif")))
            monthly_kwh_raster = join(self._output_dir, f"{OUT_PV_POWER_WIND_SPECTRAL_BASENAME}{day}_{month}.tif")
            exports.append((f"{OUT_PV_POWER_WIND_SPECTRAL_BASENAME}{day}", monthly_kwh_raster))
            monthly_kwh_rasters.append(monthly_kwh_raster)

        if self._output_direct_diffuse:
            exports.append((f"{OUT_DIRECT_RAD_BASENAME}year",
                            join(self._output_dir, f"{OUT_DIRECT_RAD_BASENAME}year.tif")))
            exports.append((f"{OUT_DIFFUSE_RAD_BASENAME}year",
                            join(self._output_dir, f"{OUT_DIFFUSE_RAD_BASENAME}year.tif")))

        yearly_kwh_raster = join(self._output_dir, f"{OUT_PV_POWER_WIND_SPECTRAL_BASENAME}year.tif")
        exports.append((f"{OUT_PV_POWER_WIND_SPECTRAL_BASENAME}year", yearly_kwh_raster))

        horizons = []
        for h_dir in self._horizon_directions():
            h_tif = join(self._output_dir, f"{HORIZON090_BASENAME}_{h_dir[0]:03d}.tif")
            horizons.append(h_tif)
            exports.append((f"{HORIZON090_BASENAME}_{h_dir[0]:03d}", h_tif))

        self._run_cmd_via_method_p(self._export_raster, exports)
        self.yearly_kwh_raster = yearly_kwh_raster
        self.monthly_kwh_rasters = monthly_kwh_rasters
        self.horizons = horizons

    def _find_grass_locn(self):
        self._grass_install_dir = ""
        g_exe_locns: List[str] = self._whereis("grass")
        for g_exe_locn in g_exe_locns:
            if g_exe_locn.endswith("grass"):
                g_root_dir: str = os.path.dirname(g_exe_locn)
                if g_root_dir.endswith("bin"):
                    g_root_dir: str = os.path.dirname(g_root_dir)
                for f in os.listdir(g_root_dir):
                    if f.startswith("grass"):
                        self._grass_install_dir = join(g_root_dir, f)
                        logging.info(f"Using grass from here: {self._grass_install_dir}")
                        return
        raise FileNotFoundError(f"Failed to find grass install dir")

    @staticmethod
    def _whereis(cmd: str) -> List[str]:
        """Based on shutil.which() but does whereis i.e. finds all matches where a dir from
        the path contains an executable file called 'cmd'"""
        matches = []
        path = os.environ.get("PATH", None)
        if path is None:
            path = os.defpath
        if path:
            path_dirs = path.split(os.pathsep)
            seen = set()
            for path_dir in path_dirs:
                if path_dir not in seen:
                    seen.add(path_dir)
                    name = os.path.join(path_dir, cmd)
                    if os.path.exists(name) and os.access(name, os.F_OK | os.X_OK) and not os.path.isdir(name):
                        matches.append(name)
        return matches


# Command line processing
# e.g. --verbose --input_dir ../pvmaps/SampleInputsNeil_4326/ --keep_temp_mapset --pv_model_coeff_file_dir ../pvgis_scripts/PVPerf elevation_4326.tif mask_4326.tif
if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                     description="Set up a grass db for PV Maps and run the PV Maps steps",
                                     prog="pvmaps")
    parser.add_argument("--grass_dbase_dir", help="Dir where grass database is to be created or has been created", type=str, default=".")
    parser.add_argument("--input_dir", help="Dir where input raster files are", type=str, default=".")
    parser.add_argument("--output_dir", help="Dir where output raster files will be written", type=str, default=".")
    parser.add_argument("--pvgis_data_tar_file", help="Path and filename of the pvgis data file (pvgis_data.tar)", type=str, default=f".{os.sep}pvgis_data.tar")
    parser.add_argument("--pv_model_coeff_file_dir", help="Dir where the model coefficient files are located (csi.coeffs & cdte.coeffs)", type=str, default=".")
    parser.add_argument("--verbose", help="Get all logging", action="store_true")
    parser.add_argument("--quiet", help="Get only error logging", action="store_true")
    parser.add_argument("--keep_temp_mapset", help="Disable cleanup temp mapset containing intermediate data", action="store_true")
    parser.add_argument("--num_processes", help="Number processes to use for the multiprocess parts", type=int, default=os.cpu_count())
    parser.add_argument("--horizon_step_degrees", help="Step to use when calculating horizons", type=int, default=30)
    parser.add_argument("--panel_type", help=f"Either {CSI} or {CDTE}", type=str, default=CSI)
    parser.add_argument("--num_pv_calcs_per_year", help="If set, number calcs do per year, divides year into approx equal buckets of days; Otherwise, monthly.", type=Optional[int], default=None)
    parser.add_argument("--output_direct_diffuse", help="If set, output direct (beam) & diffuse irradiance/irradiation rasters", action="store_true")
    parser.add_argument("--horizon_search_distance", default=1000.0)
    parser.add_argument("--flat_roof_degrees", default=10.0)
    parser.add_argument("--flat_roof_degrees_threshold", default=10.0)

    parser.add_argument("elevation_filename", help="Just the filename of the elevation raster")
    parser.add_argument("mask_filename", help="Just the filename of the mask raster")

    args = parser.parse_args()
    log_level: int = logging.INFO
    if args.verbose:
        log_level = logging.DEBUG
    elif args.quiet:
        log_level = logging.ERROR
    logging.basicConfig(format='%(asctime)s: %(levelname)s: %(message)s',
                        level=log_level, datefmt="%d/%m/%Y %H:%M:%S")

    try:
        pvmaps: PVMaps = PVMaps(
            args.grass_dbase_dir,
            args.input_dir,
            args.output_dir,
            args.pvgis_data_tar_file,
            args.pv_model_coeff_file_dir,
            args.keep_temp_mapset,
            args.num_processes,
            args.output_direct_diffuse,
            args.horizon_step_degrees,
            args.horizon_search_distance,
            args.flat_roof_degrees,
            args.flat_roof_degrees_threshold,
            args.panel_type,
            args.num_pv_calcs_per_year)
        pvmaps.create_pvmap(args.elevation_filename, args.mask_filename, None)
    except Exception as e:
        logging.error(e)
        sys.exit(1)

    sys.exit(0)
