# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
"""
Developed in https://github.com/cse-bristol/710-pvmaps-nix, see there for more details
"""
import argparse
import logging
import math
import os
import shutil
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from os.path import join
from typing import List, Tuple, Optional

from solar_pv.constants import MAX_PVMAPS_PROCESSES
from solar_pv.pvgis.grass_gis_user import GrassGISUser
from solar_pv.pvgis.pvmaps_setup import PVMapsSetup

##
# Raster names used in grass db
ELEVATION = "elevation"
ELEVATION_OVERRIDE = "elevation_override"
ELEVATION_PATCHED = "elevation_patched"
MASK = "mask"
ASPECT_OVERRIDE_COMPASS = "aspect_override_compass"
ASPECT_OVERRIDE_GRASS = "aspect_override_grass"
SLOPE_OVERRIDE = "slope_override"
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

CSI = "cSi"  # Case matches spectraleffect_ rasters!
CDTE = "CdTe"  # Case matches spectraleffect_ rasters!

PI_HALF: float = math.pi / 2.0


class PVMaps(GrassGISUser):
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
                 num_pv_calcs_per_year: Optional[int] = None,
                 job_id: Optional[int] = None):
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
        :param job_id optional id used where dirs etc need to be created for a job, uses process id and time if not set
        """
        super().__init__(27700, grass_dbase_dir, job_id, keep_temp_mapset)
        num_cpus = os.cpu_count()
        if num_cpus is None:
            num_cpus = 1
        if not (1 <= num_processes <= num_cpus):
            raise ValueError(f"Num processes must be 1 to {num_cpus}")
        _executor = ThreadPoolExecutor(max_workers=min(MAX_PVMAPS_PROCESSES, num_processes))
        self._set_executor(_executor)
        self._setup_grass_env()

        self.pvmaps_setup: PVMapsSetup = PVMapsSetup(_executor, grass_dbase_dir,
                                                     job_id, pvgis_data_tar_file,
                                                     self._grass_env, keep_temp_mapset)

        if not os.path.isdir(input_dir):
            raise ValueError(f"Input directory ({input_dir}) must exist and be a directory")
        self._input_dir: str = input_dir

        if os.path.exists(output_dir):
            if not os.path.isdir(output_dir):
                raise ValueError(f"Output directory ({output_dir}) must be a directory")
        else:
            os.makedirs(output_dir)
        self._output_dir: str = output_dir

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


        self._g_temp_mapset: str = ""

        self.solar_decl: List[float] = self._calc_solar_declinations()

        os.makedirs(self._output_dir, exist_ok=True)

        self.yearly_kwh_raster = None
        self.monthly_wh_rasters = None
        self.horizons = []

        self.elevation = ELEVATION

    def __del__(self):
        self._executor.shutdown()

    def create_pvmap(self, elevation_filename: str, mask_filename: str,
                     aspect_override_raster: Optional[str] = None,
                     slope_override_raster: Optional[str] = None,
                     elevation_override_filename: Optional[str] = None,
                     forced_slope_filename: Optional[str] = None,
                     forced_aspect_filename_compass: Optional[str] = None,
                     forced_aspect_filename_grass: Optional[str] = None,
                     forced_horizon_basefilename_grass: Optional[str] = None) -> None:
        """
        Run PVMaps steps against the input elevation and mask. Raises exception if something goes wrong.
        Creates output rasters in the dir setup in the constructor.
        :param aspect_override_raster: Values to use to override aspect raster at specific pixels
        :param slope_override_raster: Values to use to override slope raster at specific pixels
        :param elevation_override_filename: Heights from different sources to the LiDAR that should be used instead of
        the heights in the elevation raster
        :param elevation_filename: Just the filename of the elevation raster
        :param mask_filename:  Just the filename of the mask raster
        :param forced_slope_filename: (mainly for testing) None - calculate slope from elevation raster; filename - use
        instead of calculated slope raster (points in degrees, 0 = flat, 90 = vertical). Must set slope and aspect.
        :param forced_aspect_filename_compass: None - calculate slope from elevation raster; filename - use
        instead of calculated aspect raster (points in degrees, north = 0, CW). Must set slope and aspect.
        :param forced_aspect_filename_grass: (mainly for testing) None - calculate slope from elevation raster; filename - use
        instead of calculated aspect raster (points in degrees, east = 0, CCW). Must set slope and aspect.
        :param forced_horizon_basefilename_grass: (mainly for testing) None - calculate horizon from elevation raster;
        filename - will have _<angle> appended before extension, angle = steps using horizon_step_degrees, (points in
        degrees, east = 0, CCW) - so e.g. horizon.tif becomes horizon_045.tif etc
        """
        use_specific_aspect_overrides: bool = aspect_override_raster is not None
        use_specific_slope_overrides: bool = slope_override_raster is not None
        use_elevation_override: bool = elevation_override_filename is not None
        generate_aspect = forced_aspect_filename_compass is None and forced_aspect_filename_grass is None
        generate_slope = forced_slope_filename is None
        convert_compass_aspect: bool = forced_aspect_filename_compass is not None

        self.yearly_kwh_raster = None
        self.monthly_wh_rasters = None
        self.horizons = []
        self.elevation = ELEVATION

        self._create_temp_mapset()
        self._import_rasters(
            elevation_filename=elevation_filename,
            mask_filename=mask_filename,
            aspect_override_compass=aspect_override_raster,
            slope_override=slope_override_raster,
            elevation_override_filename=elevation_override_filename,
            forced_slope_filename=forced_slope_filename,
            forced_aspect_filename_compass=forced_aspect_filename_compass,
            forced_aspect_filename_grass=forced_aspect_filename_grass,
            forced_horizon_basefilename_grass=forced_horizon_basefilename_grass)

        if use_elevation_override and not forced_horizon_basefilename_grass:
            self._set_region_to_and_zoom(ELEVATION)
            self.elevation = self._create_patched_elevation()

        self._mask_fix_nulls()
        self._set_region_to_and_zoom(MASK)

        if not forced_horizon_basefilename_grass:
            self._calc_horizons_p()
        self._ensure_horizon_ranges_p()

        self._set_mask()
        if generate_aspect or generate_slope:
            self._calc_slope_aspect()

        if convert_compass_aspect:
            self._conv_aspect(ASPECT_COMPASS, ASPECT_GRASS)
        if use_specific_aspect_overrides:
            self._conv_aspect(ASPECT_OVERRIDE_COMPASS, ASPECT_OVERRIDE_GRASS)

        self._apply_slope_aspect_correction(use_specific_aspect_overrides, use_specific_slope_overrides)

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

    def _remove_temp_mapset_if_reqd(self):
        if not self._keep_temp_data:
            logging.info("_remove_temp_mapset_if_reqd")
            if self._g_temp_mapset and self._g_temp_mapset != self.PERMANENT_MAPSET:
                shutil.rmtree(join(self._g_dbase, self._g_location, self._g_temp_mapset))

    def _create_temp_mapset(self):
        """Set & create mapset - used for temp rasters etc"""
        # do equivalent of e.g "g.mapset -c mapset=nsd_4326"
        self._g_temp_mapset = f"pvmaps.{self.uid}"
        logging.info(f"_create_temp_mapset {self._g_temp_mapset}")

        # Clean up mapset left behind by a previous run
        temp_mapset_path: str = join(self._g_dbase, self._g_location, self._g_temp_mapset)
        if os.path.exists(temp_mapset_path):
            logging.warning(f"Found existing mapset at {temp_mapset_path} - removing it before re-creating")
            shutil.rmtree(temp_mapset_path)

        # Create temp mapset
        # New mapset has same CRS as self._g_location
        cmd = f"grass -c -e {self._g_dbase}/{self._g_location}/{self._g_temp_mapset}"
        self._run_cmd(cmd)

        self._update_mapset(self._g_temp_mapset)

    def _import_raster(self, filename: str, name: str) -> None:
        self._import_raster_raw(join(self._input_dir, filename), name)

    def _import_rasters_p(self, rasters_to_import: List[Tuple[str, str]]):
        logging.info("_import_rasters")

        for filename, _ in rasters_to_import:
            ip: str = join(self._input_dir, filename)
            if not os.path.isfile(ip):
                raise FileNotFoundError(f"Input raster file ({ip}) not found")

        self._run_cmd_via_method_p(self._import_raster, rasters_to_import)

    def _import_rasters(self, elevation_filename: str, mask_filename: str,
                        aspect_override_compass: Optional[str],
                        slope_override: Optional[str],
                        elevation_override_filename: Optional[str],
                        forced_slope_filename: Optional[str],
                        forced_aspect_filename_compass: Optional[str],
                        forced_aspect_filename_grass: Optional[str],
                        forced_horizon_basefilename_grass: Optional[str]):
        rasters_to_import: List[Tuple[str, str]] = [
            (elevation_filename, ELEVATION),
            (mask_filename, MASK),
        ]

        if aspect_override_compass:
            rasters_to_import.append((aspect_override_compass, ASPECT_OVERRIDE_COMPASS))

        if slope_override:
            rasters_to_import.append((slope_override, SLOPE_OVERRIDE))

        if elevation_override_filename:
            rasters_to_import.append((elevation_override_filename, ELEVATION_OVERRIDE))

        ###
        if forced_slope_filename and (forced_aspect_filename_grass or forced_aspect_filename_compass):
            rasters_to_import.append((forced_slope_filename, SLOPE))
            if forced_aspect_filename_compass:
                rasters_to_import.append((forced_aspect_filename_compass, ASPECT_COMPASS))
            elif forced_aspect_filename_grass:
                rasters_to_import.append((forced_aspect_filename_grass, ASPECT_GRASS))

        if forced_horizon_basefilename_grass:
            for grass_angle in range(0, 360, self._horizon_step):
                h_parts: Tuple[str, str] = os.path.splitext(forced_horizon_basefilename_grass)
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
        g_region_info: str = self._run_cmd(f"g.region raster={raster_name} zoom={raster_name} -g")

        # Enforce minimum raster size of 10 by 10
        for info in g_region_info.split(" "):
            if "=" in info:     # Filter the pid value
                k, v = info.split("=")
                if k in ("rows", "cols") and int(v) < 10:
                    raise ValueError(f"Minimum raster size supported by Grass is 10 by 10 (number of {k} is {v})")

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

    def _conv_aspect(self, aspect_compass: str, aspect_grass: str):
        self._run_cmd(f'r.mapcalc '
                      f'expression="{aspect_grass} = if({aspect_compass} == 0, 0, if({aspect_compass} < 90, 90 - {aspect_compass}, 450 - {aspect_compass}))"')

    def _apply_slope_aspect_correction(self, has_aspect_override: bool, has_slope_override: bool):
        logging.info("_apply_slope_aspect_correction")

        if has_aspect_override:
            aspect_cmd = \
                f'r.mapcalc ' \
                f'expression="{ASPECT_GRASS_ADJUSTED} = ' \
                f'if (isnull({ASPECT_OVERRIDE_GRASS}), {ASPECT_GRASS}, {ASPECT_OVERRIDE_GRASS})"'
        else:
            # When no aspect override, just set flat roofs to South-facing:
            aspect_cmd = \
                f'r.mapcalc '\
                f'expression="{ASPECT_GRASS_ADJUSTED} = '\
                f'if({SLOPE} < {self._flat_roof_degrees_threshold}, 270.0, {ASPECT_GRASS})"'

        if has_slope_override:
            slope_cmd = \
                f'r.mapcalc ' \
                f'expression="{SLOPE_ADJUSTED} = ' \
                f'if (isnull({SLOPE_OVERRIDE}), {SLOPE}, {SLOPE_OVERRIDE})"'
        else:
            # When no slope override, just set flat roof slopes:
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
        self._export_raster_raw(in_raster, out_raster_file, type="Float64")

    def _export_rasters(self):
        logging.info("_export_rasters")
        exports: List[(str, str)] = []
        monthly_wh_rasters = []
        for _, day, month, _ in self._pv_time_steps:
            if self._output_direct_diffuse:
                exports.append((f"{OUT_DIRECT_RAD_BASENAME}{day}",
                                join(self._output_dir, f"{OUT_DIRECT_RAD_BASENAME}{day}_{month}.tif")))
                exports.append((f"{OUT_DIFFUSE_RAD_BASENAME}{day}",
                                join(self._output_dir, f"{OUT_DIFFUSE_RAD_BASENAME}{day}_{month}.tif")))
            monthly_kwh_raster = join(self._output_dir, f"{OUT_PV_POWER_WIND_SPECTRAL_BASENAME}{day}_{month}.tif")
            exports.append((f"{OUT_PV_POWER_WIND_SPECTRAL_BASENAME}{day}", monthly_kwh_raster))
            monthly_wh_rasters.append(monthly_kwh_raster)

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
        self.monthly_wh_rasters = monthly_wh_rasters
        self.horizons = horizons


# Command line processing
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
        pvmaps.create_pvmap(args.elevation_filename, args.mask_filename)
    except Exception as e:
        logging.error(e)
        sys.exit(1)

    sys.exit(0)
