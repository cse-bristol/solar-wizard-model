# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import logging
import os
import shutil
import tarfile
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor
from os.path import join
from typing import Dict, List, Set, Tuple

from solar_pv import gdal_helpers
from pvgis.grass_gis_user import GrassGISUser
from solar_pv.transformations import _7_PARAM_SHIFT

BOUNDS_OF_27700: str = "bounds_of_27700"
PVMAPS_DONE_FNAME: str = "pvmaps.setup.done"
PVMAPS_WORLD_DONE_FNAME: str = "pvmaps.world.done"
PVGIS_DATA_UK_TAR: str = "pvgis_data_uk.tar"

class PVMapsSetup(GrassGISUser):
    # Std bounds from https://epsg.io/27700
    EPSG27700_bounds_buffer_4326: float = 1.0
    EPSG27700_bounds_4326 = (
        -9.0 - EPSG27700_bounds_buffer_4326,
        49.75 - EPSG27700_bounds_buffer_4326,
        2.01 + EPSG27700_bounds_buffer_4326,
        61.01 + EPSG27700_bounds_buffer_4326)
    EPSG27700_bounds_4326_json: bytes = bytes(
        '{"type":"Polygon",'
        '"crs":{"type":"name","properties":{"name":"EPSG:4326"}},'
        '"coordinates":[['
        f'[{EPSG27700_bounds_4326[0]},{EPSG27700_bounds_4326[1]}],'
        f'[{EPSG27700_bounds_4326[0]},{EPSG27700_bounds_4326[3]}],'
        f'[{EPSG27700_bounds_4326[2]},{EPSG27700_bounds_4326[3]}],'
        f'[{EPSG27700_bounds_4326[2]},{EPSG27700_bounds_4326[1]},],'
        f'[{EPSG27700_bounds_4326[0]},{EPSG27700_bounds_4326[1]}]]]'
        '}', 'utf-8')

    EPSG27700_bounds_buffer_27700: float = 20000.0  # Expand std 27700 bounds to include Lowestoft and Muckle Flugga
    EPSG27700_bounds_27700: Tuple[float, float, float, float] = (
        -103976.3 - EPSG27700_bounds_buffer_27700,
        -16703.87 - EPSG27700_bounds_buffer_27700,
        652897.98 + EPSG27700_bounds_buffer_27700,
        1199851.44 + EPSG27700_bounds_buffer_27700)

    thread_lock = threading.Lock()

    def __init__(self,
                 executor: ThreadPoolExecutor,
                 grass_dbase_dir: str,
                 job_id: int,
                 pvgis_data_tar_file: str,
                 dest_grass_env: Dict,
                 keep_temp_mapset: bool):
        super().__init__(4326, grass_dbase_dir, job_id, keep_temp_mapset)
        self._set_executor(executor)

        self._pvgis_data_tar: str = pvgis_data_tar_file
        self._uk_pvgis_data_tar: str = join(os.path.dirname(pvgis_data_tar_file), PVGIS_DATA_UK_TAR)
        self._dest_grass_env: Dict = dest_grass_env

        try:
            # Prevent setup running more than once concurrently
            # todo This lock won't work with multiple processes, is this an issue?
            self.thread_lock.acquire()
            if not self._is_setup_done(grass_dbase_dir):
                self._setup_grass_env()
                self._tmp_raster_dir = os.path.realpath(os.path.join(grass_dbase_dir,
                                                                     "../solar_pv", "tmp_raster_dir"))
                shutil.rmtree(self._tmp_raster_dir, ignore_errors=True)
                os.makedirs(self._tmp_raster_dir)

                self._init_grass_db(grass_dbase_dir, 27700)
                if self._is_pvgis_data_uk():
                    # Setup from intermediate UK data if available
                    self._init_pvmaps_data_from_uk_tar()
                else:
                    # Otherwise convert worldwide data for UK
                    self._init_grass_db(grass_dbase_dir, 4326)
                    self._init_pvmaps_data_from_world_tar()
                    self._transfer_rasters()
                    if not self._keep_temp_data:
                        self._delete_grass_db(grass_dbase_dir, 4326)
                self._set_setup_done(grass_dbase_dir)
        finally:
            self.thread_lock.release()
    @staticmethod
    def _set_setup_done(dir: str):
        fname = join(dir, PVMAPS_DONE_FNAME)
        open(fname, 'w').close()

    @staticmethod
    def _is_setup_done(dir: str):
        fname = join(dir, PVMAPS_DONE_FNAME)
        return os.path.exists(fname)

    def _is_pvgis_data_uk(self):
        return os.path.exists(self._uk_pvgis_data_tar)

    def _init_pvmaps_data_from_world_tar(self):
        location = join(self._g_dbase, self._g_location)
        permanent_mapset = join(self._g_dbase, self._g_location, self.PERMANENT_MAPSET)

        if not os.path.isfile(self._pvgis_data_tar):
            raise FileNotFoundError(f"File {self._pvgis_data_tar} not found")
        if not os.path.exists(permanent_mapset):
            raise FileNotFoundError(f"Grass DB path ({location}) exists but {self.PERMANENT_MAPSET} is missing!")

        chk_fname: str = join(location, PVMAPS_WORLD_DONE_FNAME)
        if not os.path.exists(chk_fname):
            logging.info(f"_init_pvmaps_data_from_world_tar (location = {location})")

            # Extract the pvgis data into it's permanent mapset
            with tarfile.open(self._pvgis_data_tar, "r") as tar:
                tar.extractall(path=permanent_mapset)
            open(chk_fname, 'w').close()

    def _get_raster_list(self, grass_env: Dict):
        rasters: str = self._run_cmd("g.list type=raster", raw_output_text=True, grass_env=grass_env)
        raster_list: List[str] = rasters.split("\n")
        raster_list = [raster.strip() for raster in raster_list if raster.strip()]
        return raster_list

    def _get_untransferred_rasters(self):
        rasters_dest: List[str] = self._get_raster_list(self._dest_grass_env)
        rasters_source: List[str] = self._get_raster_list(self._grass_env)
        rasters_to_transfer: Set[str] = set(rasters_source) - set(rasters_dest)
        return rasters_to_transfer

    def _transfer_rasters(self):
        rasters_to_transfer: Set[str] = self._get_untransferred_rasters()

        if rasters_to_transfer:
            logging.info("_transfer_rasters")
            # Set export bounds for all rasters to be the same - EPSG27700_bounds + a buffer,
            # also set the raster size to be the same as the max rows/columns of any of the rasters in the region
            self._add_27700_bounds_to_grass_db(self._grass_env, self.EPSG27700_bounds_4326_json)
            max_rows, max_cols = self._get_raster_size(rasters_to_transfer)
            self._run_cmd(f"g.region vector={BOUNDS_OF_27700} rows={max_rows} cols={max_cols}", grass_env=self._grass_env)

            args = [(raster, self._tmp_raster_dir) for raster in rasters_to_transfer]
            self._run_cmd_via_method_p(self._transfer_raster, args)
            self._create_pvgis_data_uk()

            # Check all done
            missed: Set[str] = self._get_untransferred_rasters()
            if len(missed) > 0:
                raise Exception(f"Not all rasters transferred from 4326 mapset to 27700 mapset: {missed}")

            if not self._keep_temp_data:
                shutil.rmtree(self._tmp_raster_dir)

    def _transfer_raster(self, raster: str, tmp_dir: str):
        filename_4326: str = join(tmp_dir, f"{raster}.4326.tif")
        filename_27700: str = join(tmp_dir, f"{raster}.27700.tif")

        # Export raster section within current region
        self._export_raster_raw(raster, filename_4326, type=None, grass_env=self._grass_env)

        gdal_helpers.reproject_within_bounds(filename_4326, filename_27700, src_srs="EPSG:4326", dst_srs=_7_PARAM_SHIFT,
                                             bounds=self.EPSG27700_bounds_27700, width=500, height=500)
        self._import_raster_raw(filename_27700, raster, grass_env=self._dest_grass_env)
        if not self._keep_temp_data:
            os.remove(filename_4326)

    def _add_27700_bounds_to_grass_db(self, grass_env: Dict, espg27700_bounds: bytes):
        bounds_fname: str = ""
        try:
            bounds_file = tempfile.NamedTemporaryFile(delete=False)
            bounds_fname = bounds_file.name
            bounds_file.write(espg27700_bounds)
            bounds_file.close()
            self._run_cmd(f"v.in.ogr --overwrite input={bounds_fname} output={BOUNDS_OF_27700}", grass_env=grass_env)
        finally:
            if bounds_fname:
                os.remove(bounds_fname)

    def _get_raster_size(self, rasters_to_transfer: Set[str]):
        logging.info("_get_raster_size")
        max_rows: int = 0
        max_cols: int = 0
        for raster in rasters_to_transfer:
            region_info = self._run_cmd(f"g.region vector={BOUNDS_OF_27700} raster={raster} -p", raw_output_text=True)
            rows: int = 0
            cols: int = 0
            for info in region_info.split("\n"):
                if info.startswith("rows:       "):
                    rows = int(info[12:])
                elif info.startswith("cols:       "):
                    cols = int(info[12:])
                    break
            if cols > max_cols and rows > max_rows:
                max_cols = cols
                max_rows = rows
        logging.info(f"Size of largest raster: rows = {max_rows}, cols = {max_cols} within 27700 bounds")
        return max_rows, max_cols

    def _init_pvmaps_data_from_uk_tar(self):
        logging.info(f"_init_pvmaps_data_from_uk_tar (location = {self._uk_pvgis_data_tar})")
        with tarfile.open(self._uk_pvgis_data_tar, "r") as tar:
            for member in tar.getmembers():
                tar.extract(member, path=self._tmp_raster_dir)
                fname: str = join(self._tmp_raster_dir, member.name)
                raster: str = member.name[:-len(".27700.tif")]
                self._import_raster_raw(fname, raster, grass_env=self._dest_grass_env)
                os.remove(fname)

    def _create_pvgis_data_uk(self):
        files: List[str] = os.listdir(self._tmp_raster_dir)
        with tarfile.open(self._uk_pvgis_data_tar, "w") as tar:
            for file in files:
                if file.endswith(".27700.tif"):
                    tar.add(join(self._tmp_raster_dir, file), arcname=file)
