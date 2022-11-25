import logging
import os
import tarfile
from os.path import join
from tempfile import NamedTemporaryFile
from typing import Dict, List, Set

from albion_models.transformations import _7_PARAM_SHIFT
from albion_models.solar_pv.pvgis.grass_gis_user import GrassGISUser

BOUNDS_OF_27700: str = "bounds_of_27700"
PVMAPS_DATA_EXTRACTED_FNAME: str = "pvmaps.done"

class PVMapsSetup(GrassGISUser):
    # From https://epsg.io/27700
    EPSG27700_bounds_4326: bytes = \
        b'{"type":"Polygon",' \
        b'"crs":{"type":"name","properties":{"name":"EPSG:4326"}},' \
        b'"coordinates":[[[-9.0,49.75],[-9.0,61.01],[2.01,61.01],[2.01,49.75,],[-9.0,49.75]]]}'
    EPSG27700_bounds_27700: bytes = \
        b'{"type":"Polygon",' \
        b'"crs":{"type":"name","properties":{"name":"EPSG:27700"}},' \
        b'"coordinates":[[[-103976,-16703],[-103976,1199851],[652897,1199851],[652897,-16703],[-103976,-16703]]]}'

    CROPPED_SUFFIX: str = "_c"

    def __init__(self,
                 grass_dbase_dir: str,
                 job_id: int,
                 pvgis_data_tar_file: str,
                 dest_grass_env: Dict):
        super().__init__(4326, grass_dbase_dir, job_id)
        self._pvgis_data_tar: str = pvgis_data_tar_file
        self._dest_grass_env: Dict = dest_grass_env

        self._init_pvmaps_data_from_tar()

        self._transfer_rasters()

    def _init_pvmaps_data_from_tar(self):
        location = join(self._g_dbase, self._g_location)
        permanent_mapset = join(self._g_dbase, self._g_location, self.PERMANENT_MAPSET)

        pvgis_data_extracted_fname = join(permanent_mapset, PVMAPS_DATA_EXTRACTED_FNAME)

        if not os.path.exists(pvgis_data_extracted_fname):
            if not os.path.isfile(self._pvgis_data_tar):
                raise FileNotFoundError(f"File {self._pvgis_data_tar} not found")

            logging.info(f"_init_pvmaps_data (location = {location})")

            # Extract the pvgis data into it's permanent mapset
            with tarfile.open(self._pvgis_data_tar, "r") as tar:
                tar.extractall(path=permanent_mapset)

            open(pvgis_data_extracted_fname, 'w').close()

        elif not os.path.exists(permanent_mapset):
            raise FileNotFoundError(f"Grass DB path ({location}) exists but {self.PERMANENT_MAPSET} is missing!")

    def _get_raster_list(self, grass_env: Dict):
        rasters: str = self._run_cmd("g.list type=raster", raw_output_text=True, grass_env=grass_env)
        raster_list: List[str] = rasters.split("\n")
        return [raster for raster in raster_list if not raster.endswith(self.CROPPED_SUFFIX)]

    def _transfer_rasters(self):
        rasters_dest: List[str] = self._get_raster_list(self._dest_grass_env)
        rasters_source: List[str] = self._get_raster_list(self._grass_env)
        rasters_to_transfer: Set[str] = set(rasters_source) - set(rasters_dest)

        if rasters_to_transfer:
            self._add_27700_bounds_to_grass_db(self._grass_env, self.EPSG27700_bounds_4326)
            self._add_27700_bounds_to_grass_db(self._dest_grass_env, self.EPSG27700_bounds_27700)

            max_rows, max_cols = self._get_raster_size(rasters_to_transfer)
            self._create_cropped_rasters(rasters_to_transfer, max_rows, max_cols)
            self._reproject_cropped_rasters(rasters_to_transfer, max_rows, max_cols)

    def _add_27700_bounds_to_grass_db(self, grass_env: Dict, espg27700_bounds: bytes):
        bounds_fname: str = ""
        try:
            bounds_file = NamedTemporaryFile(delete=False)
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
        for ix, raster_to_transfer in enumerate(rasters_to_transfer):
            region_info = self._run_cmd(f"g.region vector={BOUNDS_OF_27700} raster={raster_to_transfer} -p", raw_output_text=True)
            rows: int = 0
            cols: int = 0
            for info in region_info.split("\n"):
                if info.startswith("rows:       "):
                    rows = int(info[12:])
                elif info.startswith("cols:       "):
                    cols = int(info[12:])
                    break
            max_cols = max(max_cols, cols)
            max_rows = max(max_rows, rows)
            if ix % 10 == 0:
                logging.info(f"Done {ix} of {len(rasters_to_transfer)}")
        logging.info(f"Size of largest raster: rows = {max_rows}, cols = {max_cols} within 27700 bounds")
        return max_rows, max_cols

    def _create_cropped_rasters(self, rasters_to_transfer: Set[str], max_rows: int, max_cols: int):
        logging.info("_create_cropped_rasters")
        self._run_cmd(f"g.region vector={BOUNDS_OF_27700} rows={max_rows} cols={max_cols}")
        for ix, raster_to_transfer in enumerate(rasters_to_transfer):
            self._run_cmd(f'r.mapcalc --overwrite "{raster_to_transfer}{self.CROPPED_SUFFIX} = {raster_to_transfer}"')
            self._run_cmd(f"r.colors map={raster_to_transfer}{self.CROPPED_SUFFIX} rast={raster_to_transfer}")
            if ix % 10 == 0:
                logging.info(f"Done {ix} of {len(rasters_to_transfer)}")

    def _reproject_cropped_rasters(self, rasters_to_transfer: Set[str], max_rows: int, max_cols: int):
        logging.info("_reproject_cropped_rasters")
        self._run_cmd(f"g.region vector={BOUNDS_OF_27700} rows={max_rows} cols={max_cols}",
                      grass_env=self._dest_grass_env)
        for ix, raster_to_transfer in enumerate(rasters_to_transfer):
            self._run_cmd(f"r.proj "
                          f"location={self.GRASSDATA_DIR}_4326 mapset={self.PERMANENT_MAPSET} "
                          f"input={raster_to_transfer}{self.CROPPED_SUFFIX} "
                          f"output={raster_to_transfer} "
                          f"pipeline={_7_PARAM_SHIFT}",
                          grass_env=self._dest_grass_env)
            if ix % 10 == 0:
                logging.info(f"Done {ix} of {len(rasters_to_transfer)}")