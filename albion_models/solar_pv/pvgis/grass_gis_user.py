"""
Abstract base class of classes that use Grass GIS
"""
import logging
import os
import re
import shlex
import shutil
import subprocess
import tempfile
import time
from abc import ABC
from concurrent.futures import ThreadPoolExecutor, Future
from os.path import join
from subprocess import Popen
from typing import List, Optional, Dict, Tuple


class GrassGISUser(ABC):
    PERMANENT_MAPSET: str = "PERMANENT"
    GRASSDATA_DIR: str = "grassdata"

    def __init__(self, crs: int, grass_dbase_dir: str, job_id: int, keep_temp_mapset: bool):
        self._g_location: str = f"{self.GRASSDATA_DIR}_{crs}"

        if os.path.exists(grass_dbase_dir):
            if not os.path.isdir(grass_dbase_dir):
                raise ValueError(f"Grass dbase directory ({grass_dbase_dir}) must be a directory")
        else:
            os.makedirs(grass_dbase_dir)
        self._g_dbase: str = grass_dbase_dir

        if job_id:
            self.uid = f"job_{job_id}_{crs}"
        else:
            self.uid = f"{os.getpid()}_{int(time.time())}_{crs}"

        self._gisrc_filename = join(tempfile.gettempdir(), f"pvmaps.{self.uid}.rc")
        self._executor = None
        self._keep_temp_data = keep_temp_mapset

    def __del__(self):
        if self._gisrc_filename is not None and os.path.exists(self._gisrc_filename):
            os.remove(self._gisrc_filename)

    def _set_executor(self, executor: ThreadPoolExecutor):
        self._executor = executor

    def _update_mapset(self, mapset: str):
        with open(self._gisrc_filename, "w") as rcfile:
            rcfile.write(f"GISDBASE: {self._g_dbase}\n")
            rcfile.write(f"LOCATION_NAME: {self._g_location}\n")
            rcfile.write(f"MAPSET: {mapset}\n")
            rcfile.write("\n")

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

    def _setup_grass_env(self):
        """See https://grasswiki.osgeo.org/wiki/Working_with_GRASS_without_starting_it_explicitly
        """
        # write initial gisrc file
        self._update_mapset(self.PERMANENT_MAPSET)

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

    def _init_grass_db(self, grass_dbase_dir: str, crs: int):
        g_location: str = f"{self.GRASSDATA_DIR}_{crs}"
        location = join(grass_dbase_dir, g_location)
        permanent_mapset = join(grass_dbase_dir, g_location, self.PERMANENT_MAPSET)

        if not os.path.exists(location):
            logging.info(f"_init_grass_db (location = {location})")

            # Create a new grass gis database
            self._run_cmd(f"grass -c EPSG:{crs} -e {location}")

        elif not os.path.exists(permanent_mapset):
            raise FileNotFoundError(f"Grass DB path ({location}) exists but {self.PERMANENT_MAPSET} is missing!")

    def _delete_grass_db(self, grass_dbase_dir: str, crs: int):
        g_location: str = f"{self.GRASSDATA_DIR}_{crs}"
        location = join(grass_dbase_dir, g_location)
        shutil.rmtree(location, ignore_errors=True)

    def _run_cmd(self, cmd_line: str, exp_returncode: int = 0,
                 grass_env: Optional[Dict] = None, raw_output_text: bool = False) -> str:
        """ Run command in grass env, check outputs.
        :param cmd_line: Command to run
        :param exp_returncode: Raises exception if return is not this value
        :return: process output text, if raw_output_text = False, with control codes removed and prefixed with the pid
        """
        log_msg: str = ""
        raw_line_s: str

        if not grass_env:
            grass_env = self._grass_env

        arguments: List[str] = shlex.split(cmd_line)

        process = Popen(arguments, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=grass_env, text=True)
        with process.stdout:
            raw_line_s = process.stdout.read()
            line_s = raw_line_s.replace("\n", " ").strip()
            line_s = re.sub(r"[\x00-\x1f\x7f-\x9f]", "", line_s)
            if len(line_s) > 0:
                log_msg = f"{process.pid}: {line_s}"
                logging.debug(log_msg)
        rtn_code: int = process.wait()

        if rtn_code != exp_returncode:
            logging.error(log_msg)
            raise Exception(f"Command {cmd_line} returned error code {rtn_code}")
        if raw_output_text:
            return raw_line_s
        return log_msg


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

    @staticmethod
    def _run_cmd_via_method(method, args_list: List[Tuple]) -> None:
        for args in args_list:
            method(*args)

    def _import_raster_raw(self, filename: str, name: str,
                 grass_env: Optional[Dict] = None) -> None:
        self._run_cmd(f"r.import input={filename} output={name}", grass_env=grass_env)

    def _export_raster_raw(self, in_raster: str, out_raster_file: str, type: Optional[str] = "Float64",
                 grass_env: Optional[Dict] = None):
        type_arg = f"type={type}" if type else ""
        self._run_cmd(f"r.out.gdal --overwrite input={in_raster} output='{out_raster_file}' "
                      f"format=GTiff {type_arg} -c createopt=\"COMPRESS=PACKBITS,TILED=YES\"", grass_env=grass_env)