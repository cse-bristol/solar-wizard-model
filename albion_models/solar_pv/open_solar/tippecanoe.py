"""
Run tippecanoe and gather results
"""
import logging
import os
import subprocess
import shlex
from os.path import join
import resource as res

def set_num_file_handles(soft: int):
    soft, hard = res.getrlimit(res.RLIMIT_NOFILE)
    logging.info(f"Initial file handle limits are: {soft} {hard}")
    res.setrlimit(res.RLIMIT_NOFILE,(soft,hard))
    soft, hard = res.getrlimit(res.RLIMIT_NOFILE)
    logging.info(f"New file handle limits are: {soft} {hard}")


def cmd_tippecanoe(gpkg_filename: str, layer_name: str, fields: list) -> str:
    base_dirname: str = os.path.dirname(gpkg_filename)
    gpkg_bname_stem, _ = os.path.splitext(os.path.basename(gpkg_filename))

    os.makedirs(join(base_dirname, "intermediate"), exist_ok=True)
    geojson_fname = join(base_dirname, "intermediate", f"{gpkg_bname_stem}.geojson")

    sqlite_fname = join(base_dirname, f"{gpkg_bname_stem}.sqlite")

    _gpkg_to_geojson(gpkg_filename, layer_name, geojson_fname, fields)
    _geojson_to_tiles(geojson_fname, layer_name, sqlite_fname)

    return sqlite_fname


def _gpkg_to_geojson(gpkg_filename: str, layer_name: str, geojson_filename: str, fields: list):
    logging.info(f"Generating {geojson_filename} from {gpkg_filename}")
    cmdline: str = f"ogr2ogr -f GeoJSON -nln localauthority {geojson_filename} {gpkg_filename}" \
                   f" -sql 'SELECT {','.join(fields)} FROM {layer_name}'"

    p = subprocess.run(shlex.split(cmdline), capture_output=True, text=True)

    if p.returncode != 0:
        raise Exception(f"Error running ogr2ogr:\nreturncode = {p.returncode}\n"
                        f"stdout = {p.stdout}\nstderr = {p.stderr}")


def _geojson_to_tiles(geojson_filename: str, layer_name: str, sqlite_fname: str):
    logging.info(f"Generating {sqlite_fname} from {geojson_filename}")

    # Set some file handle and thread limits that allow running on bats. See "init_cpus()" in
    # https://github.com/mapbox/tippecanoe/blob/18e53cd7fb9ae6be8b89d817d69d3ce06f30eb9d/main.cpp#L217-L221
    set_num_file_handles(5000)
    tippecanoe_env = {**os.environ, "TIPPECANOE_MAX_THREADS": "64"}

    cmdline: str = (
        'tippecanoe '
        f'-o {sqlite_fname} '
        f'{geojson_filename} '
        f'--layer="{layer_name}" '
        f'--name="{layer_name}" '
        '--force '
        '--minimum-zoom=8 '   # These are the zoom levels in the Leaflet map
        '--maximum-zoom=16 '
        '--read-parallel '
        '--no-polygon-splitting '
        '--detect-shared-borders '
        '--no-tile-size-limit '
        '--simplification=10'
    )

    p = subprocess.Popen(shlex.split(cmdline), shell=False,
                         stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=tippecanoe_env)

    # Log tippecanoe o/p as it's running (as it takes a while to run!)
    for line in p.stdout:
        logging.info(f"tippecanoe: {line.strip()}")

    rtn_code = p.poll()
    if rtn_code != 0:
        raise RuntimeError(f"Error running tippecanoe: returncode = {rtn_code}")
