"""
Run tippecanoe and gather results
"""
import logging
import os
import subprocess
import shlex
from os.path import join


def cmd_tippecanoe(gpkg_filename: str, layer_name: str) -> str:
    base_dirname: str = os.path.dirname(gpkg_filename)
    gpkg_bname_stem, _ = os.path.splitext(os.path.basename(gpkg_filename))

    os.makedirs(join(base_dirname, "intermediate"), exist_ok=True)
    geojson_fname = join(base_dirname, "intermediate", f"{gpkg_bname_stem}.geojson")

    sqlite_fname = join(base_dirname, f"{gpkg_bname_stem}.sqlite")

    _gpkg_to_geojson(gpkg_filename, layer_name, geojson_fname)
    _geojson_to_tiles(geojson_fname, layer_name, sqlite_fname)

    return sqlite_fname


def _gpkg_to_geojson(gpkg_filename: str, layer_name: str, geojson_filename: str):
    logging.info(f"Generating {geojson_filename} from {gpkg_filename}")
    cmdline: str = f"ogr2ogr -f GeoJSON -nln localauthority {geojson_filename} {gpkg_filename} {layer_name}"

    p = subprocess.run(shlex.split(cmdline), capture_output=True, text=True)

    if p.returncode != 0:
        raise Exception(f"Error running ogr2ogr:\nreturncode = {p.returncode}\n"
                        f"stdout = {p.stdout}\nstderr = {p.stderr}")


def _geojson_to_tiles(geojson_filename: str, layer_name: str, sqlite_fname: str):
    logging.info(f"Generating {sqlite_fname} from {geojson_filename}")
    cmdline: str = (
        'tippecanoe '
        f'-o {sqlite_fname} '
        f'{geojson_filename} '
        f'--layer="{layer_name}" '
        f'--name="{layer_name}" '
        '--force '
        '--maximum-zoom=3 '   # See https://github.com/mapbox/tippecanoe#zoom-levels e.g. 18 = 4cm, 7 = 80m
        '--read-parallel '
        '--no-polygon-splitting '
        '--detect-shared-borders '
        '--no-tile-size-limit '
        '--simplification=10'
    )

    p = subprocess.Popen(shlex.split(cmdline), shell=False,
                         stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

    # Log tippecanoe o/p as it's running (as it takes a while to run!)
    for line in p.stdout:
        logging.info(f"tippecanoe: {line.strip()}")

    rtn_code = p.poll()
    if rtn_code != 0:
        raise RuntimeError(f"Error running tippecanoe: returncode = {rtn_code}")