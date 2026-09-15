#!/usr/bin/env python3
# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
"""
Phase 0 of the GRASS/PVMAPS removal (docs/pv-grass-removal-plan.md).

Runs the current GRASS/PVMAPS model on the committed real-area inputs and *freezes* its
output rasters (yearly kWh, monthly Wh, horizon profile) into a committed golden directory,
so the native-Python port can be validated against them to a tight tolerance AFTER GRASS is
gone. This must be run while GRASS + the PVMAPS addon modules (r.horizonmask, r.pv) still
build, since testdata/pvmaps/outputs is gitignored and there is otherwise no frozen oracle.

Run under nix-shell (needs GRASS + gdal + the model deps):

    nix-shell --run "python bin/capture_pvmaps_goldens.py --area all"

Env: PVMaps needs a GRASS dbase (default: testdata/pvmaps/grass_dbase) and the pvgis met
data. Placing pvgis_data_uk.tar next to pvgis_data.tar lets setup skip the slow world->UK
conversion.
"""
import argparse
import logging
import os
import shutil
import sys
from os.path import basename, join

from solar_pv.paths import RESOURCES_DIR
from solar_pv.pvgis import pvmaps
from solar_pv.pvgis.pvmaps import PVMaps, CSI
from solar_pv.pv.golden import fixtures
from solar_pv.pv.golden.fixtures import AREAS, Area, area_by_name
from solar_pv.util import get_cpu_count

TEST_DATA_DIR = fixtures.PVMAPS_TEST_DATA
DEFAULT_GRASS_DBASE = join(TEST_DATA_DIR, "grass_dbase")
DEFAULT_PVGIS_TAR = join(TEST_DATA_DIR, "pvgis_data_tar", "pvgis_data.tar")


def _capture_area(area: Area, grass_dbase: str, pvgis_tar: str, work_dir: str):
    if not area.has_inputs():
        raise FileNotFoundError(
            f"area {area.name!r} is missing committed inputs under {area.input_dir}")

    out_dir = join(work_dir, area.name)
    os.makedirs(out_dir, exist_ok=True)

    logging.info(f"[{area.name}] running PVMAPS (this takes a few minutes)...")
    pvm = PVMaps(
        grass_dbase_dir=os.path.realpath(grass_dbase),
        input_dir=area.input_dir,
        output_dir=out_dir,
        pvgis_data_tar_file=os.path.realpath(pvgis_tar),
        pv_model_coeff_file_dir=RESOURCES_DIR,
        keep_temp_mapset=False,
        num_processes=max(1, int(get_cpu_count() * 0.75)),
        output_direct_diffuse=False,
        horizon_step_degrees=fixtures.HORIZON_STEP_DEGREES,
        horizon_search_distance=fixtures.HORIZON_SEARCH_DISTANCE,
        flat_roof_degrees=fixtures.FLAT_ROOF_DEGREES,
        flat_roof_degrees_threshold=fixtures.FLAT_ROOF_DEGREES_THRESHOLD,
        panel_type=CSI,
    )
    pvm.create_pvmap(
        elevation_filename=area.elevation,
        mask_filename=area.mask,
        aspect_override_raster=area.aspect_override,
    )

    golden_dir = area.golden_dir
    os.makedirs(golden_dir, exist_ok=True)

    # Freeze the per-pixel quantities the port must reproduce (the aggregation seam):
    frozen = []

    def _freeze(src: str, dst_name: str):
        dst = join(golden_dir, dst_name)
        shutil.copyfile(src, dst)
        frozen.append(dst_name)

    _freeze(pvm.yearly_kwh_raster, "kwh_year.tif")
    if len(pvm.monthly_wh_rasters) != 12:
        raise ValueError(f"expected 12 monthly rasters, got {len(pvm.monthly_wh_rasters)}")
    for i, r in enumerate(pvm.monthly_wh_rasters):
        _freeze(r, f"month_{str(i + 1).zfill(2)}_wh.tif")
    # pvm.horizons are CCW from East (same order pvgis.py loads them):
    for i, r in enumerate(pvm.horizons):
        _freeze(r, f"horizon_{str(i).zfill(2)}.tif")

    logging.info(f"[{area.name}] froze {len(frozen)} golden rasters -> {golden_dir}")
    return golden_dir


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--area", default="all",
                        help="area name (see fixtures.AREAS) or 'all'")
    parser.add_argument("--grass-dbase", default=DEFAULT_GRASS_DBASE)
    parser.add_argument("--pvgis-tar", default=DEFAULT_PVGIS_TAR)
    parser.add_argument("--work-dir", default=join(TEST_DATA_DIR, "goldens", "_work"),
                        help="scratch dir for raw PVMAPS output before freezing")
    args = parser.parse_args()

    logging.basicConfig(format="%(asctime)s: %(levelname)s: %(message)s",
                        level=logging.INFO, datefmt="%d/%m/%Y %H:%M:%S")

    areas = AREAS if args.area == "all" else [area_by_name(args.area)]
    os.makedirs(args.work_dir, exist_ok=True)

    failures = []
    for area in areas:
        try:
            _capture_area(area, args.grass_dbase, args.pvgis_tar, args.work_dir)
        except Exception as e:
            logging.exception(f"[{area.name}] capture failed")
            failures.append((area.name, str(e)))

    if failures:
        logging.error(f"{len(failures)} area(s) failed: {failures}")
        sys.exit(1)
    logging.info("done")


if __name__ == "__main__":
    main()
