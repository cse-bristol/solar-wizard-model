#!/usr/bin/env python3
# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
"""
Phase 2 validation support: run PVMAPS on a test area keeping the temp mapset, then export
the exact rasters r.pv consumed (GRASS-adjusted slope/aspect, and the per-pixel Linke /
beam-coeff / diffuse-coeff / 8 temperature met rasters cropped to the region) plus the raw
per-day hpv output. This lets the Python r.pv port be validated on *identical* inputs,
isolating the port's maths from slope/aspect/horizon/met derivation.

Freezes into testdata/pvmaps/goldens/<area>/rpv/. Run while GRASS still builds:

    nix-shell --run "python bin/capture_rpv_reference.py --area thurso"
"""
import argparse
import logging
import os
from os.path import join

from solar_pv.paths import RESOURCES_DIR
from solar_pv.pvgis.pvmaps import PVMaps, CSI, SLOPE_ADJUSTED, ASPECT_GRASS_ADJUSTED
from solar_pv.pv.golden import fixtures
from solar_pv.pv.golden.fixtures import area_by_name
from solar_pv.util import get_cpu_count

TEST_DATA_DIR = fixtures.PVMAPS_TEST_DATA
DEFAULT_GRASS_DBASE = join(TEST_DATA_DIR, "grass_dbase")
DEFAULT_PVGIS_TAR = join(TEST_DATA_DIR, "pvgis_data_tar", "pvgis_data.tar")

# (index, day, month, days) representative days, matching _monthly_pv_time_steps():
MONTHLY_STEPS = [
    (0, 17, 1, 31), (1, 46, 2, 28), (2, 75, 3, 31), (3, 103, 4, 30),
    (4, 135, 5, 31), (5, 162, 6, 30), (6, 198, 7, 31), (7, 228, 8, 31),
    (8, 259, 9, 30), (9, 289, 10, 31), (10, 319, 11, 30), (11, 345, 12, 31),
]


def _export(pvm, raster, out_path):
    """r.out.gdal a raster from the current (temp) mapset or PERMANENT (met data)."""
    pvm._run_cmd(
        f"r.out.gdal --overwrite -c input={raster} output='{out_path}' format=GTiff "
        f"type=Float64 createopt=\"COMPRESS=DEFLATE,PREDICTOR=3,TILED=YES\"")


def _maybe_export(pvm, raster, out_path):
    """Export a raster that may not exist for this area (wind/spectral coverage gaps)."""
    try:
        _export(pvm, raster, out_path)
    except Exception:
        logging.info(f"  ({raster} unavailable here - port will default it to 1.0)")


def capture(area, grass_dbase, pvgis_tar):
    out_dir = join(area.golden_dir, "rpv")
    os.makedirs(out_dir, exist_ok=True)
    work = join(TEST_DATA_DIR, "goldens", "_rpv_work", area.name)
    os.makedirs(work, exist_ok=True)

    logging.info(f"[{area.name}] running PVMAPS (keep temp mapset)...")
    pvm = PVMaps(
        grass_dbase_dir=os.path.realpath(grass_dbase),
        input_dir=area.input_dir,
        output_dir=work,
        pvgis_data_tar_file=os.path.realpath(pvgis_tar),
        pv_model_coeff_file_dir=RESOURCES_DIR,
        keep_temp_mapset=True,
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

    # r.pv inputs it derived:
    _export(pvm, SLOPE_ADJUSTED, join(out_dir, "slope_adjusted.tif"))
    _export(pvm, ASPECT_GRASS_ADJUSTED, join(out_dir, "aspect_adjusted.tif"))
    _export(pvm, "elevation", join(out_dir, "elevation.tif"))
    # the exact horizon rasters r.pv consumed (horizon090_<angle>), CCW from East:
    for d in range(int(round(360 / fixtures.HORIZON_STEP_DEGREES))):
        angle = d * int(fixtures.HORIZON_STEP_DEGREES)
        _export(pvm, f"horizon090_{angle:03d}", join(out_dir, f"horizon_{d:02d}.tif"))

    # per-representative-month met rasters + raw hpv (pre wind/spectral):
    for _, day, month, _ in MONTHLY_STEPS:
        mm = f"{month:02d}"
        _export(pvm, f"tl_0m_{mm}", join(out_dir, f"linke_{mm}.tif"))
        _export(pvm, f"kcb_{mm}", join(out_dir, f"kcb_{mm}.tif"))
        _export(pvm, f"kcd_{mm}", join(out_dir, f"kcd_{mm}.tif"))
        # wind/spectral corrections (may be null in the north -> pvmaps defaults them to 1.0):
        _maybe_export(pvm, f"windeffect_{mm}", join(out_dir, f"wind_{mm}.tif"))
        _maybe_export(pvm, f"spectraleffect_cSi_{mm}", join(out_dir, f"spectral_{mm}.tif"))
        for hh in range(0, 24, 3):
            _export(pvm, f"t2m_avg_{mm}_{hh:02d}",
                    join(out_dir, f"t2m_{mm}_{hh:02d}.tif"))
        _export(pvm, f"hpv_{day}", join(out_dir, f"hpv_day{day}.tif"))
        # daily beam/diffuse/reflected irradiation on the slope (component isolation):
        _export(pvm, f"bha_{day}", join(out_dir, f"bha_day{day}.tif"))
        _export(pvm, f"dha_{day}", join(out_dir, f"dha_day{day}.tif"))
        _export(pvm, f"rha_{day}", join(out_dir, f"rha_day{day}.tif"))

    logging.info(f"[{area.name}] r.pv reference frozen -> {out_dir}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--area", default="thurso")
    p.add_argument("--grass-dbase", default=DEFAULT_GRASS_DBASE)
    p.add_argument("--pvgis-tar", default=DEFAULT_PVGIS_TAR)
    args = p.parse_args()
    logging.basicConfig(format="%(asctime)s: %(levelname)s: %(message)s",
                        level=logging.INFO, datefmt="%d/%m/%Y %H:%M:%S")
    capture(area_by_name(args.area), args.grass_dbase, args.pvgis_tar)


if __name__ == "__main__":
    main()
