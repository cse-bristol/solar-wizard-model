#!/usr/bin/env python3
# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
"""
Dev harness for Phase 1: compare the native-Python horizon port against the frozen
GRASS/PVMAPS horizon goldens. Prints, per direction, the horizon-angle error in DEGREES
(the meaningful metric for horizons — percentages explode near 0) over the golden's window.

    nix-shell --run "python bin/check_horizon_port.py --area alnwick"
    nix-shell --run "python bin/check_horizon_port.py --area all --convergence"
"""
import argparse
import math
from os.path import join

import numpy as np

from solar_pv.pv import horizon, horizon_geo
from solar_pv.pv.golden import compare
from solar_pv.pv.golden.fixtures import AREAS, area_by_name, HORIZON_STEP_DEGREES, \
    HORIZON_SEARCH_DISTANCE


def check_area(area, nominal: bool):
    elev, gt, shape = compare.read_geotiff(area.input_path(area.elevation))
    ew_res = abs(gt[1])
    ns_res = abs(gt[5])
    directions = horizon.grass_directions(HORIZON_STEP_DEGREES)

    if nominal:
        vectors = horizon.nominal_vectors(directions)
    else:
        vectors = horizon_geo.grass_marching_vectors(gt, shape, directions)

    horizons = horizon.compute_horizons(
        elev, ew_res, ns_res, vectors, HORIZON_SEARCH_DISTANCE)

    print(f"\n== {area.name} (res {ew_res:.4f}x{ns_res:.4f}, "
          f"directions={'nominal' if nominal else 'grass-geo'}) ==")
    worst = 0.0
    for d_idx, direction in enumerate(directions):
        golden, ggt, gshape = compare.read_geotiff(
            join(area.golden_dir, f"horizon_{d_idx:02d}.tif"))
        port_win = compare.crop_to_window(horizons[d_idx], gt, ggt, gshape)
        valid = np.isfinite(port_win) & np.isfinite(golden)
        if not valid.any():
            print(f"  dir {math.degrees(direction):5.0f} deg: no overlap")
            continue
        err_deg = np.degrees(np.abs(port_win[valid] - golden[valid]))
        worst = max(worst, float(err_deg.max()))
        n = int(valid.sum())
        n_gt1 = int((err_deg > 1.0).sum())
        n_gt5 = int((err_deg > 5.0).sum())
        print(f"  dir {math.degrees(direction):5.0f} deg: n={n:6d} "
              f"mean={err_deg.mean():.4f} p95={np.percentile(err_deg, 95):.4f} "
              f"max={err_deg.max():7.4f}  >1deg={n_gt1} ({100*n_gt1/n:.3f}%) "
              f">5deg={n_gt5} ({100*n_gt5/n:.3f}%)")
    print(f"  WORST max abs error: {worst:.4f} deg")
    return worst


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--area", default="all")
    p.add_argument("--nominal", action="store_true",
                   help="use nominal grid directions instead of r.horizon's geo-transform")
    args = p.parse_args()
    areas = AREAS if args.area == "all" else [area_by_name(args.area)]
    worst = 0.0
    for a in areas:
        if not a.has_goldens():
            print(f"skipping {a.name}: no goldens")
            continue
        worst = max(worst, check_area(a, args.nominal))
    print(f"\nOVERALL worst: {worst:.4f} deg")


if __name__ == "__main__":
    main()
