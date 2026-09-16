#!/usr/bin/env python3
# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
"""
Dev harness for Phase 2: feed the r.pv port the exact GRASS inputs captured by
capture_rpv_reference.py and compare the raw daily hpv against the golden.
    nix-shell --run "python bin/check_rpv_port.py --area thurso --day 162"
"""
import argparse
import math
from os.path import join

import numpy as np
from osgeo import gdal, osr

from solar_pv.pv import irradiation as ir
from solar_pv.pv.golden import compare
from solar_pv.pv.golden.fixtures import area_by_name, HORIZON_STEP_DEGREES

gdal.UseExceptions()
osr.UseExceptions()

DAY_TO_MONTH = {17: 1, 46: 2, 75: 3, 103: 4, 135: 5, 162: 6,
                198: 7, 228: 8, 259: 9, 289: 10, 319: 11, 345: 12}
CSI_COEFFS = [1.000436, -0.017237, -0.040465, -0.004702, 0.000149, 0.000170, 0.000005, 0.035]


def _solar_decl(day):
    d1 = 2.0 * math.pi * day / 365.25
    return math.asin(0.3978 * math.sin(d1 - 1.4 + 0.0355 * math.sin(d1 - 0.0489)))


def _latlon(gt, shape):
    rows, cols = shape
    cc, rr = np.meshgrid(np.arange(cols), np.arange(rows))
    x = gt[0] + (cc + 0.5) * gt[1]
    y = gt[3] + (rr + 0.5) * gt[5]
    src = osr.SpatialReference(); src.ImportFromEPSG(27700)
    dst = osr.SpatialReference(); dst.ImportFromEPSG(4326)
    src.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    dst.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    ct = osr.CoordinateTransformation(src, dst)
    pts = ct.TransformPoints(np.column_stack([x.ravel(), y.ravel()]).tolist())
    pts = np.array(pts)
    lon = np.radians(pts[:, 0]).reshape(shape)
    lat = np.radians(pts[:, 1]).reshape(shape)
    return lat, lon


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--area", default="thurso")
    p.add_argument("--day", type=int, default=162)
    args = p.parse_args()
    area = area_by_name(args.area)
    rpv = join(area.golden_dir, "rpv")
    mm = f"{DAY_TO_MONTH[args.day]:02d}"

    def rd(path):
        return compare._read_band(path)

    slope, gt, shape = compare.read_geotiff(join(rpv, "slope_adjusted.tif"))
    aspect = rd(join(rpv, "aspect_adjusted.tif"))
    elev = rd(join(rpv, "elevation.tif"))
    linke = rd(join(rpv, f"linke_{mm}.tif"))
    cbh = rd(join(rpv, f"kcb_{mm}.tif"))
    cdh = rd(join(rpv, f"kcd_{mm}.tif"))
    temps = np.stack([rd(join(rpv, f"t2m_{mm}_{hh:02d}.tif")) for hh in range(0, 24, 3)], axis=-1)
    hpv_golden = rd(join(rpv, f"hpv_day{args.day}.tif"))

    n_dir = int(round(360 / HORIZON_STEP_DEGREES))
    horizon = np.stack([rd(join(area.golden_dir, f"horizon_{d:02d}.tif"))
                        for d in range(n_dir)], axis=-1)

    lat, lon = _latlon(gt, shape)

    # evaluate only where the golden hpv is defined (mask/building pixels with valid inputs):
    valid = np.isfinite(hpv_golden) & np.isfinite(slope) & np.isfinite(aspect) & \
        np.isfinite(elev) & np.isfinite(linke) & np.isfinite(cbh) & np.isfinite(cdh) & \
        np.all(np.isfinite(temps), axis=-1) & np.all(np.isfinite(horizon), axis=-1)
    idx = np.nonzero(valid)

    declination = -_solar_decl(args.day)
    port = ir.compute_daily_pv(
        slope[idx], aspect[idx], elev[idx], lat[idx], lon[idx],
        horizon[idx], HORIZON_STEP_DEGREES,
        linke[idx], cbh[idx], cdh[idx], temps[idx], 0.2,
        args.day, declination, CSI_COEFFS)

    gold = hpv_golden[idx]
    with np.errstate(divide="ignore", invalid="ignore"):
        pc = np.abs(100.0 * (port - gold) / gold)
    pc = pc[np.isfinite(pc)]
    print(f"== {area.name} day {args.day} (month {mm}) n={len(gold)} ==")
    print(f"  golden hpv: min={gold.min():.1f} mean={gold.mean():.1f} max={gold.max():.1f}")
    print(f"  port   hpv: min={port.min():.1f} mean={port.mean():.1f} max={port.max():.1f}")
    print(f"  abs %% diff: mean={pc.mean():.3f} p95={np.percentile(pc,95):.3f} max={pc.max():.3f}")
    print(f"  within 2%%: {100*np.mean(pc<2):.1f}%%   within 5%%: {100*np.mean(pc<5):.1f}%%")


if __name__ == "__main__":
    main()
