# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
"""
Validate the r.pv port (solar_pv.pv.irradiation) against a frozen r.pv reference: the exact
GRASS-adjusted slope/aspect, horizon, per-pixel met rasters and raw daily hpv captured by
bin/capture_rpv_reference.py. Feeding identical inputs isolates the port's maths from
slope/aspect/horizon/met derivation. See docs/r-pv-algorithm.md.
"""
import math
import os
from dataclasses import dataclass
from os.path import join

import numpy as np
from osgeo import osr

from solar_pv.pv import irradiation as ir, met_data, run_pv
from solar_pv.pv.golden import compare
from solar_pv.pv.golden.fixtures import Area, HORIZON_STEP_DEGREES
from solar_pv.paths import PROJECT_ROOT

MET_TAR = join(PROJECT_ROOT, "pvgis_data_uk.tar")

osr.UseExceptions()

# (day, month) representative days, matching _monthly_pv_time_steps():
MONTHLY_DAYS = [(17, 1), (46, 2), (75, 3), (103, 4), (135, 5), (162, 6),
                (198, 7), (228, 8), (259, 9), (289, 10), (319, 11), (345, 12)]
CSI_COEFFS = [1.000436, -0.017237, -0.040465, -0.004702, 0.000149, 0.000170, 0.000005, 0.035]


def rpv_dir(area: Area) -> str:
    return join(area.golden_dir, "rpv")


def has_rpv_reference(area: Area) -> bool:
    return os.path.exists(join(rpv_dir(area), "hpv_day162.tif"))


def solar_declination(day: int) -> float:
    """PVMAPS _calc_solar_declination; r.pv uses declination = -this."""
    d1 = 2.0 * math.pi * day / 365.25
    return math.asin(0.3978 * math.sin(d1 - 1.4 + 0.0355 * math.sin(d1 - 0.0489)))


def _grass_to_compass(aspect_grass_deg: np.ndarray) -> np.ndarray:
    """The captured aspect_adjusted rasters are GRASS CCW-from-East; the port now works in
    compass, so convert (0 stays 0 = flat, else 90-a if a<90 else 450-a)."""
    a = np.asarray(aspect_grass_deg, dtype=np.float64)
    conv = np.where(a < 90.0, 90.0 - a, 450.0 - a)
    return np.where(a == 0.0, 0.0, conv)


def _latlon(gt, shape):
    rows, cols = shape
    cc, rr = np.meshgrid(np.arange(cols), np.arange(rows))
    x = (gt[0] + (cc + 0.5) * gt[1]).ravel()
    y = (gt[3] + (rr + 0.5) * gt[5]).ravel()
    src = osr.SpatialReference(); src.ImportFromEPSG(27700)
    dst = osr.SpatialReference(); dst.ImportFromEPSG(4326)
    src.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    dst.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    pts = np.array(osr.CoordinateTransformation(src, dst).TransformPoints(
        np.column_stack([x, y]).tolist()))
    return (np.radians(pts[:, 1]).reshape(shape), np.radians(pts[:, 0]).reshape(shape))


@dataclass
class RpvStats:
    area: str
    day: int
    n: int
    mean_pc: float
    p99_pc: float
    max_pc: float
    pct_within_2: float

    def __str__(self):
        return (f"{self.area} day {self.day}: n={self.n} mean={self.mean_pc:.4f}% "
                f"p99={self.p99_pc:.4f}% max={self.max_pc:.4f}% within2%={self.pct_within_2:.2f}%")


def check_day(area: Area, day: int, month: int) -> RpvStats:
    d = rpv_dir(area)
    mm = f"{month:02d}"

    def rd(name):
        return compare._read_band(join(d, name))

    slope, gt, shape = compare.read_geotiff(join(d, "slope_adjusted.tif"))
    aspect = _grass_to_compass(rd("aspect_adjusted.tif"))
    elev = rd("elevation.tif")
    linke = rd(f"linke_{mm}.tif")
    cbh = rd(f"kcb_{mm}.tif")
    cdh = rd(f"kcd_{mm}.tif")
    temps = np.stack([rd(f"t2m_{mm}_{hh:02d}.tif") for hh in range(0, 24, 3)], axis=-1)
    n_dir = int(round(360 / HORIZON_STEP_DEGREES))
    horizon = np.stack([rd(f"horizon_{i:02d}.tif") for i in range(n_dir)], axis=-1)
    hpv = rd(f"hpv_day{day}.tif")

    lat, lon = _latlon(gt, shape)
    valid = (np.isfinite(hpv) & np.isfinite(slope) & np.isfinite(aspect) & np.isfinite(elev)
             & np.isfinite(linke) & np.isfinite(cbh) & np.isfinite(cdh)
             & np.all(np.isfinite(temps), axis=-1) & np.all(np.isfinite(horizon), axis=-1))
    idx = np.nonzero(valid)

    port = ir.compute_daily_pv(
        slope[idx], aspect[idx], elev[idx], lat[idx], lon[idx],
        horizon[idx], HORIZON_STEP_DEGREES,
        linke[idx], cbh[idx], cdh[idx], temps[idx], 0.2,
        day, -solar_declination(day), CSI_COEFFS)

    gold = hpv[idx]
    with np.errstate(divide="ignore", invalid="ignore"):
        pc = np.abs(100.0 * (port - gold) / gold)
    pc = pc[np.isfinite(pc)]
    return RpvStats(area.name, day, int(pc.size), float(pc.mean()),
                    float(np.percentile(pc, 99)), float(pc.max()),
                    float(100.0 * np.mean(pc < 2.0)))


def has_met_tar() -> bool:
    return os.path.exists(MET_TAR)


def check_annual(area: Area) -> RpvStats:
    """Full pipeline: the r.pv port over all 12 months + met sampled from pvgis_data_uk.tar +
    wind/spectral + annual sum, vs the frozen kwh_year golden (using the captured GRASS-adjusted
    slope/aspect + horizon, so this isolates everything except slope/aspect derivation)."""
    d = rpv_dir(area)
    slope, gt, shape = compare.read_geotiff(join(d, "slope_adjusted.tif"))
    aspect = _grass_to_compass(compare._read_band(join(d, "aspect_adjusted.tif")))
    elev = compare._read_band(join(d, "elevation.tif"))
    n_dir = int(round(360 / HORIZON_STEP_DEGREES))
    horizon = np.stack([compare._read_band(join(d, f"horizon_{i:02d}.tif"))
                        for i in range(n_dir)], axis=-1)
    kwh_gold = compare._read_band(join(area.golden_dir, "kwh_year.tif"))

    valid = (np.isfinite(kwh_gold) & np.isfinite(slope) & np.isfinite(aspect)
             & np.isfinite(elev) & np.all(np.isfinite(horizon), axis=-1))

    met = met_data.MetData(MET_TAR, gt, shape, resample="near")
    _, kwh_year = run_pv.compute_pv(slope, aspect, elev, gt, horizon,
                                    HORIZON_STEP_DEGREES, met, CSI_COEFFS, valid=valid)

    port = kwh_year[valid]
    gold = kwh_gold[valid]
    with np.errstate(divide="ignore", invalid="ignore"):
        pc = np.abs(100.0 * (port - gold) / gold)
    pc = pc[np.isfinite(pc)]
    return RpvStats(area.name, 0, int(pc.size), float(pc.mean()),
                    float(np.percentile(pc, 99)), float(pc.max()),
                    float(100.0 * np.mean(pc < 2.0)))


def check_fields_annual(area: Area) -> RpvStats:
    """As check_annual, but through run_pv.compute_pv_fields — the whole-grid assembly the
    orchestrator uses (lat/lon + met + compute_pv + PixelFields packaging). Confirms the packaged
    kwh_year field still matches the golden."""
    d = rpv_dir(area)
    slope, gt, shape = compare.read_geotiff(join(d, "slope_adjusted.tif"))
    aspect = _grass_to_compass(compare._read_band(join(d, "aspect_adjusted.tif")))
    elev = compare._read_band(join(d, "elevation.tif"))
    n_dir = int(round(360 / HORIZON_STEP_DEGREES))
    horizon = np.stack([compare._read_band(join(d, f"horizon_{i:02d}.tif"))
                        for i in range(n_dir)], axis=-1)
    kwh_gold = compare._read_band(join(area.golden_dir, "kwh_year.tif"))

    valid = (np.isfinite(kwh_gold) & np.isfinite(slope) & np.isfinite(aspect)
             & np.isfinite(elev) & np.all(np.isfinite(horizon), axis=-1))

    met = met_data.MetData(MET_TAR, gt, shape, resample="near")
    fields = run_pv.compute_pv_fields(slope, aspect, elev, horizon, gt, met,
                                      CSI_COEFFS, HORIZON_STEP_DEGREES, valid=valid)

    port = fields.values["kwh_year"]
    gold = kwh_gold[fields.rows, fields.cols]
    with np.errstate(divide="ignore", invalid="ignore"):
        pc = np.abs(100.0 * (port - gold) / gold)
    pc = pc[np.isfinite(pc)]
    return RpvStats(area.name, 0, int(pc.size), float(pc.mean()),
                    float(np.percentile(pc, 99)), float(pc.max()),
                    float(100.0 * np.mean(pc < 2.0)))
