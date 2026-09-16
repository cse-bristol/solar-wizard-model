# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
"""
Assembly layer of the PV port: run the r.pv core (solar_pv.pv.irradiation) for each
representative month, apply the wind and spectral corrections, and sum to a yearly total,
reproducing the PVMAPS pipeline that follows r.pv. See docs/r-pv-algorithm.md.

The per-pixel outputs (monthly Wh + yearly kWh) are the seam the roof-plane aggregation
consumes, so this replaces the r.pv + wind/spectral + annual-sum steps of pvgis()/pvmaps.
"""
import math

import numpy as np

from solar_pv.pv import irradiation as ir

# (index, representative day-of-year, month, days-in-month) — matches PVMAPS
# _monthly_pv_time_steps() / _get_annual_rasters (num_days weighting):
MONTHLY_STEPS = [
    (0, 17, 1, 31), (1, 46, 2, 28), (2, 75, 3, 31), (3, 103, 4, 30),
    (4, 135, 5, 31), (5, 162, 6, 30), (6, 198, 7, 31), (7, 228, 8, 31),
    (8, 259, 9, 30), (9, 289, 10, 31), (10, 319, 11, 30), (11, 345, 12, 31),
]


def solar_declination(day: int) -> float:
    """PVMAPS _calc_solar_declination; compute_daily_pv takes declination = -this."""
    d1 = 2.0 * math.pi * day / 365.25
    return math.asin(0.3978 * math.sin(d1 - 1.4 + 0.0355 * math.sin(d1 - 0.0489)))


def _correction(factor, shape):
    """A wind/spectral correction array, with nodata/NaN -> 1.0 (PVMAPS defaults coverage
    gaps to 1.0), or all-ones when the layer is absent for this area."""
    if factor is None:
        return np.ones(shape)
    factor = np.asarray(factor, dtype=np.float64)
    return np.where(np.isfinite(factor), factor, 1.0)


def monthly_wh_and_annual(monthly_hpv, monthly_wind=None, monthly_spectral=None):
    """
    :param monthly_hpv: 12 arrays, raw daily PV energy (hpv) per representative month.
    :param monthly_wind, monthly_spectral: 12 correction arrays each (or None per-month / the
        whole list None), applied as hpv * wind * spectral with gaps -> 1.0.
    :return: (monthly_wh, kwh_year): 12 corrected representative-day Wh arrays, and the yearly
        kWh total (sum over months of wh * days-in-month * 0.001).
    """
    monthly_wind = [None] * 12 if monthly_wind is None else monthly_wind
    monthly_spectral = [None] * 12 if monthly_spectral is None else monthly_spectral

    monthly_wh = []
    kwh_year = None
    for (_, _, _, num_days), hpv, wind, spec in zip(
            MONTHLY_STEPS, monthly_hpv, monthly_wind, monthly_spectral):
        hpv = np.asarray(hpv, dtype=np.float64)
        wh = hpv * _correction(wind, hpv.shape) * _correction(spec, hpv.shape)
        monthly_wh.append(wh)
        contrib = wh * num_days
        kwh_year = contrib if kwh_year is None else kwh_year + contrib
    return monthly_wh, kwh_year * 0.001


def compute_pv(slope_deg, aspect_deg, elevation, latitude, longitude, horizon,
               horizon_step_deg, met, coeffs, valid=None, albedo=0.2):
    """
    Full r.pv-to-annual pipeline on a job grid. All spatial inputs are 2D (rows, cols), except
    `horizon` which is (rows, cols, n_dir). `met` is a solar_pv.pv.met_data.MetData bound to
    this grid. `valid` is an optional boolean mask of pixels to evaluate (default: finite
    slope/aspect/elevation). Returns (monthly_wh, kwh_year) as 2D arrays, NaN outside `valid`.
    """
    shape = np.asarray(slope_deg).shape
    if valid is None:
        valid = (np.isfinite(slope_deg) & np.isfinite(aspect_deg) & np.isfinite(elevation)
                 & np.all(np.isfinite(horizon), axis=-1))
    idx = np.nonzero(valid)

    monthly_hpv, monthly_wind, monthly_spectral = [], [], []
    for _, day, month, _ in MONTHLY_STEPS:
        m = met.for_month(month).at(idx)
        monthly_hpv.append(ir.compute_daily_pv(
            slope_deg[idx], aspect_deg[idx], elevation[idx], latitude[idx], longitude[idx],
            horizon[idx], horizon_step_deg,
            m.linke, m.cbh, m.cdh, m.temps8, albedo,
            day, -solar_declination(day), coeffs))
        monthly_wind.append(m.wind)
        monthly_spectral.append(m.spectral)

    flat_wh, flat_year = monthly_wh_and_annual(monthly_hpv, monthly_wind, monthly_spectral)

    monthly_wh = []
    for wh in flat_wh:
        full = np.full(shape, np.nan)
        full[idx] = wh
        monthly_wh.append(full)
    kwh_year = np.full(shape, np.nan)
    kwh_year[idx] = flat_year
    return monthly_wh, kwh_year
