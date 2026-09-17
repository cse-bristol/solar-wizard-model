# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
"""
PV power layer of the r.pv port: module temperature + polynomial efficiency, and the
3-hourly ambient-temperature interpolation.

Vectorised over pixels; `coeffs` are the 8 numbers from a <panel>.coeffs file
(coeffs[7] is the module-temp rise per W/m^2).
"""
import numpy as np

T_STC = 25.0


def efficiency(irr: np.ndarray, ambient_temp: np.ndarray, coeffs) -> np.ndarray:
    """Relative PV efficiency (1.0 at STC) for irradiance `irr` (W/m^2) and ambient
    temperature (deg C). Zero where irr <= 0."""
    c = coeffs
    irr = np.asarray(irr, dtype=np.float64)
    relirr = 0.001 * irr
    pos = relirr > 0.0
    out = np.zeros_like(irr)
    lnrelirr = np.log(np.where(pos, relirr, 1.0))
    tmod = irr * c[7] + np.asarray(ambient_temp, dtype=np.float64)
    tprime = tmod - T_STC
    pm = (c[0]
          + lnrelirr * (c[1] + lnrelirr * c[2])
          + tprime * (c[3] + lnrelirr * (c[4] + lnrelirr * c[5]) + c[6] * tprime))
    return np.where(pos, pm / c[0], out)


def interpolate_temperature(temps8: np.ndarray, pres_time: float,
                            longitude_rad: np.ndarray) -> np.ndarray:
    """Interpolate the 8 three-hourly ambient temperatures to solar time `pres_time` (hours),
    per r.pv temperatureInterpolate. `temps8` is (n_pixels, 8); longitude in radians.

    r.pv selects the slot from floor(locTime) but weights by the fractional locTime, so the
    weight can exceed 1 within a slot — reproduced here exactly.
    """
    n_slots = temps8.shape[1]
    interval = 24.0 / n_slots
    loc_time = pres_time - np.degrees(longitude_rad) / 15.0
    loc_time = np.mod(loc_time, 24.0)

    prevslot = (np.floor(loc_time) / interval).astype(np.int64)
    nextslot = (prevslot + 1) % n_slots
    time_frac = loc_time - interval * prevslot

    rows = np.arange(temps8.shape[0])
    prev = temps8[rows, prevslot]
    nxt = temps8[rows, nextslot]
    return prev + (time_frac / interval) * (nxt - prev)
