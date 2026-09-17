# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
"""
Native-Python port of the GRASS/PVMAPS r.pv / r.sun clear-sky irradiation + PV integration.
Faithful to r.pv's main.c / rsunlib.c for the PVMAPS invocation.

`compute_daily_pv` integrates one representative day: for each timestep from sunrise to
sunset it finds the sun position, tests terrain shadowing against the horizon profile,
computes beam/diffuse/reflected irradiance on the inclined roof (ESRA, scaled by the
real-sky kcb/kcd coefficients, with -a angle-of-incidence losses), applies the PV
temperature/efficiency model, and sums energy. Vectorised over a flat array of pixels.
"""
import math

import numpy as np

from solar_pv.pv.pv_model import efficiency, interpolate_temperature

A_R = 0.155
ANGULAR_LOSS_DENOM = 1.0 / (1.0 - math.exp(-1.0 / A_R))
HOURANGLE = math.pi / 12.0
EPS = 1e-4
DEG2RAD = math.pi / 180.0
RAD2DEG = 180.0 / math.pi
HORIZON_SCALING_FACTOR = 150.0  # r.pv stores horizons as byte = round(150*rad)


def com_sol_const(day: int) -> float:
    """Extraterrestrial normal irradiance (W/m^2) for day-of-year."""
    d1 = 2.0 * math.pi * day / 365.25
    return 1367.0 * (1.0 + 0.03344 * math.cos(d1 - 0.048869))


def _wrap_pi(a):
    """Wrap to (-pi, pi] as r.pv does for a_ln."""
    out = np.where(a > math.pi, a - 2 * math.pi, a)
    out = np.where(a < -math.pi, a + 2 * math.pi, out)
    return out


def compute_daily_pv(slope_deg, aspect_compass_deg, elevation, latitude, longitude,
                     horizon, horizon_step_deg,
                     linke, cbh, cdh, temps8, albedo,
                     day, declination, coeffs,
                     step=0.25, return_components=False):
    """
    :param slope_deg, aspect_compass_deg: (n,) roof slope and compass aspect (0 = N, clockwise)
        in degrees; aspect value exactly 0 means flat/UNDEF (as gdaldem's -zero_for_flat emits).
    :param elevation: (n,) pixel elevation (m).
    :param latitude, longitude: (n,) pixel-centre lat/lon in radians.
    :param horizon: (n, n_dir) horizon heights (radians), direction d = d*horizon_step_deg CCW
        from East (as produced by the horizon port).
    :param linke, cbh, cdh: (n,) per-pixel Linke turbidity and real-sky beam/diffuse coeffs.
    :param temps8: (n, 8) three-hourly ambient temperatures (deg C).
    :param albedo: scalar or (n,) ground albedo.
    :param day: representative day-of-year; declination in radians (already sign-correct:
        r.pv uses declination = -declin, and PVMAPS passes declin, so pass -declin here).
    :param coeffs: 8 PV model constants from the <panel>.coeffs file.
    :return: (n,) daily PV energy (the glob_pow / hpv pixel value, pre wind/spectral).
    """
    slope_deg = np.asarray(slope_deg, dtype=np.float64)
    aspect_compass_deg = np.asarray(aspect_compass_deg, dtype=np.float64)
    z = np.asarray(elevation, dtype=np.float64)
    lat = np.asarray(latitude, dtype=np.float64)
    lon = np.asarray(longitude, dtype=np.float64)
    linke = np.asarray(linke, dtype=np.float64)
    cbh = np.asarray(cbh, dtype=np.float64)
    cdh = np.asarray(cdh, dtype=np.float64)
    albedo = np.broadcast_to(np.asarray(albedo, dtype=np.float64), z.shape)

    slope = slope_deg * DEG2RAD
    # aspect is compass (0 = N, clockwise), 0 marking flat/UNDEF. r.pv works in compass
    # internally (main.c ~L1344 converts its CCW-from-East input up front, feeding both
    # cos_v/sin_v and the shift12hrs test), so the compass value is fed straight in here:
    aspect_undef = aspect_compass_deg == 0.0
    aspect = np.where(aspect_undef, 0.0, aspect_compass_deg * DEG2RAD)
    oriented = (~aspect_undef) & (slope != 0.0)

    g_norm_extra = com_sol_const(day)
    sindecl = math.sin(declination)
    cosdecl = math.cos(declination)

    # r.pv negates latitude throughout:
    sinlat = np.sin(-lat)
    coslat = np.cos(-lat)

    # --- day geometry (com_par_const): sunrise/sunset + lum_C coefficients ---
    lum_C11 = sinlat * cosdecl
    lum_C13 = -coslat * sindecl
    lum_C22 = cosdecl
    lum_C31 = coslat * cosdecl
    lum_C33 = sinlat * sindecl

    sunrise, sunset = _sunrise_sunset(lum_C31, lum_C33)

    # In all-day mode r.pv overwrites timeAngle with firstAngle (from sunrise), discarding
    # the civiltime/longitude offset com_par_const applies to timeAngle. Longitude only
    # enters via the temperature interpolation, so timeAngle is just (presTime-12)*HOURANGLE.

    # --- inclined-plane transform (com_par per-pixel + r.pv.patch north-facing fix) ---
    cos_u = np.sin(slope)
    sin_u = np.cos(slope)
    cos_v = -np.sin(aspect)
    sin_v = np.cos(aspect)
    sin_phi_l = -coslat * cos_u * sin_v + sinlat * sin_u
    latid_l = np.arcsin(np.clip(sin_phi_l, -1.0, 1.0))
    q1 = sinlat * cos_u * sin_v + coslat * sin_u
    with np.errstate(divide="ignore", invalid="ignore"):
        tan_lam_l = np.where(q1 != 0.0, -cos_u * cos_v / q1, 0.0)
    longit_l = np.where(q1 != 0.0, np.arctan(tan_lam_l), math.pi / 2.0)
    is_best_am = np.where(q1 != 0.0, tan_lam_l > 0.0, True)
    should_be_best_am = (aspect > 0.0) & (aspect <= math.pi)
    shift12 = should_be_best_am != is_best_am
    t_off = np.where(shift12, math.pi, 0.0)
    lum_C31_l = np.cos(latid_l) * cosdecl
    lum_C33_l = sin_phi_l * sindecl

    horizon_interval = horizon_step_deg * DEG2RAD
    n_dir = horizon.shape[1]
    # r.pv quantises horizons to byte/150 before the shadow test:
    horizon_q = np.round(HORIZON_SCALING_FACTOR * horizon) / HORIZON_SCALING_FACTOR

    # --- per-pixel step grid: centres at (k+0.5)*step, from firstTime to sunset ---
    sr_step_no = np.floor(sunrise / step)
    first_time = np.where((sunrise - sr_step_no * step) > 0.5 * step,
                          (sr_step_no + 1.5) * step, (sr_step_no + 0.5) * step)
    last_angle = (sunset - 12.0) * HOURANGLE

    totpower = np.zeros_like(z)
    beam_e = np.zeros_like(z)
    diff_e = np.zeros_like(z)
    refl_e = np.zeros_like(z)

    centres = (np.arange(0, int(round(24.0 / step))) + 0.5) * step
    tol = 1e-9
    for pres_time in centres:
        active = (pres_time >= first_time - tol) & \
                 (((pres_time - 12.0) * HOURANGLE) <= last_angle + tol)
        if not active.any():
            continue

        time_angle = (pres_time - 12.0) * HOURANGLE

        sin_alt, solar_alt, solar_az, sun_az = _sun_position(
            time_angle, lum_C11, lum_C13, lum_C22, lum_C31, lum_C33)

        above = active & (solar_alt > 0.0)
        if not above.any():
            continue

        # shadow test against interpolated horizon at the sun azimuth:
        is_shadow = _horizon_shadow(sun_az, horizon_q, horizon_interval, n_dir, solar_alt)

        # incidence factor s0 on the inclined plane (lumcline2):
        s0 = lum_C31_l * np.cos(-time_angle - longit_l + t_off) + lum_C33_l
        s0 = np.where(s0 < 0.0, 0.0, s0)

        lit = above & (~is_shadow) & (s0 > 0.0)

        bh = np.zeros_like(z)
        beam = np.zeros_like(z)
        beam[lit], bh[lit] = _brad_angle_loss(
            s0[lit], solar_alt[lit], sin_alt[lit], z[lit], linke[lit], cbh[lit],
            g_norm_extra, oriented[lit])

        diff, refl = _drad_angle_loss(
            s0, bh, solar_alt, sin_alt, is_shadow, slope, cbh_unused=None,
            cdh=cdh, linke=linke, albedo=albedo, g_norm_extra=g_norm_extra,
            solar_az=solar_az, aspect=aspect, oriented=oriented)

        totrad = np.where(above, beam + diff + refl, 0.0)
        temp = interpolate_temperature(temps8, pres_time, lon)
        effic = efficiency(totrad, temp, coeffs)
        totpower = totpower + np.where(above, effic * totrad * step, 0.0)

        beam_e += np.where(above, beam * step, 0.0)
        diff_e += np.where(above, diff * step, 0.0)
        refl_e += np.where(above, refl * step, 0.0)

    if return_components:
        return totpower, beam_e, diff_e, refl_e
    return totpower


def _sunrise_sunset(lum_C31, lum_C33):
    """sunrise/sunset hours per pixel (com_par_const). UK: |lum_C31| >> EPS always."""
    with np.errstate(divide="ignore", invalid="ignore"):
        pom = np.where(np.abs(lum_C31) >= EPS, -lum_C33 / lum_C31, 0.0)
    pom_ok = np.abs(pom) <= 1.0
    acos_deg = np.degrees(np.arccos(np.clip(pom, -1.0, 1.0)))
    sunrise = np.where(pom_ok, (90.0 - acos_deg) / 15.0 + 6.0,
                       np.where(pom < 0.0, 0.0, 12.0))
    sunset = np.where(pom_ok, (acos_deg - 90.0) / 15.0 + 18.0,
                      np.where(pom < 0.0, 24.0, 12.0))
    return sunrise, sunset


def _sun_position(time_angle, lum_C11, lum_C13, lum_C22, lum_C31, lum_C33):
    """com_par: solar altitude/azimuth and the CCW-from-East sun azimuth for horizon lookup."""
    cta = np.cos(time_angle)
    lx = -lum_C22 * np.sin(time_angle)
    ly = lum_C11 * cta + lum_C13
    sin_alt = lum_C31 * cta + lum_C33
    solar_alt = np.arcsin(np.clip(sin_alt, -1.0, 1.0))

    pom = np.hypot(lx, ly)
    with np.errstate(divide="ignore", invalid="ignore"):
        solar_az = np.arccos(np.clip(np.where(pom > EPS, ly / pom, 1.0), -1.0, 1.0))
    solar_az = np.where(lx < 0.0, 2 * math.pi - solar_az, solar_az)

    sun_az = np.where(solar_az < 0.5 * math.pi,
                      0.5 * math.pi - solar_az,
                      2.5 * math.pi - solar_az)
    return sin_alt, solar_alt, solar_az, sun_az


def _horizon_shadow(sun_az, horizon_q, horizon_interval, n_dir, solar_alt):
    """Interpolate the horizon profile at the sun azimuth and test shadowing (lumcline2)."""
    horiz_pos = sun_az / horizon_interval
    lo = np.floor(horiz_pos).astype(np.int64) % n_dir
    hi = (lo + 1) % n_dir
    frac = horiz_pos - np.floor(horiz_pos)
    rows = np.arange(horizon_q.shape[0])
    h_lo = horizon_q[rows, lo]
    h_hi = horizon_q[rows, hi]
    horizon_height = (1.0 - frac) * h_lo + frac * h_hi
    return horizon_height > solar_alt


def _brad_angle_loss(s0, solar_alt, sin_alt, z, linke, cbh, g_norm_extra, oriented):
    """Beam irradiance on the slope with -a angle loss; returns (br, bh)."""
    p = np.exp(-z / 8434.5)
    temp1 = 0.1594 + solar_alt * (1.123 + 0.065656 * solar_alt)
    temp2 = 1.0 + solar_alt * (28.9344 + 277.3971 * solar_alt)
    drefract = 0.061359 * temp1 / temp2
    h0 = solar_alt + drefract
    air_mass = p / (np.sin(h0) + 0.50572 * np.power(h0 * RAD2DEG + 6.07995, -1.6364))
    am2linke = 0.8662 * linke
    rayl = np.where(
        air_mass <= 20.0,
        1.0 / (6.6296 + air_mass * (1.7513 + air_mass * (-0.1202 + air_mass * (0.0065 - air_mass * 0.00013)))),
        1.0 / (10.4 + 0.718 * air_mass))
    bh = cbh * g_norm_extra * sin_alt * np.exp(-rayl * air_mass * am2linke)
    br = np.where(oriented, bh * s0 / sin_alt, bh)
    br = br * (1.0 - np.exp(-s0 / A_R)) * ANGULAR_LOSS_DENOM
    return br, bh


def _drad_angle_loss(s0, bh, solar_alt, sin_alt, is_shadow, slope, cbh_unused,
                     cdh, linke, albedo, g_norm_extra, solar_az, aspect, oriented):
    """Diffuse (returned) + reflected (rr) irradiance on the slope, with -a angle losses."""
    cs = np.cos(slope)
    ss = np.sin(slope)

    tn = -0.015843 + linke * (0.030543 + 0.0003797 * linke)
    A1b = 0.26463 + linke * (-0.061581 + 0.0031408 * linke)
    A1 = np.where(A1b * tn < 0.0022, 0.0022 / tn, A1b)
    A2 = 2.04020 + linke * (0.018945 - 0.011161 * linke)
    A3 = -1.3025 + linke * (0.039231 + 0.0085079 * linke)
    fd = A1 + A2 * sin_alt + A3 * sin_alt * sin_alt
    dh = cdh * g_norm_extra * fd * tn
    gh = bh + dh

    with np.errstate(divide="ignore", invalid="ignore"):
        kb = bh / (g_norm_extra * sin_alt)
    r_sky = (1.0 + cs) / 2.0
    a_ln = _wrap_pi(solar_az - aspect)
    fg = ss - slope * cs - math.pi * np.sin(slope / 2.0) ** 2

    fx_shadow = r_sky + fg * 0.252271
    fx_high = ((0.00263 - kb * (0.712 + 0.6883 * kb)) * fg + r_sky) * (1.0 - kb) \
        + kb * s0 / sin_alt
    with np.errstate(divide="ignore", invalid="ignore"):
        fx_low = ((0.00263 - 0.712 * kb - 0.6883 * kb * kb) * fg + r_sky) * (1.0 - kb) \
            + kb * ss * np.cos(a_ln) / (0.1 - 0.008 * solar_alt)
    fx = np.where(is_shadow | (s0 <= 0.0), fx_shadow,
                  np.where(solar_alt >= 0.1, fx_high, fx_low))

    dr = np.where(oriented, dh * fx, dh)
    rr = np.where(oriented, albedo * gh * (1.0 - cs) / 2.0, 0.0)

    c1 = 4.0 / (3.0 * math.pi)
    c2 = -0.074
    diff_coeff = ss + (math.pi - slope - ss) / (1.0 + cs)
    with np.errstate(divide="ignore", invalid="ignore"):
        refl_coeff = np.where(cs == 1.0, 0.0, ss + (slope - ss) / (1.0 - cs))
    dr = dr * (1.0 - np.exp(-(c1 * diff_coeff + c2 * diff_coeff ** 2) / A_R))
    rr = rr * (1.0 - np.exp(-(c1 * refl_coeff + c2 * refl_coeff ** 2) / A_R))
    return dr, rr
