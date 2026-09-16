# r.pv / r.sun algorithm spec (for the Phase 2 port)

Reverse-engineered from the vendored PVMAPS source (`grass_modules/r.pv/{main.c,rsunlib.c}`
+ `nix/r.pv.patch`), for the exact invocation PVMAPS makes:

```
r.pv -a -s --quiet elevation=E aspect=ASP slope=SLP
     horizon_basename=HOR horizon_step=STEP albedo_value=0.2
     linke=tl_0m_MM coefbh=kcb_MM coefdh=kcd_MM
     temperatures=t2m_avg_MM_00..21 (8 rasters)
     declin=<value> civiltime=0 modelparameters=<csi|cdte>.coeffs
     day=D step=0.25 beam_rad=.. diff_rad=.. refl_rad=.. glob_pow=hpv
```

Flags in effect: `-a` angle-loss ON; `-s` shadow ON; horizon rasters given ⇒ **horizon-data
shadow mode**; no `-i` ⇒ `highIrr = 0`; temperatures given ⇒ `useTemperature` ON; no wind ⇒
`useWind` OFF; `civiltime=0` but the option is *present* ⇒ `useCivilTime` ON with `civilTime=0`.
Output consumed downstream is **`glob_pow` (hpv)** = daily PV energy per pixel (the monthly
representative-day value); PVMAPS then multiplies by wind & spectral rasters and sums months.

All per-pixel, fully vectorisable; outer loop over ~day timesteps. Aspect/slope are the
GRASS-adjusted rasters (slope degrees; aspect CCW-from-East degrees, 0 ⇒ flat/UNDEF).

**Critical aspect gotcha (the main Phase-2 bug):** r.pv does **not** use the aspect raster
directly. On input it converts each non-zero aspect to compass (0=N) — `o = (o<90 ? 90−o :
450−o)` (main.c ~L1344), i.e. `(90 − aspect_ccw_east) mod 360` — and the *converted* value
feeds both `cos_v/sin_v` and the `shift12hrs` test. Skip this and steep roofs are orientation-
blind (south ≈ north) and off by tens of percent. Value 0 stays UNDEF (a genuinely East-facing
`aspect_ccw_east==0` roof is therefore treated as flat — a GRASS quirk). The single-value CLI
default documents this: "270 is south".

## Constants
- `G_norm_extra = com_sol_const(day) = 1367·(1 + 0.03344·cos(2π·day/365.25 − 0.048869))`.
- `declination`: PVMAPS passes `declin=` a positive value; r.pv sets `declination = −declin`.
  (PVMAPS computes `declin` via `_calc_solar_declination`, already ported; note the sign flip.)
- `a_r = 0.155`; `angular_loss_denom = 1/(1 − exp(−1/a_r))`.
- `T_STC = 25`. Model coeffs `modelConstants[0..7]` = the 8 numbers in `<panel>.coeffs`
  (csi/cdte); `[7]` (0.035) is the module-temp rise per W/m². (The hardcoded defaults in
  `initEfficiencyCoeffs` are fully overwritten when a coeffs file is given, which it always is.)
- Horizon quantisation: r.pv stores horizons as bytes, `byte = round(150·h_rad)` (SCALING_FACTOR
  = 150), read back as `byte/150`. So quantise each horizon to `round(150·h)/150` rad (≈0.38°
  steps) before the shadow test.
- `HOURANGLE = π/12`. Time step `step = 0.25` h. `horizonInterval = radians(horizon_step)`.
  `arrayNumInt = 360/horizon_step` (number of horizon directions).

## Per-pixel constant geometry (independent of time)
Inputs: latitude/longitude of the pixel centre (from the 27700→4326 projection, radians),
slope σ and aspect α (radians; α is GRASS CCW-from-East, α=UNDEF if the raster value was 0).

```
sinlat = sin(−lat);  coslat = cos(−lat)          # NB negated latitude
sindecl = sin(declination); cosdecl = cos(declination)
# day geometry (com_par_const), timeOffset := locTimeOffset below (civiltime on):
lum_C11 = sinlat·cosdecl;  lum_C13 = −coslat·sindecl;  lum_C22 = cosdecl
lum_C31 = coslat·cosdecl;  lum_C33 = sinlat·sindecl
# sunrise/sunset (only when |lum_C31| ≥ 1e-4):
pom = −lum_C33/lum_C31; if |pom|≤1: pom=acos(pom)·180/π;
      sunrise = (90−pom)/15 + 6;  sunset = (pom−90)/15 + 18
  else pom<0 ⇒ sun up all day (0,24); else ⇒ (12,12)
# inclined-plane transform (com_par per-pixel setup + r.pv.patch):
cos_u = sin(σ); sin_u = cos(σ); cos_v = −sin(α); sin_v = cos(α)   # from cos/sin(π/2∓·)
sin_phi_l = −coslat·cos_u·sin_v + sinlat·sin_u;  latid_l = asin(sin_phi_l)
q1 = sinlat·cos_u·sin_v + coslat·sin_u
if q1 ≠ 0: tan_lam_l = −cos_u·cos_v/q1; longit_l = atan(tan_lam_l); isBestAM = tan_lam_l>0
else:      longit_l = π/2; isBestAM = true
shouldBeBestAM = (0 < α ≤ π)               # α=UNDEF(flat) → treat as no orientation
shift12hrs = (shouldBeBestAM ≠ isBestAM)   # → adds π to the incidence time term
lum_C31_l = cos(latid_l)·cosdecl;  lum_C33_l = sin_phi_l·sindecl
```
Civil-time offset (civiltime on, civilTime=0): `timeOffset = 0.128·sin(dayRad−0.04887) +
0.165·sin(2·dayRad+0.34383)`, `dayRad = 2π·day/365.25`; and `longitTime = −longitude_deg/15`
enters `com_par_const` as `timeAngle −= (timeOffset+longitTime)·HOURANGLE` — folded into the
per-timestep timeAngle.

## Per-timestep loop (integrated daily, tt == NULL)
```
srStepNo = int(sunrise/step)
firstTime = (srStepNo + (1.5 if (sunrise − srStepNo·step) > 0.5·step else 0.5))·step
timeAngle0 = (firstTime − 12)·HOURANGLE;  lastAngle = (sunset − 12)·HOURANGLE
for presTime = firstTime, firstTime+step, … while timeAngle ≤ lastAngle:  # dfr = step
    (with civiltime: timeAngle_eff = timeAngle − (timeOffset+longitTime)·HOURANGLE)
    # --- sun position (com_par) ---
    cta = cos(timeAngle)
    Lx = −lum_C22·sin(timeAngle);  Ly = lum_C11·cta + lum_C13
    sinSolarAltitude = lum_C31·cta + lum_C33;  solarAltitude = asin(sinSolarAltitude)
    pom = hypot(Lx, Ly)
    solarAzimuth = acos(Ly/pom) (if pom>1e-4 else UNDEF); if Lx<0: solarAzimuth = 2π − it
    sunAzimuthAngle = (π/2 − solarAzimuth) if solarAzimuth<π/2 else (2.5π − solarAzimuth)
    # --- shadow via horizon (lumcline2) ---
    horizPos = sunAzimuthAngle / horizonInterval; lo=int(horizPos); hi=(lo+1)%arrayNumInt
    horizonHeight = (1−(horizPos−lo))·H[lo] + (horizPos−lo)·H[hi]      # H = quantised horizons
    isShadow = horizonHeight > solarAltitude
    tOff = π if shift12hrs else 0
    s0 = lum_C31_l·cos(−timeAngle − longit_l + tOff) + lum_C33_l;  if s0<0: s0 = 0
    # --- radiation (only if solarAltitude>0) ---
    if not isShadow and s0>0:  beam_irr = brad_angle_loss(s0)      # sets bh
    else:                      beam_irr = 0; bh = 0
    diff_irr = drad_angle_loss(s0, bh)      # sets rr (reflected)
    refl_irr = rr
    totrad = beam_irr + diff_irr + refl_irr
    presTemperature = temperatureInterpolate(temps8, presTime, longitude)
    effic = efficiency(totrad, presTemperature)
    totpower += effic · totrad · step
# glob_pow pixel value = totpower   (beam_e/diff_e/refl_e = Σ step·{beam,diff,refl}_irr are the
# optional beam/diff/refl_rad outputs; not needed for glob_pow)
```
(With `highIrr=0`, the clear-sky vs real-sky split in joules2 collapses: `sunRadVar_cs ==
sunRadVar`, so efficiency and power use the same `totrad`.)

## brad_angle_loss(s0) → beam on slope (sets bh = beam horizontal)
```
h = solarAltitude
p = exp(−z_orig/8434.5)                                  # z_orig = pixel elevation (m)
drefract = 0.061359·(0.1594 + h·(1.123 + 0.065656·h)) / (1 + h·(28.9344 + 277.3971·h))
h0 = h + drefract
AM = p / (sin(h0) + 0.50572·(h0·180/π + 6.07995)^−1.6364)   # optical air mass
AM2Linke = 0.8662·linke
rayl = 1/(6.6296 + AM·(1.7513 + AM·(−0.1202 + AM·(0.0065 − AM·0.00013))))  if AM≤20
       else 1/(10.4 + 0.718·AM)
bh = cbh · G_norm_extra · sinSolarAltitude · exp(−rayl·AM·AM2Linke)     # cbh = kcb pixel value
br = bh·s0/sinSolarAltitude  if (α≠UNDEF and σ≠0) else bh
br *= (1 − exp(−s0/a_r))·angular_loss_denom             # -a angle-of-incidence loss
return br
```

## drad_angle_loss(s0, bh) → diffuse on slope (sets rr = reflected on slope)
```
cs = cos(σ); ss = sin(σ)
tn  = −0.015843 + linke·(0.030543 + 0.0003797·linke)
A1b = 0.26463 + linke·(−0.061581 + 0.0031408·linke)
A1  = 0.0022/tn if A1b·tn < 0.0022 else A1b
A2  = 2.04020 + linke·(0.018945 − 0.011161·linke)
A3  = −1.3025 + linke·(0.039231 + 0.0085079·linke)
fd  = A1 + A2·sinSolarAltitude + A3·sinSolarAltitude²
dh  = cdh · G_norm_extra · fd · tn                       # cdh = kcd pixel value; diffuse horiz
gh  = bh + dh
if (α≠UNDEF and σ≠0):
    kb = bh/(G_norm_extra·sinSolarAltitude)
    r_sky = (1+cs)/2
    a_ln = solarAzimuth − α; wrap to (−π,π]
    fg = ss − σ·cs − π·sin(σ/2)²
    if isShadow or s0≤0:            fx = r_sky + fg·0.252271
    elif solarAltitude ≥ 0.1:       fx = ((0.00263 − kb·(0.712 + 0.6883·kb))·fg + r_sky)·(1−kb) + kb·s0/sinSolarAltitude
    else:                           fx = ((0.00263 − 0.712·kb − 0.6883·kb²)·fg + r_sky)·(1−kb) + kb·ss·cos(a_ln)/(0.1 − 0.008·solarAltitude)
    dr = dh·fx
    rr = alb·gh·(1−cs)/2
else: dr = dh; rr = 0
# -a angle losses:
c1 = 4/(3π); c2 = −0.074
diff_coeff = ss + (π − σ − ss)/(1+cs)
refl_coeff = 0 if cs==1 else ss + (σ − ss)/(1−cs)
dr *= 1 − exp(−(c1·diff_coeff + c2·diff_coeff²)/a_r)
rr *= 1 − exp(−(c1·refl_coeff + c2·refl_coeff²)/a_r)
return dr        # rr returned via pointer
```

## efficiency(irr, ambient_temp) — PV model
```
relirr = 0.001·irr; if relirr ≤ 0: return 0
lnrelirr = log(relirr)
tmod = irr·modelConstants[7] + ambient_temp          # module temp
tprime = tmod − 25
pm = c0 + lnrelirr·(c1 + lnrelirr·c2) + tprime·(c3 + lnrelirr·(c4 + lnrelirr·c5) + c6·tprime)
return pm/c0                                           # ci = modelConstants[i]
```

## temperatureInterpolate(temps8, presTime, longitude_rad) — 3-hourly ambient temp
```
locTime = presTime − longitude_deg/15;  wrap into [0,24)
timeInterval = 24/8 = 3
prevslot = int( floor(locTime) / 3 );  nextslot = (prevslot+1) % 8
timeFrac = locTime − 3·prevslot
return temps8[prevslot] + (timeFrac/3)·(temps8[nextslot] − temps8[prevslot])
```
(8 rasters t2m_avg_MM_{00,03,…,21}, sampled at the pixel.)

## After r.pv (PVMAPS pipeline, per representative day → month)
`hpv_wind_spectral = hpv · windeffect_MM · spectraleffect_{cSi,CdTe}_MM`, each raster
defaulting to 1.0 where the met data is missing (northern Scotland etc.). Monthly Wh
(representative-day value) → yearly kWh: `Σ_months hpv_ws[day]·num_days · 0.001`.

## Met inputs (from pvgis_data_uk.tar, 27700 GeoTIFFs), sampled per pixel
`linke = tl_0m_MM`, `cbh = kcb_MM`, `cdh = kcd_MM`, `temps8 = t2m_avg_MM_HH`,
`windeffect_MM`, `spectraleffect_{cSi,CdTe}_MM`. MM = representative month; the 12
representative (day, month) pairs are `_monthly_pv_time_steps()`.

## Validation staging (which golden isolates what)
- **thurso**: no wind, no spectral ⇒ monthly golden == raw hpv. Cleanest core-r.pv check.
- **alnwick / alnwick_flat**: wind present, spectral missing ⇒ hpv·wind.
- **real_data**: wind + spectral.
Feed the port the *same* slope/aspect/horizon rasters r.pv consumed (capture them as goldens)
to isolate the r.pv math from slope/aspect/horizon derivation.
