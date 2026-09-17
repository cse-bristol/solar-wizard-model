# Plan: replace GRASS GIS / PVMAPS with a native-Python solar model

Status: **complete** (all phases done + acceptance gate passed) · Owner: Neil · Last updated: 2026-09-16

## Goal

Remove the runtime dependency on GRASS GIS and the PVMAPS addon modules
(`r.horizonmask`, `r.pv`) by porting the parts we actually use to Python (numpy).

Why:

- **Maintenance**: the model is pinned to nixpkgs 22.05 solely to build GRASS 8.2 +
  PVMAPS (`nix/grass-8.2.0-pvmaps.nix`, `default.nix` `pkgs2205`). Updating is painful.
- **Impedance mismatch**: the real numerical work is two algorithms, wrapped in ~4 files
  of subprocess/mapset/raster-import-export plumbing
  (`grass_gis_user.py`, `pvmaps.py`, `pvmaps_setup.py`) plus a Postgis raster round-trip
  in `aggregate_pixel_results.py`. A numpy port collapses most of this.
- **Structure**: whole-job raster processing forces the current DB-centric shape. Working
  in-process on arrays is a prerequisite for the longer-term goals of running a single
  building end-to-end and eventually dropping the DB dependency.

## Agreed decisions (2026-09-15)

- **Accuracy target: tight, ~1–2% on yearly kWh vs the current GRASS/PVMAPS output.**
- **Scope: through Phase 3** — port the two algorithms *and* collapse the
  raster→Postgis→pixels round-trip. Per-building restructure is a follow-up, not in scope.
- **Validation oracle: the frozen GRASS/PVMAPS output**, not the PVGIS web API.
  PVMAPS/`r.pv` is frozen; the PVGIS API is still actively developed and has **diverged**
  from it, so the API is only a secondary *shape* check (seasonality, orientation
  response) at a loose tolerance — never the pass/fail gate for the 1–2% target.
- **Slope/aspect stay on GDAL (`gdaldem`), unchanged (decided 2026-09-16).** See below.

## Decision: slope/aspect generation stays on GDAL

We keep slope and aspect exactly as they are — computed once by `generate_rasters` via
`gdal_helpers.slope`/`aspect` (`gdaldem`, GRASS-free) and loaded to the `SLOPE`/`ASPECT`
tables. We do **not** port GRASS `r.slope.aspect`. Why:

- **No GRASS dependency to remove.** Production already forces the GDAL rasters into r.pv
  (`forced_slope_filename` / `forced_aspect_filename_compass` in `pvgis.py`), deliberately —
  the code notes GRASS `r.slope.aspect` aspects diverge by ~3° after the 27700 switch. So
  porting `r.slope.aspect` would re-implement something the model chose not to use.
- **Aspect is a shared, first-class input.** Roof detection (`detect_roofs.py`) reads the
  same `ASPECT`/`SLOPE` tables. It must stay a single `generate_rasters` output, not be
  re-derived inside the PV port (which would risk the PV and roof-detection paths drifting).
- **The PV port treats slope/aspect as inputs**, like horizon — agnostic to provenance.
- **What actually drives the PV aspect is the roof-plane override, not the raw raster.**
  `generate_aspect_override_raster` rasterises each *usable* plane's single fitted
  aspect/slope; `gdaldem` is only the base/fallback outside detected planes. So "the PV
  aspect" is really roof-detection output, already in the DB — another reason the base
  raster's exact provenance matters little.

Consequences for the remaining work:
- The port must reproduce `create_pvmap`'s `_apply_slope_aspect_correction` — flat-roof
  default (slope < threshold → 10°/270) and the override merge — as a few numpy `where`
  ops, not a raster algorithm.
- Optional cleanup (low priority): today aspect makes a compass→grass→compass round-trip
  (`gdaldem` compass → `_conv_aspect` → r.pv's internal grass→compass). The port can carry
  one convention and drop the churn, provided the flat-roof/override merge stays consistent.
- **Validation gap to close in Phase 3:** the current goldens used GRASS `r.slope.aspect`
  (the capture neither forced the GDAL rasters nor applied plane overrides), so they are
  *not* the production path — fine for isolating the r.pv maths (what Phase 2 validated), but
  the production drop-in should be validated one level up: run a real job area through both
  the old GRASS `pvgis()` and the new port (GDAL slope/aspect + real roof-plane overrides)
  and diff the `pv_roof_plane` results.

## What GRASS actually does for us

Two numerical jobs, plus data-staging plumbing that exists only because GRASS keeps data
in its own raster database:

| Job | GRASS module | Computes | Current code |
|---|---|---|---|
| **A. Horizon** | `r.horizonmask` | Per masked pixel, per azimuth direction (CCW from East), max terrain elevation angle within `horizon_search_radius`. Radians, clamped 0–π/2. | `pvmaps._calc_horizon` (`pvmaps.py:412`) |
| **B. Irradiation → PV** | `r.pv` (JRC PVMAPS fork of `r.sun`) | Per pixel, integrate a representative day per month (`step=0.25h`): beam/diffuse/reflected irradiance with horizon shadowing, then PVMAPS module-temperature + 8-coeff polynomial efficiency (`resources/{csi,cdte}.coeffs`) → PV power as if each pixel were a **1 kWp** system. Wind + spectral corrections, summed to monthly Wh + yearly kWh. | `pvmaps._pv_calc`, `_apply_wind/spectral_corrections`, `_get_annual_rasters` (`pvmaps.py:486–557`) |

Everything else is plumbing that disappears with GRASS:

- `grass_gis_user.py` — env, subprocess, gisrc, mapsets, threaded command runner.
- `pvmaps_setup.py` — stages `pvgis_data.tar` (worldwide met rasters) into a 4326 GRASS DB,
  reprojects the UK subset into a 27700 GRASS DB, caches as `pvgis_data_uk.tar`. Exists
  **only** because the met data has to live inside GRASS.
- `pvmaps.py` — ~30 `r.mapcalc`/`r.*` shell calls: mask null-fix, slope/aspect calc,
  compass↔GRASS aspect conversion, flat-roof overrides, horizon clamp, PV calc, wind/
  spectral corrections, annual sum, raster import/export.
- `aggregate_pixel_results.py` — reads exported rasters back **out of Postgis**
  (GeoTIFF → `rasters_to_postgis` → `pixels_for_buildings`) to aggregate to roof planes.

The `r.pv` met inputs from `pvgis_data.tar` are just **UK-wide data rasters** we sample:
Linke turbidity `tl_0m_MM`, beam/diffuse clear-sky coefficients `kcb_MM`/`kcd_MM`,
3-hourly temperatures `t2m_avg_MM_HH` (+ `t_gradient`, `t_offset_f_era`),
`windeffect_MM`, `spectraleffect_{cSi,CdTe}_MM`. **None needs GRASS.**

## The contract to reproduce

The port is a drop-in replacement for `pvgis(...)` (`model_solar_pv.py:146`). The natural
seam is the per-pixel quantities `aggregate_pixel_results` already consumes:

- `kwh_year` — 1 kWp-equivalent yearly kWh, per pixel.
- `month_01_wh` … `month_12_wh` — representative-day Wh, per pixel.
- `horizon_00` … `horizon_NN` — radians, per pixel.

Match at that seam and the whole downstream (`aggregate_pixel_results`,
`pv_roof_plane.horizon real[]`, reports, `PV_MODEL_VERSION`) is untouched in Phases 1–2.

## Target architecture

Replace `solar_pv/pvgis/{pvmaps,pvmaps_setup,grass_gis_user}.py` with:

```
solar_pv/pv/horizon.py       # port of r.horizonmask: DEM + mask -> per-pixel horizon profile
solar_pv/pv/irradiation.py   # port of r.sun clear-sky model (ESRA / Hofierka-Šúri)
solar_pv/pv/pv_model.py      # r.pv PV layer: module temp + poly efficiency + wind/spectral
solar_pv/pv/met_data.py      # sample the 27700 met GeoTIFFs (from pvgis_data_uk.tar)
solar_pv/pv/run_pv.py        # orchestration replacing pvgis()
solar_pv/pv/golden/          # Phase 0 validation harness (see below)
```

Notes:

- **Met data** ships as pre-extracted **27700 GeoTIFFs** — precisely the contents of the
  `pvgis_data_uk.tar` already provided (396 members). Sampled per-job with numpy/GDAL.
  Deletes `pvmaps_setup.py` and the `PVGIS_GRASS_DBASE_DIR` env var; changes the form of
  the `pvgis_data.tar` dependency to "a directory of GeoTIFFs".
- **Reuse**: solar declination is already ported (`pvmaps._calc_solar_declination`,
  `pvmaps.py:255`); monthly representative days are `_monthly_pv_time_steps`
  (`pvmaps.py:274`). Aspect/slope convention conversions (`pvmaps.py:442`) and flat-roof
  overrides (`pvmaps.py:446`) become plain numpy; GDAL-computed slope/aspect are already
  fed in (`pvgis.py:108`).
- **Parallelism**: vectorise across pixels with numpy; outer loop is ~12 days × ~96
  timesteps. Keep an `mp` pool at the per-tile/per-building level if needed, instead of
  GRASS's per-raster threads.

## Validation strategy (Phase 0 is load-bearing)

**There is currently no frozen PVMAPS oracle in the repo.** `testdata/pvmaps/outputs`
(the `hpv_wind_spectral_*` / `horizon090_*` golden rasters) is **gitignored** and
regenerated on each GRASS test run. The only committed references are gdaldem slope/aspect
(`test_pvmaps_real_data/expected/`) and the cached PVGIS API (`api_real_pv_output.pkl`,
1024 `([12×E_d], E_year)` tuples) — which has diverged from PVMAPS.

So the **first task is to generate and *freeze* PVMAPS golden rasters** for a set of
validation areas, while GRASS still builds. Losing the ability to run PVMAPS before this
is done means losing the only oracle for the 1–2% target.

Three tiers of check, tightest first:

1. **Port vs frozen PVMAPS golden rasters** (pass/fail, ≤~1–2% on yearly kWh; a companion
   tolerance on monthly Wh and on horizon angle in degrees). Primary gate.
2. **Component checks** — horizon rasters vs frozen `horizon090_*`; optionally beam/diffuse
   intermediate rasters. Localises regressions.
3. **PVGIS API shape check** (loose, secondary) — reuse the sample points +
   `api_real_pv_output.pkl` machinery in `test_pvmaps.py` to confirm the port stays in the
   right ballpark and tracks seasonality/orientation. Not a gate.

Validation areas already have committed inputs (elevation, mask, flat-roof overrides,
sample locations): `test_pvmaps_real_data` and `test_pvmaps_real_data_{alnwick,alnwick_flat,thurso}`.
These span flat/hilly/urban/coastal and edge-of-met-coverage (thurso lacks spectral+wind
data), which is good coverage. Freeze goldens for all four.

## Phased plan

**Phase 0 — Freeze goldens + build the harness (do first, while GRASS builds).**
- Script to run the current `PVMaps` on the committed real-area inputs and freeze
  yearly/monthly/horizon output rasters into a committed golden directory (not the
  gitignored `outputs/`). Uses the provided `pvgis_data_uk.tar` so setup skips the slow
  world→UK conversion.
- Reusable comparison harness: raster %-diff (masked), API shape-diff (reuse
  `test_pvmaps.py` helpers), acceptance thresholds. Wired as a test that is skipped until
  `solar_pv.pv.run_pv` exists, then flips to the pass/fail gate.
- **Characterise** the natural port-vs-GRASS divergence early to confirm 1–2% is
  achievable before committing to Phase 2 in full.

**Phase 1 — Port A (horizon). ✅ DONE (2026-09-15).** `solar_pv/pv/horizon.py` +
`horizon_geo.py`, a faithful numpy port of `r.horizonmask` (nearest-neighbour ray march,
`length = hypot(di·ew_res, dj·ns_res)`, Earth-curvature drop, running-max slope, clamp
0–π/2). Takes a mask so only building-footprint pixels are evaluated (index-gather over the
masked origins, so a sparse mask is cheap), matching r.horizonmask; the full DEM is still
used as terrain. Validated against the frozen goldens (`solar_pv/pv/golden/horizon_check.py`,
`test_golden.HorizonPortTest`): **mean abs error ≤0.04°, p99 = 0° (>99% of pixels
bit-identical to GRASS)** across all four areas; the only differences are 0.02–0.13% of
pixels at near-obstruction diagonal nearest-neighbour ties (inherent to the algorithm; they
wash out in roof-plane averaging). Plus analytic unit tests in `solar_pv/pv/test_horizon.py`.

Things learned that Phase 2 depends on:
- r.horizon does **not** march along the nominal grid azimuth: it derives the direction via
  a geographic round-trip, so rays are rotated by the local grid convergence (up to ~1.3° at
  Thurso). `horizon_geo.grass_marching_vectors` reproduces this; ignoring it moves whole
  percent of pixels. r.pv/r.sun read the horizon rasters by nominal index, so the port keeps
  the same per-direction ordering.
- The committed test elevation and the goldens sit on grids offset by a **sub-pixel** shift
  (GRASS resamples the DEM onto its mask-zoomed region on import); the validation warps the
  elevation onto the golden grid first. Production rasters share one grid, so this is a
  fixture-only concern — but Phase 2's PV port must consume the *same* slope/aspect/horizon
  grid the goldens were computed on.
- The `360 % horizon_slices` truncation quirk (`pvmaps.py:59`) is gone for free — the port
  takes float direction steps.

**Phase 2 — Port B (irradiation + PV).** The hard, high-effort part. Full spec reverse-
engineered in [`docs/r-pv-algorithm.md`](r-pv-algorithm.md).
- **✅ r.pv core done (2026-09-15):** `solar_pv/pv/irradiation.py` + `pv_model.py` port the
  ESRA clear-sky model (beam/diffuse/reflected on the inclined roof with horizon shadowing,
  −a angle losses, real-sky kcb/kcd coefficients) plus the PVMAPS temperature/efficiency PV
  layer, integrated over the representative day. Validated against a frozen r.pv reference
  (`bin/capture_rpv_reference.py` → `solar_pv/pv/golden/rpv_check.py`,
  `test_golden.RpvCorePortTest`) on **identical** inputs: all 12 months of thurso match raw
  `hpv` to **mean <0.006%, 100% within 2%** (a few sub-0.1% shadow-boundary ties). The one
  subtle bug was the aspect convention — r.pv converts the raster to compass internally
  before the geometry transform (see the spec's gotcha).
- **✅ met sampling + assembly + end-to-end done (2026-09-16):** `solar_pv/pv/met_data.py`
  samples the met GeoTIFFs straight from `pvgis_data_uk.tar` via `/vsitar/` (nearest-neighbour,
  which reproduces GRASS's r.import resampling **exactly** — 0.00000 diff vs the captured met);
  `solar_pv/pv/run_pv.py` applies the wind + spectral corrections (gaps → 1.0) and sums
  months→year, and `compute_pv` is the whole-grid orchestrator. Validated end-to-end against
  the `kwh_year` golden for thurso (`test_golden.AnnualPortTest`): **mean 0.0007%, 100% within
  2%** with met sampled live from the tar (thurso does carry wind+spectral ≈1.05, so this
  exercises those paths). Self-contained tests (`test_run_pv.py`, `test_met_data.py`,
  `test_irradiation.py` regression fixture) survive removal of the golden scaffolding.
- **✅ slope/aspect correction done (2026-09-16):** `solar_pv/pv/slope_aspect.py` ports
  `_apply_slope_aspect_correction` (compass→grass conversion + flat-roof default + roof-plane
  slope/aspect override merge) as numpy `where` ops, feeding `compute_daily_pv` its GRASS-
  convention `aspect_adjusted`. Unit-tested (`test_slope_aspect.py`). Slope/aspect themselves
  stay on GDAL (see the decision above).

**Phase 2 is complete** as a numerical port: elevation/mask/slope/aspect/overrides + met →
per-pixel monthly Wh + yearly kWh, every stage validated against GRASS to <0.01% (or unit-
tested where it's pure logic). What remains is integration, below.

**Phase 3 — Remove the DB round-trip (the payoff). ▶ started (2026-09-16).**
- **✅ core primitive:** `solar_pv/pv/pixels.py` `pixels_for_geoms` extracts per-building
  pixels straight from the in-memory PV arrays (centre-in-polygon, shapely-vectorised),
  returning the exact `{toid: [pixel dict]}` shape `aggregate_pixel_results` consumes — the
  in-process replacement for `postgis.pixels_for_buildings`. `run_pv.field_arrays` packages
  the compute output into the field dict it wants (kwh_year, month_NN_wh, horizon_NN). Both
  unit-tested (`test_pixels.py`, `test_run_pv.py`).
- **✅ injectable pixel source (2026-09-16):** `aggregate_pixel_results.aggregate_from_arrays`
  drives the unchanged `_aggregate_pixel_data` weighting math from in-memory `field_arrays`
  (via `pixels_for_geoms`) instead of `pixels_for_buildings`. Roof planes + building geoms
  still load from the DB (`_load_roof_planes` / new `_load_building_geoms`, same paged
  building selection). Runs single-process, extracting each page's pixels in the main process;
  sharing the arrays across a worker pool (memmap/shared memory) is deferred until benchmarked
  against a real large-area job. The DELETE + `pv_building` INSERT are factored into
  `_delete_existing_results` / `_insert_pv_buildings`, shared with the old GRASS path.
  Validated DB-free by rasterising the committed pixel-aggregation fixture and re-extracting it
  by geometry — identical roof-plane `kwh_year_avg` + horizon (`test_aggregate_pixel_results.
  AggregateFromArraysTest`).
- **✅ top-level orchestrator (2026-09-16):** `run_pv.run_pv()` replaces `pvgis()` end-to-end
  with no GRASS and no Postgis raster round-trip: read elevation/slope/aspect/mask + build the
  DB overrides → patch elevation → horizon (Phase 1, convergence-corrected vectors) →
  slope/aspect correction (Phase 2) → `compute_pv_fields` (lat/lon + met + `compute_pv` +
  `field_arrays`) → `aggregate_from_arrays`. Mask + override rasters are warped onto the
  elevation grid (`_read_on_grid`), reproducing GRASS's region-zoom co-registration. Met now
  comes from `pvgis_data_uk.tar` (no `PVGIS_GRASS_DBASE_DIR`). `compute_pv_fields` is validated
  against the `kwh_year` golden through the full assembly (`test_golden.FieldsAnnualPortTest`,
  <2% all four areas) and its wiring unit-tested (`test_run_pv.ComputePvFieldsTest`).
- **✅ wired into `model_solar_pv` (2026-09-16):** the `pvgis()` call is swapped for `run_pv()`.
  The old GRASS `pvgis()` / `pvmaps` stay in the tree for the validation diff and are removed in
  Phase 4.
- **✅ memory: sparse fields (2026-09-16):** the per-pixel PV fields exist only at the
  building-footprint pixels, a small fraction of a job grid, so materialising them full-grid was
  untenable (~35 GB for a 5 km job at 1 m, dominated by the `(rows, cols, n_dir)` horizon).
  `horizon.compute_horizons_flat`, `run_pv.compute_pv_flat` and `solar_pv/pv/pixels.PixelFields`
  carry only the N valid pixels (flat arrays + a searchsorted coordinate lookup) end-to-end;
  `run_pv` never builds a full-grid field, and `field_arrays` stores float32. Peak drops to a few
  GB, scaling with building count not the bbox. `compute_pv`/`compute_horizons` keep a dense form
  for the small-grid validation. (Remaining transient: the met layers are still sampled full-grid
  per month — a later lever if needed.)
- **✅ aggregation paginated + parallel (2026-09-16):** `aggregate_from_arrays` pages over
  buildings (bounding the DB result + working set); the main process does the DB reads and
  per-page pixel extraction (which needs the in-memory fields), and a worker pool runs the
  CPU-heavy `_aggregate_pixel_data`, with at most ~2×workers pages in flight so memory stays
  bounded. Matches the GRASS path's parallelism for large jobs (500k+ buildings). Worker/page
  tuning wants a real-job profile.
- **✅ mask read without resampling (2026-09-16):** the buffered building mask shares the
  elevation grid's pixel phase and resolution (rasterised with `gdal_rasterize -tap`; elevation
  is warped onto `mask_buf0`'s grid), differing only in extent, so it is cropped by integer pixel
  offset (`_read_cropped`) rather than warped.
- **✅ acceptance gate passed (2026-09-16):** a real job area was run through both paths (GRASS
  job 677, no-GRASS job 678, same bounds) and the `pv_roof_plane` outputs diffed. Roof detection
  (RANSAC) is only ~96% deterministic between runs, so the comparison was restricted to the
  1173 **geometrically-identical** roofs (same slope/aspect/area) to isolate the PV model:

  | Level | no-GRASS vs GRASS |
  |---|---|
  | Portfolio total Σ kwh_year_avg (geom-identical) | **−0.08%** |
  | Portfolio total (entire file, incl. roof-detection noise) | −0.07% |
  | Per-roof kwh_year_avg | mean **0.81%**, median 0.52%, p95 2.6%, p99 4.1%, max 9.1% |
  | Horizon angle | mean 0.14°, p99 2.3° |

  Meets the 1–2% target with **no systematic bias** (signed total ≈ 0). The per-roof tail is a
  roof-*size* effect, not a model error: the >2% roofs are small (median 14.8 m² vs 17.9), and
  the spread falls monotonically with area (0–10 m²: 1.0% mean; 50 m²+: 0.28%) — the signature of
  the pixel→roof averaging under the pixel-selection change (`ST_Clip` centroids → centre-in-
  polygon), which washes out at portfolio level. Winter months show larger % (Dec 3.2% mean) from
  low-sun percentage amplification but barely move the annual. For a fully confound-free re-check
  before/after any future change, run both PV models over the **same** `roof_polygons` (skip
  re-detection) to remove the RANSAC noise.

**Phase 4 — Delete GRASS + toolchain. ✅ DONE (2026-09-16).** Deleted `grass_gis_user.py`,
`pvmaps.py`, `pvmaps_setup.py`, `pvgis.py` (the old GRASS orchestrator), `nix/grass-8.2.0-pvmaps.nix`
+ its patches/`proj-4.9.3.nix`, the GRASS-oracle capture scripts (`bin/capture_*`,
`bin/validate_pvmaps.py`), and the `test_pvmaps/` suite. Removed the `pkgs2205` 22.05 pin +
`buildGrass` from `default.nix` and the GRASS package + `PVGIS_GRASS_DBASE_DIR` from the webapp's
`server/machine-configuration.nix`, and the dead GRASS aggregation functions from
`aggregate_pixel_results.py`. Moved `aggregate_pixel_results.py` into `solar_pv/pv/` and removed
the now-empty `solar_pv/pvgis/` package; deleted the golden-validation harness
(`solar_pv/pv/golden/`, `testdata/pvmaps/`, `grass_modules/`, the `bin/` re-write helpers) now the
port is validated (the self-contained `test_irradiation` regression fixture remains as the in-repo
oracle). Updated both `CLAUDE.md`s and the READMEs. The `pixels_for_buildings` /
`rasters_to_postgis` / `create_raster_table` helpers stay (still used by roof detection and raster
loading). All tests pass with no GRASS; `nix-shell` builds without the 22.05 pin.

**`PV_MODEL_VERSION` stays 2**: it is a webapp gate about the `pv_roof_plane` *output schema*
(which is unchanged), not the algorithm, so cost-benefit seeding/reports still work across the
GRASS→native switch. (The original plan called for a bump; on review that was the wrong lever.)

Ops note: `PVGIS_DATA_TAR_FILE_DIR` must now contain `pvgis_data_uk.tar` (the reprojected UK
subset) rather than `pvgis_data.tar`; `PVGIS_GRASS_DBASE_DIR` is no longer read.

## Risks & effort

- **r.sun port accuracy (Phase 2) dominates.** Well-documented, fully vectorisable, but
  getting beam/diffuse/reflected + the PVMAPS temperature/efficiency layer to agree to
  1–2% is iterative. Budget most of the project here.
- **Hitting 1–2% vs a frozen, quirky C implementation** may surface undocumented PVMAPS
  behaviours (e.g. the flat-roof aspect/slope overrides, the wind/spectral null-defaulting
  to 1.0 in `pvmaps.py:521/531`, the `r.pv` return-code-1-on-success quirk). Component
  checks (tier 2) are there to localise these.
- **Performance**: GRASS is C + multiprocessed for a reason. Spike a large-area job early
  in Phase 2 before committing to the numpy approach.
- **Met-data provenance**: one-time dependency on the UK 27700 GeoTIFFs. The provided
  `pvgis_data_uk.tar` matches what `pvmaps_setup._transfer_raster` produces (4326→27700 via
  the 7-param shift, `pvmaps_setup.py:162`); script the extraction reproducibly.

## Open questions

- Where do frozen goldens live? Committing ~1 MB/area of GeoTIFF is fine; a larger set may
  want git-lfs or a data bucket. (Current `outputs/` is gitignored precisely to avoid this.)
- Do any reports/consumers read `pv_roof_plane.horizon` in a way that constrains the
  horizon output format/precision beyond what aggregation needs? (Not found in webapp `src/`
  so far.)
