# PV port golden validation (Phase 0)

Validation harness for replacing GRASS/PVMAPS with the native-Python PV model.
Full plan: [`docs/pv-grass-removal-plan.md`](../../../docs/pv-grass-removal-plan.md).

## Why this exists

The tight target (≤~1–2% on yearly kWh) is measured against the **frozen GRASS/PVMAPS
output**, because PVMAPS/`r.pv` is frozen and the PVGIS web API has diverged from it. The
API is only a loose *shape* check.

`testdata/pvmaps/outputs` (the GRASS golden rasters) is **gitignored** and regenerated on
each test run, so there is no frozen oracle in the repo. Before GRASS is removed we must
capture and commit goldens.

## Layout

- `fixtures.py` — the validation `AREAS` (flat/hilly/urban/coastal + edge-of-met-coverage),
  their committed inputs, and loaders for the cached PVGIS API ground truth.
- `compare.py` — `raster_diff()`/`array_diff()` (masked per-pixel %/abs stats),
  `point_year_diff()`, and geotiff read + grid-alignment helpers.
- `horizon_check.py` — Phase 1: warps the elevation onto the golden grid, runs
  `solar_pv.pv.horizon`, and reports per-area angle-error stats.
- `test_golden.py` — fixture-sanity tests, `HorizonPortTest` (Phase 1 gate, active), and
  `PortAcceptanceTest` (Phase 2 PV gate, skipped until `solar_pv.pv.run_pv` exists).
- `../../../bin/capture_pvmaps_goldens.py` — runs PVMAPS and freezes goldens into
  `testdata/pvmaps/goldens/<area>/` as `kwh_year.tif`, `month_NN_wh.tif`, `horizon_NN.tif`.
- `../../../bin/check_horizon_port.py` — dev diagnostic: per-direction horizon error vs the
  goldens (`--nominal` to compare without the grid-convergence correction).

## Usage

Everything below needs `nix-shell` (numpy/GDAL need its `libstdc++`; capture needs GRASS +
the PVMAPS addons).

Fixture sanity + the Phase 1 horizon gate (no GRASS needed — the goldens are already frozen,
so set `NIX_BUILD_GRASS=false` to skip the slow build):

```shell
NIX_BUILD_GRASS=false nix-shell --run \
  "python -m unittest solar_pv.pv.golden.test_golden solar_pv.pv.test_horizon"
```

Freeze goldens (needs GRASS + PVMAPS; do this while they still build):

```shell
nix-shell --run "python bin/capture_pvmaps_goldens.py --area all"
```

Once `solar_pv/pv/run_pv.py` and the goldens exist, un-skip `PortAcceptanceTest` in
`test_golden.py` — it becomes the project's pass/fail gate.
