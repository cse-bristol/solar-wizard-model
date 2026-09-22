# e2e test fixtures

Fixtures for `solar_pv/test_e2e.py`, a ~330-building patch of central Bristol.

- `buildings.gpkg` — OSM building footprints (`osm_id` field), EPSG:27700.
- `lidar.tif` — the 1m DSM tile covering them, cropped to the building extent plus a
  300m margin (the test's `horizon_search_radius`).
- `pvgis_data_uk.tar` — a cut-down met-data tar so the test runs without the ~640MB
  UK-wide `pvgis_data_uk.tar`. It holds only the layers `MetData.for_month` reads
  (air temp, Linke turbidity, real-sky beam/diffuse coefficients, wind and both
  panels' spectral correction), each cropped to a window around the fixture extent.
  `MetData` warps every layer onto the job grid by coordinate, so a layer that fully
  covers the grid bounds yields the same result as the full-UK original.

## Regenerating the met tar

Needs the full `pvgis_data_uk.tar` and the nix-shell (for GDAL):

```shell
nix-shell --run "python3 bin/make_cutdown_met_tar.py /path/to/pvgis_data_uk.tar"
```

Widen `BUFFER_M`, or update `LIDAR`, in `bin/make_cutdown_met_tar.py` if the fixture
extent changes.
