# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
"""
The native-Python PV model: from a job's rasters (elevation, mask, slope/aspect + overrides)
and the meteorological inputs, compute per-pixel solar PV generation and aggregate it to roof
planes, with no GRASS and no raster round-trip through Postgis.

`run_pv` is the top-level orchestrator. Below it, the r.pv core (solar_pv.pv.irradiation) runs
for each representative month; `monthly_wh_and_annual` applies the wind and spectral corrections
and sums to a yearly total (the r.pv + wind/spectral + annual-sum steps of the PVMAPS pipeline);
`compute_pv_flat`/`compute_pv_fields` drive the whole grid.
"""
import math
import os
from typing import Dict, List, Optional, Sequence, Tuple
import logging

import numpy as np
from osgeo import gdal, osr

from solar_pv.pv import irradiation as ir
from solar_pv.pv.pixels import PixelFields
from solar_pv import stage
from solar_pv.constants import SYSTEM_LOSS
from solar_pv.paths import RESOURCES_DIR
from solar_pv.pv import horizon as hz, horizon_geo, slope_aspect
from solar_pv.pv.met_data import MetData
from solar_pv.pv.aggregate_pixel_results import aggregate_from_arrays
from solar_pv.rasters import (create_elevation_override_raster,
                                generate_aspect_override_raster,
                                generate_slope_override_raster)

gdal.UseExceptions()
osr.UseExceptions()

# a GDAL geotransform (ox, ew_res, row_skew, oy, col_skew, ns_res):
GeoTransform = Tuple[float, float, float, float, float, float]

# (index, representative day-of-year, month, days-in-month) — matches PVMAPS
# _monthly_pv_time_steps() / _get_annual_rasters (num_days weighting):
MONTHLY_STEPS = [
    (0, 17, 1, 31), (1, 46, 2, 28), (2, 75, 3, 31), (3, 105, 4, 30),
    (4, 135, 5, 31), (5, 162, 6, 30), (6, 198, 7, 31), (7, 228, 8, 31),
    (8, 259, 9, 30), (9, 289, 10, 31), (10, 319, 11, 30), (11, 345, 12, 31),
]


def solar_declination(day: int) -> float:
    """Solar declination (radians) in r.pv's sign convention, ready to pass to
    compute_daily_pv (the negative of PVMAPS's _calc_solar_declination)."""
    d1 = 2.0 * math.pi * day / 365.25
    return -math.asin(0.3978 * math.sin(d1 - 1.4 + 0.0355 * math.sin(d1 - 0.0489)))


def patch_elevation(elevation: np.ndarray, override: np.ndarray) -> np.ndarray:
    """Merge a building-height override into the elevation (both full-grid, NaN = nodata): take
    the max where both are present (so a modelled height below the LiDAR keeps the LiDAR), else
    whichever is present. np.fmax ignores NaN, giving exactly that."""
    return np.fmax(override, elevation)


def _correction(factor: Optional[np.ndarray], shape: Tuple[int, ...]) -> np.ndarray:
    """A wind/spectral correction array, with nodata/NaN -> 1.0 (PVMAPS defaults coverage
    gaps to 1.0), or all-ones when the layer is absent for this area."""
    if factor is None:
        return np.ones(shape)
    factor = np.asarray(factor, dtype=np.float64)
    return np.where(np.isfinite(factor), factor, 1.0)


def monthly_wh_and_annual(monthly_hpv: Sequence[np.ndarray],
                          monthly_wind: Optional[Sequence[Optional[np.ndarray]]] = None,
                          monthly_spectral: Optional[Sequence[Optional[np.ndarray]]] = None
                          ) -> Tuple[List[np.ndarray], np.ndarray]:
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


def field_arrays(kwh_year: np.ndarray, monthly_wh: Sequence[np.ndarray],
                 horizon: np.ndarray) -> Dict[str, np.ndarray]:
    """Package the per-pixel PV outputs into the {field: array} dict the roof-plane aggregation
    consumes, keyed and ordered as kwh_year, month_01_wh..month_12_wh, horizon_00..horizon_NN.
    Works on either the full grid (`horizon` (rows, cols, n_dir)) or the flat valid-pixel form
    (`horizon` (N, n_dir)). Values are cast to float32 — kWh/radian outputs don't need float64,
    and it halves the stored size."""
    out = {"kwh_year": np.asarray(kwh_year, dtype=np.float32)}
    for i, wh in enumerate(monthly_wh):
        out[f"month_{i + 1:02d}_wh"] = np.asarray(wh, dtype=np.float32)
    for d in range(horizon.shape[-1]):
        out[f"horizon_{d:02d}"] = np.asarray(horizon[..., d], dtype=np.float32)
    return out


def compute_pv_flat(rows: np.ndarray, cols: np.ndarray,
                    slope_deg: np.ndarray, aspect_compass_deg: np.ndarray, elevation: np.ndarray,
                    horizon: np.ndarray, geotransform: GeoTransform, horizon_step_deg: float,
                    met, coeffs: Sequence[float], albedo: float = 0.2
                    ) -> Tuple[List[np.ndarray], np.ndarray]:
    """
    r.pv-to-annual pipeline evaluated only at the given valid pixels (no full-grid arrays). All
    per-pixel inputs are flat (N,), except `horizon` which is (N, n_dir); `aspect_compass_deg`
    is compass (0 = N / flat). `rows`/`cols` are the pixels' grid indices, used to sample met
    and lat/lon. Returns (monthly_wh, kwh_year) as flat (N,) arrays.
    """
    idx = (np.asarray(rows), np.asarray(cols))
    lat, lon = _latlon_at(geotransform, idx[0], idx[1])

    monthly_hpv, monthly_wind, monthly_spectral = [], [], []
    for _, day, month, _ in MONTHLY_STEPS:
        m = met.for_month(month, idx)
        monthly_hpv.append(ir.compute_daily_pv(
            slope_deg, aspect_compass_deg, elevation, lat, lon,
            horizon, horizon_step_deg,
            m.linke, m.cbh, m.cdh, m.temps8, albedo,
            day, solar_declination(day), coeffs))
        monthly_wind.append(m.wind)
        monthly_spectral.append(m.spectral)

    return monthly_wh_and_annual(monthly_hpv, monthly_wind, monthly_spectral)


def _latlon_at(geotransform: GeoTransform, rows: np.ndarray,
               cols: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
    """Latitude and longitude (radians) at the given pixel row/col indices of a north-up
    EPSG:27700 grid. Transforms only those pixels, not the whole grid."""
    rows = np.asarray(rows)
    cols = np.asarray(cols)
    if rows.size == 0:
        return np.empty(0), np.empty(0)
    x = geotransform[0] + (cols + 0.5) * geotransform[1]
    y = geotransform[3] + (rows + 0.5) * geotransform[5]
    src = osr.SpatialReference(); src.ImportFromEPSG(27700)
    dst = osr.SpatialReference(); dst.ImportFromEPSG(4326)
    src.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    dst.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    pts = np.array(osr.CoordinateTransformation(src, dst).TransformPoints(
        np.column_stack([x, y]).tolist()))
    return np.radians(pts[:, 1]), np.radians(pts[:, 0])


def compute_pv_fields(slope_adjusted: np.ndarray, aspect_adjusted: np.ndarray,
                      elevation: np.ndarray, horizon: np.ndarray,
                      geotransform: GeoTransform, met, coeffs: Sequence[float],
                      horizon_step_deg: float, valid: Optional[np.ndarray] = None,
                      albedo: float = 0.2) -> PixelFields:
    """
    Assemble the sparse per-pixel PV fields from full-grid corrected slope/aspect, elevation and
    horizon — the seam pixels_for_geoms/aggregate_from_arrays consumes. For small grids
    (validation); run_pv builds the same PixelFields on a real job grid without densifying.

    :param slope_adjusted: (rows, cols) slope (degrees), after the flat-roof/override merge.
    :param aspect_adjusted: (rows, cols) compass aspect (degrees, 0 = N / flat), after the merge.
    :param elevation: (rows, cols) elevation (m).
    :param horizon: (rows, cols, n_dir) horizon heights (radians).
    :param geotransform: GDAL geotransform of the grid.
    :param met: solar_pv.pv.met_data.MetData bound to this grid.
    :param coeffs: 8 PV-model constants (a <panel>.coeffs file).
    :param horizon_step_deg: degrees between horizon directions.
    :param valid: optional boolean mask of pixels to evaluate.
    :return: a PixelFields (kwh_year, month_01_wh..month_12_wh, horizon_00..NN at valid pixels).
    """
    shape = np.asarray(elevation).shape
    if valid is None:
        valid = (np.isfinite(slope_adjusted) & np.isfinite(aspect_adjusted)
                 & np.isfinite(elevation) & np.all(np.isfinite(horizon), axis=-1))
    idx = np.nonzero(valid)
    monthly_wh, kwh_year = compute_pv_flat(
        idx[0], idx[1], slope_adjusted[idx], aspect_adjusted[idx], elevation[idx],
        horizon[idx], geotransform, horizon_step_deg, met, coeffs, albedo)
    values = field_arrays(kwh_year, monthly_wh, horizon[idx])
    return PixelFields(geotransform, shape, idx[0], idx[1], values)


# pv_tech (the model param / PVGIS name) -> the panel key, which is both the <panel>.coeffs
# stem and the spectraleffect_<panel>_ met raster suffix (case matters):
_PANEL_FOR_TECH = {"crystSi": "cSi", "CdTe": "CdTe"}


def _load_coeffs(panel: str, resources_dir: str) -> List[float]:
    """Read the 8 PV-model constants from resources/<panel>.coeffs."""
    path = os.path.join(resources_dir, f"{panel.lower()}.coeffs")
    with open(path) as f:
        return [float(line) for line in f if line.strip()]


def _read_raster(path: str) -> Tuple[np.ndarray, GeoTransform, Tuple[int, int]]:
    """Read a single-band raster to (array float64 with nodata->NaN, geotransform, shape)."""
    ds = gdal.Open(path)
    band = ds.GetRasterBand(1)
    arr = band.ReadAsArray().astype(np.float64)
    nodata = band.GetNoDataValue()
    if nodata is not None and not np.isnan(nodata):
        arr = np.where(arr == nodata, np.nan, arr)
    return arr, ds.GetGeoTransform(), arr.shape


def _read_aligned(path: str, ref_shape: Tuple[int, int]) -> np.ndarray:
    """Read a raster already co-registered to the reference grid, asserting its shape matches
    (slope/aspect derive from the elevation grid; the overrides are rasterised onto it)."""
    arr = _read_raster(path)[0]
    if arr.shape != ref_shape:
        raise ValueError(f"{path} shape {arr.shape} != grid {ref_shape}; expected co-registered")
    return arr


def _read_cropped(path: str, ref_gt: GeoTransform, ref_shape: Tuple[int, int]) -> np.ndarray:
    """Read a raster that shares the reference grid's pixel phase and resolution but not its
    extent (e.g. the buffered building mask vs the elevation window), cropped to the reference
    grid by integer pixel offset. Exact — no resampling; out-of-source cells come back NaN."""
    arr, gt, _ = _read_raster(path)
    if abs(gt[1] - ref_gt[1]) > 1e-6 or abs(gt[5] - ref_gt[5]) > 1e-6:
        raise ValueError(f"{path} resolution {(gt[1], gt[5])} != grid {(ref_gt[1], ref_gt[5])}")
    col_off = int(round((ref_gt[0] - gt[0]) / gt[1]))
    row_off = int(round((ref_gt[3] - gt[3]) / gt[5]))
    rows, cols = ref_shape
    out = np.full(ref_shape, np.nan)
    sr0, sc0 = max(0, row_off), max(0, col_off)
    dr0, dc0 = max(0, -row_off), max(0, -col_off)
    rh = min(arr.shape[0] - sr0, rows - dr0)
    cw = min(arr.shape[1] - sc0, cols - dc0)
    if rh > 0 and cw > 0:
        out[dr0:dr0 + rh, dc0:dc0 + cw] = arr[sr0:sr0 + rh, sc0:sc0 + cw]
    return out


def run_pv(pg_uri: str,
           job_id: int,
           solar_dir: str,
           resolution_metres: float,
           pv_tech: str,
           horizon_search_radius: int,
           horizon_slices: int,
           peak_power_per_m2: float,
           elevation_raster: str,
           mask_raster: str,
           slope_raster: str,
           aspect_raster: str,
           met_tar: str,
           debug_mode: bool = False) -> None:
    """
    Compute per-pixel PV entirely in-process (horizon + slope/aspect correction + irradiation/PV
    + met) and aggregate to roof planes, with no GRASS and no raster round-trip through Postgis.

    Slope/aspect stay on GDAL (produced by generate_rasters); the roof-plane and building-height
    overrides are still built from the DB.
    """

    panel = _PANEL_FOR_TECH.get(pv_tech)
    if panel is None:
        raise ValueError(f"Unsupported panel type '{pv_tech}'")
    coeffs = _load_coeffs(panel, RESOURCES_DIR)
    horizon_step_deg = 360.0 / horizon_slices

    # Elevation is the reference grid, and the terrain for horizon, so it stays full-grid.
    # slope/aspect derive from it and share it; the overrides are rasterised straight onto it
    # (grid_bounds) so they read back aligned; the mask shares the pixel phase but has a wider
    # extent, so it is cropped (not resampled) onto the grid. Everything except elevation and
    # mask is indexed to the building pixels as soon
    # as it is read, so no full-grid PV field arrays are ever built (the fields would be tens of
    # GB at 1 m over a large job; sparse keeps it to the footprint pixels).
    elevation, gt, shape = _read_raster(elevation_raster)
    grid_bounds = (gt[0], gt[3] + gt[5] * shape[0], gt[0] + gt[1] * shape[1], gt[3])

    logging.info("Generating override rasters...")
    elevation_override_raster = create_elevation_override_raster(
        pg_uri=pg_uri, job_id=job_id, solar_dir=solar_dir,
        elevation_raster_27700_filename=elevation_raster, bounds=grid_bounds)
    aspect_override_raster = generate_aspect_override_raster(
        pg_uri=pg_uri, job_id=job_id, solar_dir=solar_dir,
        mask_raster_27700_filename=mask_raster, bounds=grid_bounds)
    slope_override_raster = generate_slope_override_raster(
        pg_uri=pg_uri, job_id=job_id, solar_dir=solar_dir,
        mask_raster_27700_filename=mask_raster, bounds=grid_bounds)

    # Patched elevation feeds both horizon and PV (see patch_elevation):
    if elevation_override_raster:
        elevation = patch_elevation(elevation, _read_aligned(elevation_override_raster, shape))

    mask_bool = np.nan_to_num(_read_cropped(mask_raster, gt, shape)) != 0

    logging.info("Computing horizon profiles...")
    directions = hz.grass_directions(horizon_step_deg)
    vectors = horizon_geo.grass_marching_vectors(gt, shape, directions)
    # horizon only at the building pixels (hrow, hcol); (M, n_dir), never a full grid:
    horizon, hrow, hcol = hz.compute_horizons_flat(
        elevation, abs(gt[1]), abs(gt[5]), vectors, float(horizon_search_radius), mask=mask_bool)

    logging.info("Applying slope/aspect correction...")
    # index slope/aspect/overrides to the building pixels as they are read (the full arrays are
    # transient); apply_correction then works on the flat (M,) arrays:
    slope_adjusted, aspect_adjusted = slope_aspect.apply_correction(
        slope_deg=_read_aligned(slope_raster, shape)[hrow, hcol],
        aspect_compass_deg=_read_aligned(aspect_raster, shape)[hrow, hcol],
        aspect_override_compass_deg=_read_aligned(aspect_override_raster, shape)[hrow, hcol],
        slope_override_deg=_read_aligned(slope_override_raster, shape)[hrow, hcol])
    elev = elevation[hrow, hcol]

    # drop building pixels missing slope/aspect/elevation (horizon is finite by construction):
    keep = (np.isfinite(slope_adjusted) & np.isfinite(aspect_adjusted) & np.isfinite(elev)
            & np.all(np.isfinite(horizon), axis=-1))
    prows, pcols = hrow[keep], hcol[keep]

    logging.info("Computing PV...")
    met = MetData(met_tar, gt, shape, panel=panel)
    monthly_wh, kwh_year = compute_pv_flat(
        prows, pcols, slope_adjusted[keep], aspect_adjusted[keep], elev[keep], horizon[keep],
        gt, horizon_step_deg, met, coeffs)
    fields = PixelFields(gt, shape, prows, pcols,
                         field_arrays(kwh_year, monthly_wh, horizon[keep]))

    logging.info("Aggregating pixel-level results to roof planes...")
    aggregate_from_arrays(pg_uri=pg_uri, job_id=job_id, pixel_fields=fields,
                          resolution=resolution_metres,
                          peak_power_per_m2=peak_power_per_m2, system_loss=SYSTEM_LOSS)

    stage.set_stage(pg_uri, job_id, stage.Stage.PVGIS)
