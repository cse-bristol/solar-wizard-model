# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
"""
Sample the meteorological inputs (Linke turbidity, real-sky beam/diffuse coefficients, 3-hourly
air temperature, wind and spectral corrections) for a job's raster grid. The data are UK-wide
EPSG:27700 GeoTIFFs inside pvgis_data_uk.tar (~1.6 km x 2.5 km cells); GDAL reads them in place
via /vsitar/.
"""
from dataclasses import dataclass
from typing import Tuple

import numpy as np
from osgeo import gdal

gdal.UseExceptions()

GeoTransform = Tuple[float, float, float, float, float, float]

# suffix inside pvgis_data_uk.tar (rasters are named e.g. tl_0m_06.27700.tif):
_SUFFIX = ".27700.tif"


@dataclass
class MonthMet:
    """Per-month met arrays for a pixel selection (temps8 keeps a trailing 8-slot axis; wind/
    spectral may be None where the layer has no coverage -> the assembly defaults them to 1.0).
    Flat (N,)/(N, 8) when for_month was given pixel indices, else full-grid (rows, cols)/(…, 8)."""
    linke: np.ndarray
    cbh: np.ndarray
    cdh: np.ndarray
    temps8: np.ndarray
    wind: np.ndarray
    spectral: np.ndarray


class MetData:
    """Samples pvgis_data_uk.tar onto a fixed job grid (geotransform + shape).

    Nearest-neighbour: each job pixel takes the met cell containing its centre. The met layers
    all share one grid. Its CRS is labelled as the 7-parameter Helmert approximation of
    EPSG:27700 (as in solar_pv.transformations) rather than EPSG:27700 itself; the grid is
    treated as EPSG:27700 regardless, as the 1-9 m datum difference is negligible against its
    ~1.6 x 2.5 km cells.
    """

    def __init__(self, tar_path: str, gt, shape, panel: str = "cSi"):
        self._tar = tar_path
        self._gt = gt
        self._shape = shape
        self._panel = panel
        # (idx, met cells) for the last pixel selection sampled:
        self._cells = None

    def _path(self, name: str) -> str:
        return f"/vsitar/{self._tar}/{name}{_SUFFIX}"

    def _source_cells(self, idx) -> Tuple[np.ndarray, np.ndarray, np.ndarray, GeoTransform]:
        """(met row, met col, in-extent mask, met geotransform) for the pixels `idx`, cached as
        every layer and month shares the met grid (callers pass the same idx for each month)."""
        if self._cells is not None and self._cells[0] is idx:
            return self._cells[1]
        if idx is None:
            rows, cols = (a.ravel() for a in np.indices(self._shape))
        else:
            rows, cols = np.asarray(idx[0]), np.asarray(idx[1])
        gt = self._gt
        x = gt[0] + (cols + 0.5) * gt[1]
        y = gt[3] + (rows + 0.5) * gt[5]

        ds = gdal.Open(self._path("tl_0m_01"))
        met_gt = ds.GetGeoTransform()
        met_col = np.floor((x - met_gt[0]) / met_gt[1]).astype(np.int64)
        met_row = np.floor((y - met_gt[3]) / met_gt[5]).astype(np.int64)
        inside = ((met_row >= 0) & (met_row < ds.RasterYSize)
                  & (met_col >= 0) & (met_col < ds.RasterXSize))
        self._cells = (idx, (met_row, met_col, inside, met_gt))
        return self._cells[1]

    def _sample(self, name: str, idx=None, required: bool = True):
        """Sample one met layer at the pixels `idx` (a (rows, cols) pixel selection) as a flat
        array, or at every pixel as a full (rows, cols) grid when idx is None. `required=False`
        layers absent from the tar return None."""
        try:
            ds = gdal.Open(self._path(name))
        except RuntimeError:
            if required:
                raise
            return None
        met_row, met_col, inside, met_gt = self._source_cells(idx)
        if ds.GetGeoTransform() != met_gt:
            raise ValueError(f"{name} is not on the same grid as the other met layers")
        band = ds.GetRasterBand(1)
        nodata = band.GetNoDataValue()
        # pixels outside the met extent get nodata, or 0 where the layer has none (as a warp):
        out = np.full(met_row.shape, np.nan if nodata is not None else 0.0)
        if inside.any():
            r, c = met_row[inside], met_col[inside]
            r0, c0 = int(r.min()), int(c.min())
            window = band.ReadAsArray(c0, r0, int(c.max()) - c0 + 1,
                                      int(r.max()) - r0 + 1).astype(np.float64)
            if nodata is not None and not np.isnan(nodata):
                window = np.where(window == nodata, np.nan, window)
            out[inside] = window[r - r0, c - c0]
        return out.reshape(self._shape) if idx is None else out

    def for_month(self, month: int, idx=None) -> MonthMet:
        """Sample every layer for `month`. Pass `idx` (the building pixels' (rows, cols)) on a
        real job so each layer is sliced to the footprint pixels as it is read, never held as a
        full grid; omit it (full grid) only for small validation grids."""
        mm = f"{month:02d}"
        temps8 = np.stack([self._sample(f"t2m_avg_{mm}_{hh:02d}", idx) for hh in range(0, 24, 3)],
                          axis=-1)
        return MonthMet(
            linke=self._sample(f"tl_0m_{mm}", idx),
            cbh=self._sample(f"kcb_{mm}", idx),
            cdh=self._sample(f"kcd_{mm}", idx),
            temps8=temps8,
            wind=self._sample(f"windeffect_{mm}", idx, required=False),
            spectral=self._sample(f"spectraleffect_{self._panel}_{mm}", idx, required=False))
