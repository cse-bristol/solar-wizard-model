# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
"""
Sample the meteorological inputs (Linke turbidity, real-sky beam/diffuse coefficients, 3-hourly
air temperature, wind and spectral corrections) for a job's raster grid. The data are UK-wide
EPSG:27700 GeoTIFFs inside pvgis_data_uk.tar (~1.6 km cells); GDAL reads them in place via
/vsitar/.
"""
from dataclasses import dataclass

import numpy as np
from osgeo import gdal

gdal.UseExceptions()

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
    """Samples pvgis_data_uk.tar onto a fixed job grid (geotransform + shape)."""

    def __init__(self, tar_path: str, gt, shape, panel: str = "cSi",
                 resample: str = "near"):
        self._tar = tar_path
        self._gt = gt
        self._shape = shape
        self._panel = panel
        self._resample = resample

    def _sample(self, name: str, idx=None, required: bool = True):
        """Warp one met layer onto the job grid and return it, indexed to `idx` (a (rows, cols)
        pixel selection) when given so no full-grid array outlives the warp. `required=False`
        layers absent from the tar return None (the Warp raises on a missing source)."""
        rows, cols = self._shape
        gt = self._gt
        src = f"/vsitar/{self._tar}/{name}{_SUFFIX}"
        bounds = (gt[0], gt[3] + gt[5] * rows, gt[0] + gt[1] * cols, gt[3])
        try:
            ds = gdal.Warp("", src, format="MEM", xRes=abs(gt[1]), yRes=abs(gt[5]),
                           resampleAlg=self._resample, outputBounds=bounds, dstSRS="EPSG:27700")
        except RuntimeError:
            if required:
                raise
            return None
        band = ds.GetRasterBand(1)
        arr = band.ReadAsArray().astype(np.float64)
        nodata = band.GetNoDataValue()
        if nodata is not None and not np.isnan(nodata):
            arr = np.where(arr == nodata, np.nan, arr)
        return arr if idx is None else arr[idx]

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
