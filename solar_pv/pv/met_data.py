# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
"""
Sample the PVMAPS meteorological inputs (Linke turbidity, real-sky beam/diffuse coefficients,
3-hourly air temperature, wind and spectral corrections) for a job's raster grid, replacing
the GRASS raster database that pvmaps_setup built. The data are UK-wide EPSG:27700 GeoTIFFs
inside pvgis_data_uk.tar (~1.6 km cells); GDAL reads them in place via /vsitar/.

See docs/r-pv-algorithm.md for how each layer feeds r.pv.
"""
from dataclasses import dataclass

import numpy as np
from osgeo import gdal

gdal.UseExceptions()

# suffix inside pvgis_data_uk.tar (rasters are named e.g. tl_0m_06.27700.tif):
_SUFFIX = ".27700.tif"


@dataclass
class MonthMet:
    """Per-month met arrays on the job grid (temps8 is (rows, cols, 8); wind/spectral may be
    None where the layer has no coverage -> the assembly defaults them to 1.0)."""
    linke: np.ndarray
    cbh: np.ndarray
    cdh: np.ndarray
    temps8: np.ndarray
    wind: np.ndarray
    spectral: np.ndarray

    def at(self, idx) -> "MonthMet":
        """Index every layer to a pixel selection (temps8 keeps its 8-slot axis)."""
        return MonthMet(
            self.linke[idx], self.cbh[idx], self.cdh[idx], self.temps8[idx],
            None if self.wind is None else self.wind[idx],
            None if self.spectral is None else self.spectral[idx])


class MetData:
    """Samples pvgis_data_uk.tar onto a fixed job grid (geotransform + shape)."""

    def __init__(self, tar_path: str, gt, shape, panel: str = "cSi",
                 resample: str = "near"):
        self._tar = tar_path
        self._gt = gt
        self._shape = shape
        self._panel = panel
        self._resample = resample

    def _sample(self, name: str, required: bool = True):
        rows, cols = self._shape
        gt = self._gt
        src = f"/vsitar/{self._tar}/{name}{_SUFFIX}"
        try:
            gdal.Open(src)
        except RuntimeError:
            if required:
                raise
            return None
        bounds = (gt[0], gt[3] + gt[5] * rows, gt[0] + gt[1] * cols, gt[3])
        mem = "/vsimem/met_sample.tif"
        gdal.Warp(mem, src, xRes=abs(gt[1]), yRes=abs(gt[5]), resampleAlg=self._resample,
                  outputBounds=bounds, dstSRS="EPSG:27700")
        ds = gdal.Open(mem)
        band = ds.GetRasterBand(1)
        arr = band.ReadAsArray().astype(np.float64)
        nodata = band.GetNoDataValue()
        if nodata is not None and not np.isnan(nodata):
            arr = np.where(arr == nodata, np.nan, arr)
        ds = None
        gdal.Unlink(mem)
        return arr

    def for_month(self, month: int) -> MonthMet:
        mm = f"{month:02d}"
        temps8 = np.stack([self._sample(f"t2m_avg_{mm}_{hh:02d}") for hh in range(0, 24, 3)],
                          axis=-1)
        return MonthMet(
            linke=self._sample(f"tl_0m_{mm}"),
            cbh=self._sample(f"kcb_{mm}"),
            cdh=self._sample(f"kcd_{mm}"),
            temps8=temps8,
            wind=self._sample(f"windeffect_{mm}", required=False),
            spectral=self._sample(f"spectraleffect_{self._panel}_{mm}", required=False))
