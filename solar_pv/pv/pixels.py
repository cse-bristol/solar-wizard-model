# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
"""
Sparse per-pixel PV fields and per-building extraction.

The PV outputs (kwh_year, month_NN_wh, horizon_NN) are meaningful only at the building-footprint
pixels, a small fraction of a job grid. `PixelFields` stores them as flat length-N arrays at the
valid pixels rather than as full 2D grids (which, at 1 m over a 5 km job, would be tens of GB).

`pixels_for_geoms` returns the {toid: [pixel dict]} shape the roof-plane aggregation consumes:
each pixel dict has x, y, pixel_id, toid and one entry per field. A pixel belongs to a building
when its centre falls inside the building geometry (matching the centre-based raster clipping
this replaces); the aggregation then does the precise per-roof-plane intersection weighting.
"""
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import numpy as np
import shapely
from shapely import points as shapely_points

GeoTransform = Tuple[float, float, float, float, float, float]


@dataclass
class PixelFields:
    """PV fields stored only at the N valid (building-footprint) pixels of a job grid.

    :param geotransform: GDAL geotransform of the full grid.
    :param shape: (rows, cols) of the full grid.
    :param rows, cols: (N,) grid indices of the valid pixels.
    :param values: {field name: (N,) array}, e.g. kwh_year, month_01_wh.., horizon_00...
    """
    geotransform: GeoTransform
    shape: Tuple[int, int]
    rows: np.ndarray
    cols: np.ndarray
    values: Dict[str, np.ndarray]
    _sorted_keys: Optional[np.ndarray] = field(default=None, repr=False, compare=False)
    _order: Optional[np.ndarray] = field(default=None, repr=False, compare=False)

    def positions(self, rows: np.ndarray, cols: np.ndarray) -> np.ndarray:
        """Positions in the value arrays for the given (rows, cols) grid indices, or -1 where a
        cell is not one of the stored valid pixels."""
        n = self.rows.size
        query = np.full(np.asarray(rows).shape, -1, dtype=np.int64)
        if n == 0:
            return query
        if self._sorted_keys is None:
            keys = self.rows.astype(np.int64) * self.shape[1] + self.cols.astype(np.int64)
            self._order = np.argsort(keys, kind="stable")
            self._sorted_keys = keys[self._order]
        q = np.asarray(rows, dtype=np.int64) * self.shape[1] + np.asarray(cols, dtype=np.int64)
        ins = np.clip(np.searchsorted(self._sorted_keys, q), 0, n - 1)
        matched = self._sorted_keys[ins] == q
        query[matched] = self._order[ins[matched]]
        return query

    @classmethod
    def from_dense(cls, field_arrays: Dict[str, np.ndarray], geotransform: GeoTransform,
                   valid: Optional[np.ndarray] = None) -> "PixelFields":
        """Build from full-grid arrays (test/convenience). Valid pixels default to those finite
        in every field."""
        arrs = {k: np.asarray(v) for k, v in field_arrays.items()}
        shape = next(iter(arrs.values())).shape
        if valid is None:
            valid = np.ones(shape, dtype=bool)
            for a in arrs.values():
                valid &= np.isfinite(a)
        rows, cols = np.nonzero(valid)
        return cls(geotransform, shape, rows, cols, {k: a[rows, cols] for k, a in arrs.items()})


def pixels_for_geoms(pixel_fields: PixelFields,
                     geoms_by_toid: Dict[str, object]) -> Dict[str, List[dict]]:
    """
    :param pixel_fields: the sparse PV fields on a north-up grid.
    :param geoms_by_toid: {toid: shapely (EPSG:27700) building geometry}.
    :return: {toid: [pixel dict]}, each dict with x, y, pixel_id, toid + one key per field.
    """
    pf = pixel_fields
    ox, ew, _, oy, _, ns = pf.geotransform  # ns is negative (north-up)
    rows, cols = pf.shape
    fields = list(pf.values)

    by_toid: Dict[str, List[dict]] = defaultdict(list)
    for toid, geom in geoms_by_toid.items():
        minx, miny, maxx, maxy = geom.bounds
        # pixel-column/row window covering the geom bbox (centres are at +0.5):
        c0 = max(0, int(np.floor((minx - ox) / ew)))
        c1 = min(cols, int(np.ceil((maxx - ox) / ew)) + 1)
        r0 = max(0, int(np.floor((maxy - oy) / ns)))  # ns<0 -> maxy gives the top row
        r1 = min(rows, int(np.ceil((miny - oy) / ns)) + 1)
        if c0 >= c1 or r0 >= r1:
            continue

        rr, cc = np.meshgrid(np.arange(r0, r1), np.arange(c0, c1), indexing="ij")
        rr = rr.ravel()
        cc = cc.ravel()
        xs = ox + (cc + 0.5) * ew
        ys = oy + (rr + 0.5) * ns
        inside = shapely.contains(geom, shapely_points(xs, ys))
        if not inside.any():
            continue
        rr, cc, xs, ys = rr[inside], cc[inside], xs[inside], ys[inside]
        # keep only pixels present in the sparse set (a building pixel with no PV data is
        # dropped, matching the raster clip this replaces):
        pos = pf.positions(rr, cc)
        present = pos >= 0
        for r, c, x, y, p in zip(rr[present], cc[present], xs[present], ys[present], pos[present]):
            pixel = {"toid": toid, "x": float(x), "y": float(y),
                     "pixel_id": f"{toid}:{x}:{y}"}
            for f in fields:
                pixel[f] = float(pf.values[f][p])
            by_toid[toid].append(pixel)

    return dict(by_toid)
