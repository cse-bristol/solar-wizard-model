# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
"""
Tile a region on a grid rotated to a given angle, choosing which tiles are occupied by
an exactly-solved min-cut MRF: per-tile cost is the area mismatch against the region,
plus `perimeter_lambda` per tile edge of boundary. The pairwise term is Potts, hence
submodular, so a single max-flow gives the global optimum; `perimeter_lambda = 0` is
the plain `occupancy_threshold` rule.

Ported from the ukbf regularizer (ukbf/regularize/occupancy.py). Coverage is measured
exactly with shapely rather than by rasterizing.
"""
import math
from typing import Optional, Union

import numpy as np
import scipy.sparse as sp
import shapely
from scipy.sparse.csgraph import breadth_first_order, maximum_flow
from shapely import affinity
from shapely.geometry import Polygon, MultiPolygon, Point

# Coverage is measured on a sub-tile grid this many times finer per axis; doubles as the
# grid-phase search resolution (offsets of 1/TILE_SUBSAMPLE of a tile).
TILE_SUBSAMPLE = 4
# Phase offsets solved exactly, ranked by their cheap threshold energy (an upper bound on
# the optimum). 0 = solve every offset.
PHASE_CANDIDATES = 4
_MAX_FINE_CELLS = 4_000_000
# maximum_flow needs int32 capacities: energy is quantized to this many steps per tile.
_ENERGY_SCALE = 10_000
# Fill cost for a tile with no legal area. Only has to beat the largest perimeter saving
# a tile can buy (4 * lambda).
_FORBID = 1000.0
# The tile grid extends this many tiles past the region's bounds (and up to one more, to fit
# whole tiles):
_GRID_MARGIN_TILES = 2


def tile_polygon(region: Union[Polygon, MultiPolygon],
                 legal: Union[Polygon, MultiPolygon],
                 angle_deg: float,
                 origin: Point,
                 tile_m: float,
                 perimeter_lambda: float = 0.3,
                 occupancy_threshold: float = 0.5) -> Optional[Union[Polygon, MultiPolygon]]:
    """
    Tile-quantize `region` on a grid rotated CCW by `angle_deg` about `origin`.

    Coverage is measured relative to the part of each tile inside `legal`, so a tile
    straddling the legal edge is promoted if the region fills its legal part; the caller
    re-clips the result to `legal`. Returns None if no tile is occupied.
    """
    rot = _rotate_valid(region, angle_deg, origin)
    rot_legal = _rotate_valid(legal, angle_deg, origin)
    rot = rot.intersection(rot_legal)
    if rot.is_empty:
        return None

    minx, miny, maxx, maxy = rot.bounds
    margin = _GRID_MARGIN_TILES * tile_m
    minx, miny = minx - margin, miny - margin
    maxx, maxy = maxx + margin, maxy + margin
    ncol = int(math.ceil((maxx - minx) / tile_m))
    nrow = int(math.ceil((maxy - miny) / tile_m))
    if nrow < 2 or ncol < 2:
        return None
    sub = TILE_SUBSAMPLE
    while nrow * ncol * sub * sub > _MAX_FINE_CELLS and sub > 1:
        sub //= 2
    fpix = tile_m / sub

    fine = _cell_fractions(rot, minx, maxy, fpix, nrow * sub, ncol * sub)
    fine_legal = _cell_fractions(rot_legal, minx, maxy, fpix, nrow * sub, ncol * sub)

    chosen = _best_phase(fine, fine_legal, nrow, ncol, sub, perimeter_lambda, occupancy_threshold)
    if chosen is None:
        return None
    i, j, filled = chosen

    rows, cols = np.nonzero(filled)
    x0 = minx + j * fpix
    y0 = maxy - i * fpix
    tiles = shapely.box(x0 + cols * tile_m, y0 - (rows + 1) * tile_m,
                        x0 + (cols + 1) * tile_m, y0 - rows * tile_m)
    # union leaves a vertex at every tile corner along a straight edge
    tiled = shapely.union_all(tiles).simplify(0)
    return _rotate_valid(tiled, -angle_deg, origin)


def legal_window(region: Union[Polygon, MultiPolygon], angle_deg: float, origin: Point, tile_m: float) -> Polygon:
    """
    A rectangle that `tile_polygon`'s grid for `region` (rotated by `angle_deg` about
    `origin`) is within: `legal` only matters within it.
    """
    minx, miny, maxx, maxy = affinity.rotate(region, angle_deg, origin=origin).bounds
    # the grid's margin, up to a tile more to fit whole tiles, and a tile to spare:
    margin = (_GRID_MARGIN_TILES + 2) * tile_m
    window = shapely.box(minx - margin, miny - margin, maxx + margin, maxy + margin)
    return affinity.rotate(window, -angle_deg, origin=origin)


def _rotate_valid(geom, angle_deg: float, origin: Point):
    """
    Rotation moves vertices off-grid, which can turn a near-degenerate sliver (e.g. left
    by clipping to a meeting line) into a self-intersection that breaks overlays.
    """
    rot = affinity.rotate(geom, angle_deg, origin=origin)
    if rot.is_valid:
        return rot
    try:
        return shapely.make_valid(rot, method='structure', keep_collapsed=False)
    except shapely.errors.GEOSException:
        # GEOS can fail on a multipolygon with a self-intersecting part ("unable to assign
        # free hole to a shell"), even snapped to a grid, though not on each part alone;
        # buffer(0) alone drops the self-intersecting part.
        parts = [shapely.make_valid(part, method='structure', keep_collapsed=False) for part in shapely.get_parts(rot)]
        return shapely.GeometryCollection(parts).buffer(0)


def _cell_fractions(geom, minx: float, maxy: float, fpix: float, nrow: int, ncol: int) -> np.ndarray:
    """Exact fraction of each fine cell covered by `geom`. Row 0 is the top (maxy) row."""
    fracs = np.zeros((nrow, ncol))
    if geom.is_empty:
        return fracs
    gminx, gminy, gmaxx, gmaxy = geom.bounds
    c0 = max(int((gminx - minx) / fpix) - 1, 0)
    c1 = min(int(math.ceil((gmaxx - minx) / fpix)) + 1, ncol)
    r0 = max(int((maxy - gmaxy) / fpix) - 1, 0)
    r1 = min(int(math.ceil((maxy - gminy) / fpix)) + 1, nrow)
    if c0 >= c1 or r0 >= r1:
        return fracs

    cc, rr = np.meshgrid(np.arange(c0, c1), np.arange(r0, r1))
    cells = shapely.box(minx + cc * fpix, maxy - (rr + 1) * fpix,
                        minx + (cc + 1) * fpix, maxy - rr * fpix)
    shapely.prepare(geom)
    inside = shapely.contains(geom, cells)
    edge = shapely.intersects(geom, cells) & ~inside
    window = np.zeros(cells.shape)
    window[inside] = 1.0
    # Rounded, as regions are made of pixels, so many cells are exactly half covered, and
    # which way floating-point error falls mustn't decide them:
    window[edge] = np.round(shapely.area(shapely.intersection(cells[edge], geom)) / (fpix * fpix), 9)
    fracs[r0:r1, c0:c1] = np.minimum(window, 1.0)
    return fracs


def _blocks(a: np.ndarray, i: int, j: int, r: int, c: int, sub: int) -> np.ndarray:
    """Per-tile sum of fine cells for grid phase offset (i, j)."""
    return a[i:i + r * sub, j:j + c * sub].reshape(r, sub, c, sub).sum(axis=(1, 3))


def _coverage(fine: np.ndarray, fine_legal: np.ndarray, i: int, j: int,
              nrow: int, ncol: int, sub: int):
    """
    Per-tile covered fraction of the legal part of the tile, and the legal part as a
    fraction of a whole tile. One tile row/column is dropped so every offset in
    [0, sub) stays inside the padded grid.
    """
    r, c = nrow - 1, ncol - 1
    num = _blocks(fine, i, j, r, c, sub)
    den = _blocks(fine_legal, i, j, r, c, sub)
    frac = np.divide(num, den, out=np.zeros(den.shape), where=den > 1e-9)
    return np.minimum(frac, 1.0), den / float(sub * sub)


def _cut_edges(filled: np.ndarray, weight: np.ndarray) -> int:
    """
    Perimeter of the filled set in tile edges. Edges against a tile with no legal area
    are not counted: that edge is fixed by the legal boundary rather than chosen, and
    charging for it would bias the labelling away from filling up to it.
    """
    legal = weight > 0
    vert = (filled[:-1, :] != filled[1:, :]) & legal[:-1, :] & legal[1:, :]
    horz = (filled[:, :-1] != filled[:, 1:]) & legal[:, :-1] & legal[:, 1:]
    return int(vert.sum() + horz.sum())


def _data_costs(frac: np.ndarray, weight: np.ndarray, occ: float):
    """
    Per-tile cost of calling a tile empty vs filled, in units of one tile of area. At
    `occ` = 0.5 this is exactly the area symmetric difference over the legal part of the tile.
    """
    cost0 = 2.0 * (1.0 - occ) * frac * weight
    cost1 = 2.0 * occ * (1.0 - frac) * weight
    cost1 = np.where(weight > 0, cost1, _FORBID)
    return cost0, cost1


def _energy(frac: np.ndarray, weight: np.ndarray, filled: np.ndarray, lam: float, occ: float) -> float:
    cost0, cost1 = _data_costs(frac, weight, occ)
    return float(np.where(filled, cost1, cost0).sum() + lam * _cut_edges(filled, weight))


def _mincut_labels(frac: np.ndarray, weight: np.ndarray, lam: float, occ: float) -> np.ndarray:
    """
    Occupancy grid minimising `_energy`, by max-flow / min-cut. Graph: source -> tile with
    capacity cost(empty), tile -> sink with capacity cost(filled), and lambda both ways on
    every 4-neighbour pair. Tiles still reachable from the source in the residual graph
    are filled.
    """
    r, c = frac.shape
    n = r * c
    src, snk = n, n + 1
    cost0, cost1 = _data_costs(frac, weight, occ)
    ids = np.arange(n, dtype=np.int32).reshape(r, c)
    lo = np.concatenate([ids[:-1, :].ravel(), ids[:, :-1].ravel()])
    hi = np.concatenate([ids[1:, :].ravel(), ids[:, 1:].ravel()])
    legal = (weight > 0).ravel()
    pair = lam * (legal[lo] & legal[hi])

    rows = np.concatenate([np.full(n, src, np.int32), ids.ravel(), lo, hi])
    cols = np.concatenate([ids.ravel(), np.full(n, snk, np.int32), hi, lo])
    data = np.rint(np.concatenate([cost0.ravel(), cost1.ravel(), pair, pair])
                   * _ENERGY_SCALE).astype(np.int32)
    cap = sp.coo_matrix((data, (rows, cols)), shape=(n + 2, n + 2)).tocsr()

    residual = cap - maximum_flow(cap, src, snk).flow
    residual.eliminate_zeros()  # a saturated arc must not be traversable
    reached = breadth_first_order(residual, src, directed=True, return_predecessors=False)
    filled = np.zeros(n + 2, dtype=bool)
    filled[reached] = True
    return filled[:n].reshape(r, c)


def _best_phase(fine: np.ndarray, fine_legal: np.ndarray, nrow: int, ncol: int, sub: int,
                lam: float, occ: float):
    """
    Grid phase offset and occupancy grid minimising `_energy`; returns (i, j, filled) or
    None if no offset yields an occupied grid. The phase is searched because pinning the
    grid to the bounding box makes it arbitrary, which can shift a whole edge by half a tile.
    """
    ranked = []
    for i in range(sub):
        for j in range(sub):
            frac, weight = _coverage(fine, fine_legal, i, j, nrow, ncol, sub)
            thresholded = (frac >= occ) & (weight > 0)
            if not thresholded.any():
                continue
            ranked.append((_energy(frac, weight, thresholded, lam, occ), i, j, frac, weight, thresholded))
    if not ranked:
        return None
    ranked.sort(key=lambda r: r[0])
    if lam <= 0:
        _, i, j, _, _, thresholded = ranked[0]
        return i, j, thresholded

    keep = len(ranked) if PHASE_CANDIDATES <= 0 else PHASE_CANDIDATES
    best = None
    for _, i, j, frac, weight, _ in ranked[:keep]:
        filled = _mincut_labels(frac, weight, lam, occ)
        if not filled.any():
            continue
        energy = _energy(frac, weight, filled, lam, occ)
        if best is None or energy < best[0]:
            best = (energy, i, j, filled)
    return best[1:] if best is not None else None
