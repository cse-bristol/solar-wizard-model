# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import math
import unittest
from unittest import mock

from typing import Sequence

import numpy as np

from solar_pv.pv import horizon

# huge radius => curvature drop negligible, so analytic angles are exact:
NO_CURVATURE = 1e15
EAST = (1.0, 0.0)
NORTH = (0.0, 1.0)
WEST = (-1.0, 0.0)
SOUTH = (0.0, -1.0)



def compute_horizons(elevation: np.ndarray,
                     ew_res: float,
                     ns_res: float,
                     direction_vectors: Sequence,
                     max_distance: float,
                     earth_radius: float = horizon.EARTH_RADIUS,
                     mask: np.ndarray = None) -> np.ndarray:
    """Dense form: as compute_horizons_flat, but scattered back into a full
    (n_directions, rows, cols) grid (NaN for unevaluated / nodata-origin cells). Used where the
    grid is small (validation); prefer compute_horizons_flat on real job grids."""
    values, orow, ocol = horizon.compute_horizons_flat(
        elevation, ew_res, ns_res, direction_vectors, max_distance, earth_radius, mask)
    rows, cols = np.asarray(elevation).shape
    out = np.full((len(direction_vectors), rows, cols), np.nan, dtype=np.float64)
    for d_idx in range(len(direction_vectors)):
        out[d_idx][orow, ocol] = values[:, d_idx]
    return out


def reference_horizons(z: np.ndarray, ew_res: float, ns_res: float, direction_vectors: Sequence,
                       max_distance: float, earth_radius: float = horizon.EARTH_RADIUS,
                       mask: np.ndarray = None) -> np.ndarray:
    """The plain r.horizon march, one origin at a time and with no early termination, as
    (n_evaluated, n_directions) in compute_horizons_flat's row-major origin order."""
    valid = np.isfinite(z) & (z > horizon.NODATA_BELOW)
    evaluate = valid if mask is None else valid & mask
    rows, cols = z.shape
    stepxy = 0.5 * (ew_res + ns_res)
    out = []
    for r, c in zip(*np.nonzero(evaluate)):
        row = []
        for cos_a, sin_a in direction_vectors:
            best = -np.inf
            for k in range(1, int(math.ceil(max_distance / stepxy)) + 2):
                di = math.floor(k * stepxy * cos_a / ew_res + 0.5)
                dj = math.floor(k * stepxy * sin_a / ns_res + 0.5)
                length = math.hypot(di * ew_res, dj * ns_res)
                if length > max_distance:
                    break
                if (di, dj) == (0, 0) or not (0 <= r - dj < rows and 0 <= c + di < cols):
                    continue
                if valid[r - dj, c + di]:
                    curvature = 0.5 * length * length / earth_radius
                    best = max(best, (z[r - dj, c + di] - z[r, c] - curvature) / length)
            row.append(min(max(math.atan(best), 0.0), math.pi / 2.0))
        out.append(row)
    return np.array(out).reshape(-1, len(direction_vectors))


class HorizonTest(unittest.TestCase):

    def test_flat_terrain_has_zero_horizon(self):
        z = np.zeros((5, 5))
        out = compute_horizons(z, 1.0, 1.0, [EAST, NORTH, WEST, SOUTH],
                                       max_distance=100, earth_radius=NO_CURVATURE)
        np.testing.assert_allclose(out, 0.0, atol=1e-12)

    def test_wall_to_the_east(self):
        # origin at (1,4); a 10 m cell 4 cells east at (1,8); 1 m cells.
        z = np.zeros((3, 11))
        z[1, 8] = 10.0
        out = compute_horizons(z, 1.0, 1.0, [EAST, WEST],
                                       max_distance=100, earth_radius=NO_CURVATURE)
        east, west = out[0], out[1]
        self.assertAlmostEqual(east[1, 4], math.atan(10.0 / 4.0), places=6)
        # nothing to the west of the origin -> zero horizon looking west:
        self.assertAlmostEqual(west[1, 4], 0.0, places=12)

    def test_horizon_never_exceeds_pi_half(self):
        # a very tall adjacent cell gives a near-vertical angle, bounded by pi/2:
        z = np.zeros((3, 3))
        z[1, 2] = 1e6
        out = compute_horizons(z, 1.0, 1.0, [EAST], max_distance=10,
                                       earth_radius=NO_CURVATURE)
        self.assertLessEqual(out[0][1, 1], math.pi / 2.0)
        self.assertAlmostEqual(out[0][1, 1], math.pi / 2.0, places=5)

    def test_nodata_origin_is_nan(self):
        z = np.zeros((3, 3))
        z[1, 1] = -9999.0
        out = compute_horizons(z, 1.0, 1.0, [EAST], max_distance=10,
                                       earth_radius=NO_CURVATURE)
        self.assertTrue(math.isnan(out[0][1, 1]))

    def test_non_square_cells_use_axis_resolution(self):
        # ns_res != ew_res: a cell 3 rows north at 2 m rows is 6 m away.
        z = np.zeros((7, 3))
        z[1, 1] = 4.0   # north is row-decreasing; origin (4,1), obstacle 3 rows north
        out = compute_horizons(z, 1.0, 2.0, [NORTH], max_distance=100,
                                       earth_radius=NO_CURVATURE)
        self.assertAlmostEqual(out[0][4, 1], math.atan(4.0 / 6.0), places=6)

    def test_mask_restricts_evaluated_cells(self):
        # a wall east of two origin cells; only one is masked -> only it gets a value.
        z = np.zeros((3, 11))
        z[1, 8] = 10.0
        mask = np.zeros((3, 11), dtype=bool)
        mask[1, 4] = True
        out = compute_horizons(z, 1.0, 1.0, [EAST], max_distance=100,
                                       earth_radius=NO_CURVATURE, mask=mask)
        self.assertAlmostEqual(out[0][1, 4], math.atan(10.0 / 4.0), places=6)
        self.assertTrue(math.isnan(out[0][1, 5]))  # not masked -> not evaluated    
        # masking the origin does not change the terrain seen from a masked cell:
        no_mask = compute_horizons(z, 1.0, 1.0, [EAST], max_distance=100,
                                           earth_radius=NO_CURVATURE)
        self.assertAlmostEqual(out[0][1, 4], no_mask[0][1, 4], places=12)

    def test_curvature_lowers_horizon(self):    
        z = np.zeros((3, 21))
        z[1, 20] = 5.0
        with_curv = compute_horizons(z, 1.0, 1.0, [EAST], max_distance=100,
                                             earth_radius=horizon.EARTH_RADIUS)
        without = compute_horizons(z, 1.0, 1.0, [EAST], max_distance=100,
                                           earth_radius=NO_CURVATURE)
        self.assertLess(with_curv[0][1, 4], without[0][1, 4])

    def test_matches_reference_march(self):
        # rough terrain with nodata, origins right up to the grid edge (rays leaving the grid),
        # non-square cells, and small chunks/batches so work is split across several threads
        # and early termination kicks in mid-ray. Must be bit-identical, not just close.
        rng = np.random.default_rng(0)
        z = rng.uniform(0.0, 5.0, (40, 30)) + rng.choice([0.0, 0.0, 0.0, 20.0], (40, 30))
        z[rng.random(z.shape) < 0.05] = -9999.0
        z[rng.random(z.shape) < 0.02] = np.nan
        mask = rng.random(z.shape) < 0.6
        vectors = horizon.nominal_vectors(horizon.grass_directions(20.0))
        with mock.patch.object(horizon, "CHUNK_SIZE", 64), \
                mock.patch.object(horizon, "BATCH_SIZE", 256):
            values, _, _ = horizon.compute_horizons_flat(z, 1.0, 2.0, vectors, 25.0,
                                                         mask=mask, workers=4)
        expected = reference_horizons(z, 1.0, 2.0, vectors, 25.0, mask=mask)
        np.testing.assert_array_equal(values, expected)


if __name__ == "__main__":
    unittest.main()
