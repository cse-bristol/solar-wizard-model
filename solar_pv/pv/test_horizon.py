# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import math
import unittest

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


if __name__ == "__main__":
    unittest.main()
