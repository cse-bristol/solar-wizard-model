# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import json
import unittest
from os.path import join

import numpy as np
from shapely import box, unary_union

from solar_pv.paths import TEST_DATA
from solar_pv import tables
from solar_pv.constants import SYSTEM_LOSS
from solar_pv.pv.pixels import PixelFields, pixels_for_geoms
from solar_pv.pv.aggregate_pixel_results import _aggregate_page, _aggregate_pixel_data
from solar_pv.test_utils.test_funcs import ParameterisedTestCase

_PIXEL_DATA = join(TEST_DATA, "pixel_aggregation")

# the per-pixel PV field names, in the order run_pv produces them (36 horizon slices, matching
# the 0001.json fixture):
RASTER_TABLES = (['kwh_year'] + [f'month_{i:02d}_wh' for i in range(1, 13)]
                 + [f'horizon_{i:02d}' for i in range(36)])


def _load_data(filename: str) -> dict:
    with open(filename) as f:
        return json.load(f)


def _aggregate(filename: str):
    job_id = 0
    schema = tables.schema(job_id)
    raster_tables = [f"{schema}.{t}" for t in RASTER_TABLES]
    resolution = 1.0
    peak_power_per_m2 = 0.2
    system_loss = SYSTEM_LOSS
    building = _load_data(join(_PIXEL_DATA, filename))
    pixels = building['pixels']
    roofs = building['roofs']
    return _aggregate_pixel_data(
        pixels=pixels,
        roof_planes=roofs,
        job_id=job_id,
        pixel_fields=[t.split(".")[1] for t in raster_tables],
        resolution=resolution,
        peak_power_per_m2=peak_power_per_m2,
        system_loss=system_loss)


class PixelAggregateTestCase(ParameterisedTestCase):

    def test_pixel_aggregation(self):
        def _roof_field(filename, roof_plane_id: int, field: str):
            roofs = _aggregate(filename)
            roof = [r for r in roofs if r['roof_plane_id'] == roof_plane_id]
            assert len(roof) == 1, f"No roof with ID {roof_plane_id} found. Possible values: {[r['roof_plane_id'] for r in roofs]}"
            val = roof[0][field]
            if isinstance(val, float):
                return round(val, 3)
            elif isinstance(val, list):
                return [round(v, 3) for v in val]
            else:
                return val

        self.parameterised_test([
            ('0001.json', 43989, 'kwh_year_avg', 3063.89),
            ('0001.json', 43989, 'kwh_m01_avg', 103.26),
            ('0001.json', 43989, 'kwh_m06_avg', 398.89),
            ('0001.json', 43989, 'horizon', [0.06, 0.16, 0.24, 0.36, 0.37, 0.39, 0.49, 0.5, 0.49, 0.49, 0.5, 0.5, 0.41, 0.41, 0.38, 0.26, 0.25, 0.16, 0.08, 0.07, 0.06, 0.03, 0.06, 0.03, 0.02, 0.01, 0.0, 0.01, 0.0, 0.0, 0.01, 0.0, 0.0, 0.04, 0.05, 0.03]),
        ], _roof_field)


def _field_arrays_from_pixels(pixels, fields):
    """Rasterise fixture pixels (1 m grid, centres at *.5) into {field: 2D array}, plus the
    grid's geotransform and a geometry covering exactly those pixels' cells."""
    xs = [p['x'] for p in pixels]
    ys = [p['y'] for p in pixels]
    min_x, max_x, min_y, max_y = min(xs), max(xs), min(ys), max(ys)
    cols = int(round(max_x - min_x)) + 1
    rows = int(round(max_y - min_y)) + 1
    gt = (min_x - 0.5, 1.0, 0.0, max_y + 0.5, 0.0, -1.0)

    arrays = {f: np.full((rows, cols), np.nan) for f in fields}
    for p in pixels:
        r = int(round(max_y - p['y']))
        c = int(round(p['x'] - min_x))
        for f in fields:
            arrays[f][r, c] = p[f]

    geom = unary_union([box(p['x'] - 0.5, p['y'] - 0.5, p['x'] + 0.5, p['y'] + 0.5)
                        for p in pixels])
    return arrays, gt, geom


class AggregateFromArraysTest(unittest.TestCase):
    """The in-process pixel source (pixels_for_geoms) drives the same roof-plane aggregation as
    the Postgis pixel source: rasterise the fixture pixels, re-extract them by geometry, and
    confirm the roof-plane outputs are unchanged."""

    def test_matches_direct_pixel_aggregation(self):
        job_id = 0
        pixel_fields = RASTER_TABLES
        with open(join(_PIXEL_DATA, "0001.json")) as f:
            building = json.load(f)
        pixels = building['pixels']
        toid = pixels[0]['toid']

        arrays, gt, geom = _field_arrays_from_pixels(pixels, pixel_fields)
        pf = PixelFields.from_dense(arrays, gt)
        extracted = pixels_for_geoms(pf, {toid: geom})[toid]
        self.assertEqual(len(extracted), len(pixels),
                         "geometry re-extraction recovered a different pixel set")

        def aggregate(px):
            # fresh roof planes each time (_aggregate_pixel_data mutates them):
            roofs = json.loads(json.dumps(building['roofs']))
            return _aggregate_pixel_data(
                pixels=px, roof_planes=roofs, job_id=job_id,
                pixel_fields=pixel_fields, resolution=1.0,
                peak_power_per_m2=0.2, system_loss=SYSTEM_LOSS)

        from_db = {r['roof_plane_id']: r for r in aggregate(pixels)}
        from_arrays = {r['roof_plane_id']: r for r in aggregate(extracted)}

        self.assertEqual(set(from_db), set(from_arrays))
        for rpid, db_roof in from_db.items():
            arr_roof = from_arrays[rpid]
            self.assertAlmostEqual(db_roof['kwh_year_avg'], arr_roof['kwh_year_avg'], places=6)
            self.assertEqual([round(h, 6) for h in db_roof['horizon']],
                             [round(h, 6) for h in arr_roof['horizon']])

    def test_aggregate_page_matches_serial(self):
        # the worker wrapper _aggregate_page aggregates a page's buildings identically to calling
        # _aggregate_pixel_data per building (no DB / pool needed):
        job_id = 0
        with open(join(_PIXEL_DATA, "0001.json")) as f:
            building = json.load(f)
        toid = building['pixels'][0]['toid']
        job = (job_id, RASTER_TABLES, 1.0, 0.2, SYSTEM_LOSS,
               {toid: json.loads(json.dumps(building['roofs']))},
               {toid: building['pixels']})
        worker = {r['roof_plane_id']: r for r in _aggregate_page(job)}

        serial = {r['roof_plane_id']: r for r in _aggregate_pixel_data(
            pixels=building['pixels'], roof_planes=json.loads(json.dumps(building['roofs'])),
            job_id=job_id, pixel_fields=RASTER_TABLES, resolution=1.0,
            peak_power_per_m2=0.2, system_loss=SYSTEM_LOSS)}

        self.assertEqual(set(worker), set(serial))
        for rpid, w in worker.items():
            self.assertAlmostEqual(w['kwh_year_avg'], serial[rpid]['kwh_year_avg'], places=6)
