# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import json
from os.path import join

from solar_pv.paths import TEST_DATA
from solar_pv import tables
from solar_pv.constants import SYSTEM_LOSS
from solar_pv.pvgis.aggregate_pixel_results import _aggregate_pixel_data
from solar_pv.pvgis.dev_aggregate_pixel_results import RASTER_TABLES
from solar_pv.test_utils.test_funcs import ParameterisedTestCase

_PIXEL_DATA = join(TEST_DATA, "pixel_aggregation")


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
            ('0001.json', 43989, 'kwh_year_avg', 3527.02),
            ('0001.json', 43989, 'kwh_m01_avg', 118.76),
            ('0001.json', 43989, 'kwh_m06_avg', 459.72),
            ('0001.json', 43989, 'horizon', [0.06, 0.16, 0.24, 0.36, 0.37, 0.39, 0.49, 0.5, 0.49, 0.49, 0.5, 0.5, 0.41, 0.41, 0.38, 0.26, 0.25, 0.16, 0.08, 0.07, 0.06, 0.03, 0.06, 0.03, 0.02, 0.01, 0.0, 0.01, 0.0, 0.0, 0.01, 0.0, 0.0, 0.04, 0.05, 0.03]),
        ], _roof_field)
