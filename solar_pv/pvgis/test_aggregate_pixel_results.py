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
        def _panel_field(filename, panel_id: int, field: str):
            panels, roofs = _aggregate(filename)
            panel = [p for p in panels if p['panel_id'] == panel_id]
            assert len(panel) == 1, f"No panel with ID {panel_id} found. Possible values: {[p['panel_id'] for p in panels]}"
            val = panel[0][field]
            if isinstance(val, float):
                return round(val, 3)
            elif isinstance(val, list):
                return [round(v, 3) for v in val]
            else:
                return val

        self.parameterised_test([
            ('osgb1000014995063.json', 8075, 'kwh_year_avg', 256.655),
            ('osgb1000014995063.json', 8075, 'kwh_m01_avg', 5.327),
            ('osgb1000014995063.json', 8075, 'kwh_m06_avg', 38.079),
            ('osgb1000014995063.json', 8075, 'horizon', [0.432, 0.412, 0.437, 0.445, 0.479, 0.495, 0.463, 0.404, 0.356, 0.29, 0.285, 0.199, 0.198, 0.079, 0.14, 0.141, 0.225, 0.266, 0.298, 0.289, 0.314, 0.329, 0.34, 0.337, 0.33, 0.272, 0.252, 0.163, 0.151, 0.095, 0.087, 0.064, 0.211, 0.215, 0.369, 0.399]),
        ], _panel_field)
