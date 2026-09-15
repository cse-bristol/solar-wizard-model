# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
"""
Validation areas and fixture loading for the PV port golden tests.

Inputs (elevation/mask/overrides/sample points/cached API) are committed under
testdata/pvmaps/test_pvmaps_real_data*/inputs. Frozen GRASS/PVMAPS golden output rasters
are written by bin/capture_pvmaps_goldens.py into GOLDEN_ROOT (committed separately from
the gitignored testdata/pvmaps/outputs).

See docs/pv-grass-removal-plan.md.
"""
import os
import pickle
from dataclasses import dataclass, field
from os.path import join
from typing import List, Optional, Tuple

from solar_pv.paths import TEST_DATA

PVMAPS_TEST_DATA = join(TEST_DATA, "pvmaps")
# Frozen goldens live here (committed), NOT in the gitignored .../pvmaps/outputs:
GOLDEN_ROOT = join(PVMAPS_TEST_DATA, "goldens")

# The deployed model's defaults, used when capturing the goldens:
FLAT_ROOF_DEGREES = 10.0
FLAT_ROOF_DEGREES_THRESHOLD = 5.0
HORIZON_SEARCH_DISTANCE = 1000.0
HORIZON_STEP_DEGREES = 45  # 8 slices; keep coarse for fast goldens


@dataclass
class Area:
    """A validation area with committed inputs and (once captured) frozen goldens."""
    name: str
    # dir under testdata/pvmaps holding inputs/ (and, historically, outputs/):
    subdir: str
    elevation: str = "elevation_27700.tif"
    mask: str = "mask_27700.tif"
    # compass-aspect override raster (flat-roof handling), or None:
    aspect_override: Optional[str] = None
    notes: str = ""

    @property
    def input_dir(self) -> str:
        return os.path.realpath(join(PVMAPS_TEST_DATA, self.subdir, "inputs"))

    @property
    def golden_dir(self) -> str:
        return os.path.realpath(join(GOLDEN_ROOT, self.name))

    def input_path(self, filename: str) -> str:
        return join(self.input_dir, filename)

    def has_inputs(self) -> bool:
        return (os.path.exists(self.input_path(self.elevation))
                and os.path.exists(self.input_path(self.mask)))

    def has_goldens(self) -> bool:
        return os.path.exists(join(self.golden_dir, "kwh_year.tif"))

    def load_api_ground_truth(self) -> Optional[Tuple[List[Tuple[float, float]], list]]:
        """
        Returns (sample_points_en_27700, api_results) or None if the cache is absent.
        api_results[i] == ([12 monthly E_d], E_year) for sample_points[i].
        NB: PVGIS API, secondary shape-check only — not the tight oracle.
        """
        locns_pkl = self.input_path("loc_real_pv_sample_locns.pkl")
        api_pkl = self.input_path("api_real_pv_output.pkl")
        if not (os.path.exists(locns_pkl) and os.path.exists(api_pkl)):
            return None
        with open(locns_pkl, "rb") as f:
            sampled = pickle.load(f)  # list of (x_px, y_px, east, north)
        with open(api_pkl, "rb") as f:
            api_results = pickle.load(f)
        points = [(float(e), float(n)) for _, _, e, n in sampled]
        if len(points) != len(api_results):
            return None
        return points, api_results


# Coverage: flat/hilly/urban/coastal + edge-of-met-coverage (thurso lacks spectral+wind).
AREAS: List[Area] = [
    Area("real_data", "test_pvmaps_real_data",
         aspect_override="flat_roof_nan_27700.tif",
         notes="original real-data area; flat roofs -> NaN aspect override"),
    Area("alnwick", "test_pvmaps_real_data_alnwick",
         notes="missing pvmaps solar spectral data"),
    Area("alnwick_flat", "test_pvmaps_real_data_alnwick_flat",
         aspect_override="flat_roof_aspect_27700.tif",
         notes="flat roofs; missing pvmaps solar spectral data"),
    Area("thurso", "test_pvmaps_real_data_thurso",
         notes="edge of met coverage: missing pvmaps spectral AND wind data"),
]


def area_by_name(name: str) -> Area:
    for a in AREAS:
        if a.name == name:
            return a
    raise KeyError(f"no validation area named {name!r}")
