# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import logging
import os.path
from typing import List

from osgeo import ogr


def get_layer_names(gpkg_filename: str) -> List[str]:
    """Get the names of layers in a gpkg"""
    if os.path.isfile(gpkg_filename):
        ds = ogr.Open(gpkg_filename)
        if ds:
            return [ds.GetLayerByIndex(l_ix).GetName() for l_ix in range(ds.GetLayerCount())]
        else:
            logging.error("ogr.Open({gpkg_filename}) failed")
    return []
