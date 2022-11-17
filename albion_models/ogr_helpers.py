import os.path
from typing import List

from osgeo import ogr


def get_layer_names(gpkg_filename: str) -> List[str]:
    """Get the names of layers in a gpkg"""
    if os.path.isfile(gpkg_filename):
        ds = ogr.Open(gpkg_filename)
        return [ds.GetLayerByIndex(l_ix).GetName() for l_ix in range(ds.GetLayerCount())]
    return []
