# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
"""
GDAL/OSR helpers for the horizon port: replicate r.horizonmask's exact marching directions
(kept separate from the pure-numpy horizon.py).

r.horizon does not march straight along the nominal grid azimuth. For each point it steps a
tiny distance in *geographic* space and transforms back to the projected CRS, so the
marching vector is rotated by the local grid convergence (main.c, calculate()). We reproduce
that transform at the tile centre — convergence varies only ~0.01 deg/km, negligible across
a job tile.
"""
import math
from typing import List, Sequence, Tuple

from osgeo import osr

osr.UseExceptions()


def _transformers(epsg: int):
    proj = osr.SpatialReference(); proj.ImportFromEPSG(epsg)
    geo = osr.SpatialReference(); geo.ImportFromEPSG(4326)
    proj.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)  # x=east, y=north
    geo.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)   # x=lon,  y=lat
    return (osr.CoordinateTransformation(proj, geo),
            osr.CoordinateTransformation(geo, proj))


def grass_marching_vectors(gt, shape, directions_rad: Sequence[float],
                           epsg: int = 27700) -> List[Tuple[float, float]]:
    """
    (cos, sin) marching unit vectors matching r.horizon's convergence-corrected directions,
    evaluated at the tile centre. `gt`/`shape` are the elevation raster's geotransform and
    (rows, cols). Directions are radians CCW from East.
    """
    fwd, inv = _transformers(epsg)
    rows, cols = shape
    xp = gt[0] + gt[1] * cols / 2.0
    yp = gt[3] + gt[5] * rows / 2.0

    lon, lat, _ = fwd.TransformPoint(xp, yp)
    lon_r = math.radians(lon)
    lat_r = math.radians(lat)

    vectors = []
    for angle in directions_rad:
        input_angle = (angle + math.pi / 2.0) % (2.0 * math.pi)
        delt_lat = -0.0001 * math.cos(input_angle)
        delt_lon = 0.0001 * math.sin(input_angle) / math.cos(lat_r)
        new_lon = math.degrees(lon_r + delt_lon)
        new_lat = math.degrees(lat_r + delt_lat)
        east2, north2, _ = inv.TransformPoint(new_lon, new_lat)
        delt_east = east2 - xp
        delt_nor = north2 - yp
        delt_dist = math.hypot(delt_east, delt_nor)
        cos_a = delt_east / delt_dist
        sin_a = delt_nor / delt_dist
        if abs(sin_a) < 1e-7:
            sin_a = 0.0
        if abs(cos_a) < 1e-7:
            cos_a = 0.0
        vectors.append((cos_a, sin_a))
    return vectors
