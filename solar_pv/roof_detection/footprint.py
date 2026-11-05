# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
"""
A building's faces (the edges of its footprint), which roof planes' aspects are snapped to.
"""
from dataclasses import dataclass
from typing import List, Optional

import numpy as np
import shapely
from shapely.geometry import Polygon
from shapely.strtree import STRtree

from solar_pv.geos import simplify_by_angle, polygon_line_segments, azimuth_deg, deg_diff


@dataclass
class FaceIndex:
    """The building's simplified faces, a spatial index over them, and their azimuths
    (whole degrees, truncated)."""
    line_segments: list
    rtree: STRtree
    azimuths: List[int]

    @classmethod
    def of(cls, polygon: Polygon) -> "FaceIndex":
        polygon = simplify_by_angle(polygon, tolerance_degrees=2.0)
        return cls.of_faces(polygon_line_segments(polygon, min_length=1.0))

    @classmethod
    def of_faces(cls, line_segments: list) -> "FaceIndex":
        return cls(line_segments, STRtree(line_segments),
                   [int(azimuth_deg(seg.coords[0], seg.coords[1])) for seg in line_segments])

    def faces_near(self, points: np.ndarray, points_tree: STRtree, distance: float) -> np.ndarray:
        """
        Indices of the faces that intersect `MultiPoint(points).buffer(distance)`, in
        the order `self.rtree.query` would return them.

        Buffering thousands of points into one polygon is expensive, so this finds the
        faces within `distance` of any point, then tests each against the buffers of
        just the points near it. Those buffers use `buffer()`'s default of 16 segments
        per quarter circle - the faces between the polygonal and true circle are
        excluded, as they would be by the MultiPoint buffer. A face nearer a point than
        the polygon's inscribed circle intersects its buffer, so only the points in the
        sliver between are buffered.
        """
        x0, y0, x1, y1 = shapely.total_bounds(points)
        candidates = self.rtree.query(shapely.box(x0 - distance, y0 - distance, x1 + distance, y1 + distance))
        inscribed = distance * np.cos(np.pi / 64) - 1e-6
        keep = np.zeros(len(candidates), dtype=bool)
        for i, face_idx in enumerate(candidates):
            face = self.line_segments[face_idx]
            near = points_tree.query(face, predicate='dwithin', distance=distance)
            if len(near) == 0:
                continue
            dist = shapely.distance(face, points[near])
            keep[i] = dist.min() < inscribed or \
                shapely.intersects(face, shapely.buffer(points[near[dist >= inscribed]], distance, quad_segs=16)).any()
        return candidates[keep]


def closest_azimuth(azimuths: List[float], aspect: float, thresh: float) -> Optional[float]:
    az = min(azimuths, key=lambda az_: deg_diff(az_, aspect))
    if deg_diff(az, aspect) < thresh:
        return az
    else:
        return None
