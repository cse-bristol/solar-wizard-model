# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
from dataclasses import dataclass
from typing import Dict, Any, Tuple, Optional, List

from shapely import wkt, strtree
from shapely.geometry import LineString, Point
from shapely.ops import substring

from solar_pv.geos import perpendicular_bisector


@dataclass
class HeightAggregator:
    pixels_within: int = 0
    pixels_without: int = 0
    within_elevation_sum: float = 0.0
    without_elevation_sum: float = 0.0

    def __init__(self, pixels: List[dict], debug: bool = False) -> None:
        self.debug = debug
        for pixel in pixels:
            self._process_pixel(pixel)

    def _process_pixel(self, pixel: Dict[str, Any]):
        if pixel['within_building']:
            self.pixels_within += 1
            self.within_elevation_sum += pixel['elevation']
        elif pixel['without_building']:
            self.pixels_without += 1
            self.without_elevation_sum += pixel['elevation']

    def average_heights(self) -> Tuple[Optional[float], Optional[float]]:
        if self.pixels_without > 0 and self.pixels_within > 0:
            return (self.within_elevation_sum / self.pixels_within,
                    self.without_elevation_sum / self.pixels_without)
        else:
            return None, None

    def height(self) -> Optional[float]:
        h_within, h_without = self.average_heights()
        if h_without and h_within:
            return h_within - h_without
        else:
            return None


def check_perimeter_gradient(building,
                             resolution_metres: float,
                             segment_length: int = 2,
                             bisector_length: int = 5,
                             gradient_threshold: float = 0.5,
                             bad_bisector_ratio: float = 0.52,
                             debug: bool = False) -> Tuple[Optional[str], Optional[float], Optional[float]]:
    """
    Attempt to detect outdated LiDAR.

    * Every `segment_length` metres along the building perimeter, take the
    perpendicular bisector of the line segment at that point and find all the pixels
    that lie on it within a given distance (`bisector_length`).

    * Take the difference in average height between the interior and exterior pixels
    that lie on that bisector. If it's below `gradient_threshold` metres, it counts
    as a bad bisector (as the height of the land effectively hasn't changed while
    traversing that bisector, despite in theory it crossing the building bounds)

    * if more than `bad_bisector_ratio` bisectors are like this, consider the LiDAR
    outdated.
    """
    geom = wkt.loads(building['geom'])
    points = []
    for pixel in building['pixels']:
        point = Point(pixel['x'], pixel['y'])
        points.append(point)
    pixel_rtree = strtree.STRtree(points)

    length = int(geom.exterior.length)
    total = 0
    bad = 0
    min_ground_height = 9999
    max_ground_height = 0
    min_building_height = 9999

    for start in range(0, length, segment_length):
        # Find a straight line-segment:
        segment = substring(geom.exterior, start, start + segment_length)
        if len(segment.coords) > 2:
            segment = LineString(segment.coords[:2])

        if segment.length < 0.01:
            continue

        perp_bisector = perpendicular_bisector(segment, bisector_length)

        # Find all the pixels that lie on it:
        idxs = pixel_rtree.query(perp_bisector.buffer(resolution_metres / 2), predicate='intersects')
        pixels_on_cross = [building['pixels'][idx] for idx in idxs]

        # Count the bisectors where the difference in average height between
        # internal and external pixels is below the threshold:
        ha = HeightAggregator(pixels_on_cross)
        h_within, h_without = ha.average_heights()
        if h_without and h_within:
            if h_within - h_without < gradient_threshold:
                total += 1
                bad += 1
            else:
                min_building_height = min(min_building_height, h_within)
                min_ground_height = min(min_ground_height, h_without)
                max_ground_height = min(max(max_ground_height, h_without), min_building_height - 0.1)
                total += 1

    if debug:
        print(f"Perimeter gradient results: total: {total}, bad: {bad}, ratio: {bad / total if bad > 0 else 'NA'}")
        print(f"{building['toid']} min_gh: {min_ground_height} max_gh: {max_ground_height} min_bh: {min_building_height}")
    if total > 0 and bad / total > bad_bisector_ratio:
        return "OUTDATED_LIDAR_COVERAGE", None, None
    elif total > 0:
        return None, round(min_ground_height, 1), round(max_ground_height, 1)
    else:
        return None, None, None
