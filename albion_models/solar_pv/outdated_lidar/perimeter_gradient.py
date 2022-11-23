from dataclasses import dataclass
from typing import Dict, Any, Tuple, Optional

from shapely import wkt, strtree
from shapely.geometry import LineString, Point
from shapely.ops import substring


@dataclass
class HeightAggregator:
    pixels_within: int = 0
    pixels_without: int = 0
    within_elevation_sum: float = 0.0
    without_elevation_sum: float = 0.0

    def __init__(self, pixels, debug: bool = False) -> None:
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


def _perpendicular_bisector(line_segment: LineString, length: float):
    l1 = line_segment.parallel_offset(length / 2, side='left')
    l2 = line_segment.parallel_offset(length / 2, side='right')
    return LineString([l1.centroid.coords[0], l2.centroid.coords[0]])


def check_perimeter_gradient(building,
                             resolution_metres: float,
                             segment_length: int = 2,
                             bisector_length: int = 5,
                             gradient_threshold: float = 0.5,
                             bad_bisector_ratio: float = 0.52,
                             debug: bool = False):
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
    pixels = []
    pixels_by_id = {}
    for pixel in building['pixels']:
        point = Point(pixel['x'], pixel['y'])
        pixels.append(point)
        pixels_by_id[id(point)] = pixel
    pixel_rtree = strtree.STRtree(pixels)

    length = int(geom.exterior.length)
    total = 0
    bad = 0
    for start in range(0, length, segment_length):
        # Find a straight line-segment:
        segment = substring(geom.exterior, start, start + segment_length)
        if len(segment.coords) > 2:
            segment = LineString(segment.coords[:2])

        # Find the perpendicular bisector of the line-segment:
        perp_bisector = _perpendicular_bisector(segment, bisector_length)

        # Find all the pixels that lie on it:
        points_on_cross = pixel_rtree.query(perp_bisector.buffer(resolution_metres / 2))
        pixels_on_cross = [pixels_by_id[id(p)] for p in points_on_cross]

        # Count the bisectors where the difference in average height between
        # internal and external pixels is below the threshold:
        ha = HeightAggregator(pixels_on_cross)
        h_within, h_without = ha.average_heights()
        if h_without and h_within:
            # print(h_within - h_without)
            if h_within - h_without < gradient_threshold:
                total += 1
                bad += 1
            else:
                total += 1

    if debug:
        print(f"Perimeter gradient results: total: {total}, bad: {bad}, ratio: {bad / total}")
    if total > 0 and bad / total > bad_bisector_ratio:
        return "OUTDATED_LIDAR_COVERAGE"
    else:
        return None
