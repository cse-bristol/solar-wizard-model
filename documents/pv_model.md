# PV model documentation

The solar PV suitability model has the following main stages:

1. setup
2. detection of outdated LiDAR
3. roof plane detection: fitting of planes to LiDAR points
4. creation of roof polygons
5. panel placement within roof polygons
6. evaluation of PV suitability

## 1. setup

Model run state is stored in a temporary postgres schema with a name like `solar_pv_job_{job_id}`.

Aspect (compass facing) and slope rasters are created from the elevation raster.

## 2. detection of outdated LiDAR

In many places, our LiDAR data is several years older than our building polygon data. There are inevitably buildings that have been built or significantly changed since the LiDAR survey that are in the building polygon data. If we do not detect these, the roof planes detected on these buildings will not be reliable.

The algorithm for detecting these buildings is as follows:

For a given building:
* Every `segment_length` metres along the building perimeter, take the perpendicular bisector of the line segment at that point and find all the pixels that lie on it within a given distance (`bisector_length`).

* Take the difference in average height between the interior and exterior pixels that lie on that bisector. If it's below `gradient_threshold` metres, it counts as a bad bisector (as the height of the land effectively hasn't changed while traversing that bisector, despite in theory it crossing the building bounds)

* if more than `bad_bisector_ratio` bisectors are like this, consider the LiDAR outdated.

The values used by default for the parameters are: 
* `segment_length`: 2m 
* `bisector_length`: 5m
* `gradient_threshold`: 0.5 
* `bad_bisector_ratio`: 0.52

These values were reached experimentally.

## 3. roof plane detection

We next try to fit planes to the LiDAR data, which is treated as a set of 3D points for this purpose. To do this we use a modified version of an algorithm called RANSAC (RAndom SAmple Consensus), which is a standard approach in the literature for detecting roofs in LiDAR data.

The basic RANSAC algorithm is as follows:
* pick 3 random points from the set of points
* fit a plane to those points
* fit the other points to that plane, and keep any that are within `residual_threshold` of the plane
* continue until some criteria is reached, usually when a plane has been found where more than some number of points have been fit

You can then remove those points from the set, and continue to try and fit more planes until no more are found.

The following changes are made following recommendations in Tarsha-Kurdi, 2007:

* Tarsha-Kurdi, 2007 recommends rejecting planes where the (x,y) points in the plane do not form a single contiguous region of the LIDAR. On small buildings this mostly helps but does exclude some valid planes where the correctly-fitted plane also happens to fit to other pixels in disconnected areas of the roof. We have modified it to allow planes where a small number of non-contiguous pixels fit, as long as the area ratio of those non-contiguous pixels to the area of the main mass of contiguous pixels is small. We do not do this for large buildings, which often have disconnected areas of roof which lie on the same plane, but we do still only extract the largest contiguous region as one plane.

* Do not optimise for number of points within `residual_threshold` distance from plane, instead optimise for lowest standard deviation of all points within `residual_threshold` distance from plane (Tarsha-Kurdi, 2007). In a normal regression trying to fit as many points as possible makes sense, but for roof plane fitting we know it is very likely that there will be multiple planes to fit in a given data set, so fitting more is not necessarily better.

The following changes are made based on our own experimentation:

* Ignore very steep slopes - since we don't care about walls and the LIDAR is cropped to the building bounds the steep ones are likely to be false positives.

* Reject planes where the area of the polygon formed by the inliers in the xy plane is significantly less than the area of the convex hull of that polygon. This is intended to reject planes which have cut across a roof and so have a u-shaped intersect with the actual points.

* Reject planes where the `thinness ratio` is too low - i.e the shape of the polygon is very long and thin. The `thinness ratio` is defined as `4 * pi * area / perimeter^2`, and is a standard GIS approach to detecting sliver polygons. Even if these were accurately detected roofs, they're no good for PV panels so we can safely ignore them.

* Using aspect (compass direction) data created as a raster operation from the LiDAR, reject planes where the circular standard deviation of the aspect of each point is too high, or the circular mean of the aspect of each point differs from the aspect of the plane by too much.

* Constrain the selection of the initial sample of 3 points to points whose detected aspect is close, if this is possible.

Planes that have too few pixels fit to them are also rejected.

Any buildings where no roof planes were detected that passed the above tests are now marked as excluded.

## 4. creation of roof polygons

At this stage, for each building we now have a set of roof planes, and each roof plane is associated with a set of 3D points. This needs to be converted into a polygon so that we can model panel placement within each polygon.

The algorithm is as follows:

For each roof plane:
1. If a roof plane is almost aligned with one of the building edges, alter it to align exactly with the orientation of the building.
2. Draw a square around each pixel that has been fit to the plane, rotated to match the aspect of the plane, where the length of the square edge is `sqrt(res^2 * 2)`, so that the squares overlap even for roof planes whose aspect is as far as possible from aligned to a NSEW grid (i.e. 45 degrees from 0).
3. Negatively buffer the polygon formed by the overlapping squares by `-((sqrt(res^2 * 2) - res) / 2)`
4. If this has formed a multi-polygon, take the largest polygon from the multi-polygon.

At this point, larger roofs are mostly ok but smaller roofs (such as on domestic terraces) the polygons created can be too jagged to actually fit a reasonable number of panels in. A secondary algorithm is applied where a series of roof polygon 'archetypes' are tested against the existing roof polygon, and the one which differs least from the roof polygon is chosen to use in place of the roof polygon.

The archetypes are formed by grids of panels in shapes like squares, rectangles, rectangles with various corners cut off, triangles, rectangles with a cutout at the bottom in the centre, and so on. Each archetype is rotated to have the same aspect as the compared roof polygon. Any archetypes whose total area is more than the total area plus a constant of the current roof polygon is discarded. Then they are scored by a metric which tries to minimise a) the area where the archetype does not overlap the roof polygon, and b) the area where the roof polygon does not overlap the archetype, with the former given more weight.

Finally,

1. Enforce the minimum distance to the edge of the roof by intersecting the roof plane with a negatively buffered building polygon
2. Ensure the plane does not overlap with any other plane, giving priority to the most-southerly-facing planes.
3. If this has formed a multi-polygon, take the largest polygon from the multi-polygon.

## 5. panel placement within roof polygons

The panel placement algorithm is fairly simplistic: we drag a grid of portrait-oriented panels and a grid of landscape-oriented panels across the roof polygon and choose the set of panels where the most panels are contained within the polygon. For flat roofs, the spacing to leave between rows of panels is also calculated.

## 6. evaluation of PV suitability

For actual modelling of PV suitability, we use [PVMAPS](https://joint-research-centre.ec.europa.eu/pvgis-online-tool/pvgis-data-download/pvmaps_en), which is a [GRASS GIS](https://grass.osgeo.org/) plugin written in C. This is a raster operation.

TODO

* horizon profile (inc. burn-in of missing buildings)
* pv calc
* spectral correction
* wind correction
* aggregation of raster data to panel-level data