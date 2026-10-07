# PV model documentation

The solar PV suitability model has the following main stages:

1. setup
2. detection of outdated LiDAR
3. roof plane detection: fitting of planes to LiDAR points
4. creation of roof polygons
5. evaluation of PV suitability

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

We next try to fit planes to the LiDAR data, which is treated as a set of 3D points for this purpose. To do this we use a modified version of an algorithm called RANSAC (RAndom SAmple Consensus), which is a standard approach in the literature for detecting roofs in LiDAR data, and DETSAC, a deterministic variant of it (see below). DETSAC is run first, and RANSAC is then used to look for any planes DETSAC has missed.

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

* Use a tighter `residual_threshold` (0.1m) for flat planes, as flat roofs are more likely to be covered in obstacles like HVAC units, pipes, and parapets.

Planes that have too few pixels fit to them are also rejected.

All of these changes apply to DETSAC as well, apart from the constraint on the selection of the initial sample, as DETSAC does not sample.

### DETSAC

RANSAC is not deterministic: it begins by taking 3 points at random, and if it gets unlucky with this choice it can produce bad results. This happens most often on challenging roofs, like narrow terraces with shallow roofs. DETSAC replaces the random sampling with a set of candidate planes made in advance for each building:

* The points are grouped by height using DBSCAN, so that parts of the roof at very different heights are separated.
* Each connected region of similar height is segmented by aspect, using SLIC superpixels which are then merged where their aspect is similar (a region adjacency graph cut). This is done at two thresholds, 29 degrees and 15 degrees.
* A plane is fitted to the points of each segment, and each of these is a candidate plane.

Each candidate plane is also given a `sample_residual_threshold` of either 0.25m or 2m. The points of the segment the candidate was made from count as inliers if they are within this distance of the plane, even if they are further than `residual_threshold` from it. This helps on roofs where the segment the plane was fitted to is not entirely on the plane itself.

DETSAC then runs like RANSAC, except that instead of trying random samples it tries every candidate plane, and keeps the best one that passes the checks above. A candidate plane that fails any of the checks other than the score is not tried again for that building. Points already fit to another plane are never considered inliers. As with RANSAC, the found plane's points are removed, and DETSAC is run again until no more planes are found.

If more than 25% of the building's points have not been fit to a plane by DETSAC, RANSAC is then run on the remaining points.

Planes with a mean absolute error of 1m or more are discarded, and their points are left to be picked up in the next step.

### merging and messy roofs

After detection, adjacent planes are merged, and so are planes and the adjacent points that were not fit to any plane. This is done using a region adjacency graph where each node is either a plane or a single unfitted point. Two nodes are merged if a plane fitted to the points of both is still a good fit.

Flat roofs are often covered in obstacles like HVAC units and pipes, which can lead to roof planes that don't reflect the space actually available for panels. To detect these, groups of unfitted points that do not touch the edge of the building are treated as obstacles. A flat plane is rejected as messy if the obstacles it touches make up a large proportion of its size, or if it touches many separate obstacles. If too much of the building is either obstacles or messy flat planes, the whole building is rejected.

Any buildings where no roof planes were detected that passed the above tests are now marked as excluded.

### flat roofs

A plane is considered flat if its slope is 4.9 degrees or less (`FLAT_ROOF_DEGREES_THRESHOLD`).

The aspect of a pitched plane is aligned to the azimuth of a nearby building edge, which must be within 15.5 degrees of the plane's aspect. The aspect of a flat plane is not meaningful, so instead it is set to the azimuth of whichever nearby building edge faces closest to south, which must be within 46 degrees of south. This is the direction rows of panels on the flat roof are assumed to face. Planes where no suitable building edge is found are rejected.

## 4. creation of roof polygons

At this stage, for each building we now have a set of roof planes, and each roof plane is associated with a set of 3D points. This needs to be converted into a polygon so that we can model PV output within each polygon.

Two polygons are made for each roof plane:

* the **raw** polygon, which is as close as possible to the pixels RANSAC actually fit to the plane. A square is drawn around each pixel fit to the plane, the squares are merged, and the result is de-zigzagged (each vertex is moved halfway towards the next, which smooths the staircase effect of the pixel edges). If this has formed a multi-polygon, the largest polygon is taken.

* the **grown** polygon, which is a tidier shape based on the raw polygon. If a bounding box rotated to match the aspect of the plane is close in area to the raw polygon, the bounding box is used. Otherwise, the raw polygon is gridded onto a 1m grid oriented to the aspect of the plane, and the result is de-zigzagged.

Both polygons are constrained to the building, with a minimum distance to the edge of the building of `min_dist_to_edge_m`.

Then,

1. Pitched roof planes that touch, have the same aspect, and have slopes within 4 degrees of each other are merged, and their polygons re-made. Flat roof planes are never merged.
2. Overlaps between grown polygons are removed by splitting the overlapping area evenly between the two planes, using the straight line between the points where their boundaries cross if that doesn't leave the overlap, or else a Voronoi-based centre-line.
3. The raw polygon is clipped to its grown polygon.

Finally, planes are marked unusable if they are steeper than `max_roof_slope_degrees`, if their aspect is within `min_roof_degrees_from_north` of north, or if their area is below `min_roof_area_m`.

## 5. evaluation of PV suitability

For actual modelling of PV suitability, we use a native Python (numpy/GDAL) port of the [PVMAPS](https://joint-research-centre.ec.europa.eu/pvgis-online-tool/pvgis-data-download/pvmaps_en) `r.pv` algorithm. PVMAPS is a [GRASS GIS](https://grass.osgeo.org/) plugin written in C; the port reproduces its calculations without GRASS being needed. See the README for how closely the port's results match the original. This is a raster operation.

The meteorological data is the UK subset of the PVMAPS `pvgis_data.tar`, reprojected to EPSG:27700 and packaged as `pvgis_data_uk.tar`.

### horizon detection

To begin with, a horizon profile is detected for each pixel. This basically uses ray-tracing in `horizon_slices` compass slices (36 by default), up to `horizon_search_radius` metres away (1000m by default), to work out the degree to which the horizon is blocked in each slice. A flat horizon would be 0 degrees and a wall that stretched to the point directly above your head would be a 90 degree horizon. (the numbers are actually in radians though).

Horizon detection takes the LiDAR elevation raster as an input, which for this case has been modified so that any buildings where the LiDAR has been detected as outdated are burned-in to the raster for the sake of horizon-blocking. 

Horizons are only calculated for pixels within buildings, but every pixel of the elevation raster is still considered as horizon-blocking - an addition to the standard [r.horizon](https://grass.osgeo.org/grass82/manuals/r.horizon.html) - which can use a mask but then stops masked pixels being considered as horizon-blocking.

### slope and aspect

Within usable roof planes, the slope and aspect rasters created at setup are replaced with the slope and aspect of the roof plane, so that every pixel in a plane is modelled at the same orientation. For flat roofs the slope used is the tilt that panels are assumed to be mounted at (`flat_roof_degrees`, 10 degrees by default), rather than the slope of the roof itself.

### r.pv

At this point, the port of the `r.pv` module from PVMAPS is run. This is a modified version of the [r.sun](https://grass.osgeo.org/grass82/manuals/r.sun.html) solar irradiation model. See the [documentation for PVMAPS](https://re.jrc.ec.europa.eu/pvmaps/pvmaps.pdf) for more information. PVMAPS takes irradiation levels, temperature, slope and aspect of panel, and historical cloud cover into account when modelling PV output.

The daily solar irradiation and predicted kWh are calculated for 12 days of the year, one in the middle of each month. The monthly and yearly values are then calculated from these representative days. For each of these 12 days, in 15-minute steps:
* The Linke turbidity and real-sky beam and diffuse coefficients for that month from the PVMAPS data are used as input. These rasters have a resolution of roughly 1.6km by 2.5km and include the effect of cloud coverage.
* the position of the sun is modelled in concert with the horizon profile generated previously to calculate the actual levels of insolation for that 15-minute time period.
* the effect of temperature on PV outputs is also modelled, using 3-hourly air temperature data at the same resolution.

Insolation and PV output for the whole day are then calculated from the 15-minute steps.

The model parameters used depend on `pv_tech`, which is either `crystSi` (crystalline silicon) or `CdTe` (cadmium telluride).

### wind correction

See section 4.3 in the [documentation for PVMAPS](https://re.jrc.ec.europa.eu/pvmaps/pvmaps.pdf) for information on the method used.

### spectral correction

See section 4.4 in the [documentation for PVMAPS](https://re.jrc.ec.europa.eu/pvmaps/pvmaps.pdf) for information on the method used.

### aggregation from pixels to roof planes

Finally the corrected annual kWh, monthly kWh, and horizon profile for each pixel are aggregated to roof-plane-level facts.

kWh figures are calculated over the raw roof polygon, as this is the area actually fitted to the LiDAR. The horizon profile is averaged over the pixels intersecting the grown roof polygon.

PVMAPS models each pixel as if it was a 1kWp installation, so each intersecting pixel's output is adjusted by:
* the configured output per m^2 (`peak_power_per_m2`)
* the proportion of which the pixel overlaps the roof polygon
* the configured system loss (14%, which covers losses due to cabling, the inverter, and degradation with age)

and then scaled by the panel area of the roof plane.

The roof area is the planar area of the raw polygon, corrected for the slope of the roof. On pitched roofs, panels are assumed to be flush-mounted and to cover the whole roof, so the panel area is the same as the roof area.

### flat roofs

On flat roofs, panels are assumed to be mounted in rows tilted at `flat_roof_degrees`, facing the aspect of the roof plane (see section 3). The rows need to be spaced apart so that each row doesn't shade the one behind it. Rows are spaced so there is no shading while the sun is above 15 degrees (`FLAT_ROOF_ROW_SPACING_SUN_ELEVATION_DEGREES`), which gives a ground cover ratio (panel area per m^2 of flat roof) of:

`1 / (cos(tilt) + sin(tilt) / tan(sun elevation))`

which at the default tilt of 10 degrees is about 0.61. The panel area of a flat roof is its planar area multiplied by this ratio, and kWp and kWh are based on that panel area.

### P90

`kwh_year` is a central (P50) estimate for a typical meteorological year. `kwh_year_p90` is a conservative estimate: a poor-weather year that real generation would still exceed ~90% of the time. It is calculated as:

`kwh_year * (1 - Z_P90 * INTERANNUAL_GHI_COV)`

where `Z_P90` is the one-sided 90th-percentile z-score of a normal distribution (1.282) and `INTERANNUAL_GHI_COV` is the coefficient of variation of UK annual global horizontal irradiation between years (5%). This gives a derate of about 6.4%.

This only accounts for variation in the weather between years - it does not account for uncertainty in the roof geometry or in the model itself. That is covered (qualitatively) by the confidence score.

### confidence

Each roof plane is given a `confidence` score between 0 and 1, which is a measure of how well we know the roof surface. It is made up of 5 sub-scores, each between 0 and 1:

* `fit`: how well the plane fits the LiDAR points, from the mean absolute error of the RANSAC fit. It is 1 at or below an error of 0.05m, falling on a log scale to 0 at the maximum error roof detection accepts (1m).
* `resolution`: the resolution of the LiDAR used, as finer LiDAR resolves roof detail better.
* `geom_agreement`: the ratio of the area of the raw roof polygon to the area of the grown roof polygon. The closer they are, the less ambiguity there is about the extent of the roof.
* `aspect`: the circular standard deviation of the aspect of the pixels fit to the plane, scoring 0 at the maximum roof detection accepts. Aspect is meaningless for flat roofs and noisy for shallow ones, so this penalty is not applied at all to flat roofs and fades in linearly to full strength at 20 degrees.
* `shape`: the thinness ratio of the plane (see section 3), relative to the minimum roof detection accepts for a plane of its size.

These are combined with a weighted geometric mean.

Each sub-score is floored at 0.05 before being combined, so that a single sub-score of zero puts a roof at the bottom of the ranking without discarding what the other sub-scores say about it.

### skew

Each roof plane's `meta` also has a `skew` entry, set by roof detection for every plane, usable or not: how far the plane's stored orientation is from the plane implied by the LiDAR pixels within its grown roof polygon (the polygon that is output, and that the horizon is averaged over). It isn't part of the confidence score yet.

The residuals of those pixels from the stored plane are regressed on x and y. The slope terms are the tilt error, the constant is the height offset at the pixels' centroid, and what is left is roughness. This separates out what the MAE mixes together: a small plane can fit to within a few cm and still be several degrees out, and a cluttered roof can have a high MAE and the right orientation - and only the orientation affects kWh/kWp. The implied plane is a robust fit (least trimmed squares, then Tukey biweight), so it is the plane most of the pixels lie on rather than a compromise with pixels of a neighbouring face or of clutter. It must fit at least 3/4 of the pixels: on a plane only 3 or 4 pixels deep, a plane through half of them is just two rows, which can be fitted almost exactly at the wrong tilt, leaving out the mixed ridge or eave rows. So up to 1/4 of the pixels can be off the plane. Where more are, the polygon straddles two faces and there is no dominant plane (see `roughness_m`).

* `tilt_err_deg`: the angle between the stored and implied planes.
* `tilt_z`: the tilt error relative to its standard error. The standard error is a jackknife over one-pixel-wide bands, across and along the slope: how much the tilt moves when a band is left out. The residuals of a small plane aren't independent noise (a ridge or eave row is offset as a whole), so a standard error that assumes they are makes small planes' tilts look far more certain than they are. Null if the pixels are fewer than 3 bands deep across the slope.
* `bands`: how many pixels deep across the slope the plane is.
* `offset_m`: the height of the implied plane above the stored one, at the pixels' centroid.
* `roughness_m`: the spread of the pixels about the implied plane. Where a polygon straddles two faces roughly equally there is no dominant plane, and the implied plane is an average of the two which fits neither: a high roughness shows this.
* `contamination_deg`: the angle between the implied plane and a least-squares fit to all the pixels: how far pixels that aren't on the plane would pull it.
* `outlier_share`: the share of pixels off the implied plane.
* `implied_slope`, `implied_aspect`: the implied plane's orientation.
* `pixels`: the number of pixels.

`skew` is null where there are fewer than 10 pixels, or they don't span at least two pixels in x and in y.
