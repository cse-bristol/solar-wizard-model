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

We next fit planes to the LiDAR pixels of each building, treating each pixel as a 3D point (`roof_detection/detect_roofs.py`). Only pixels above the building's ground height are used. The approach follows PEaRL (Propose, Expand And Re-estimate Labels; Isack & Boykov, 2012): propose more candidate planes than are needed, then alternately assign pixels to the planes and refit the planes to their pixels, keeping a plane only if it explains its pixels well enough to be worth having.

### proposing candidate planes

Candidates come from region growing on local plane fits (`roof_detection/segment_planes.py`):

* Each pixel is given a normal from the best-fitting of the nine 3x3 windows that contain it, rather than the window centred on it: at a ridge, hip, eave or step the centred window straddles two planes, but some window lies wholly on the pixel's side. A pixel whose best window fits a plane to within 0.1m is *locally planar*, and its normal is reliable.
* Regions grow from the best-fitting unused pixel, over neighbouring pixels which lie within 0.25m (0.1m for flat regions) of the region's plane, refitted as it grows, and, if their own normal is reliable, whose normal is within 12 degrees of the region's. The distance test separates parallel planes at different heights and stops a pitched region at a ridge; the normal test stops it at shallow creases.

This deliberately over-segments: a region only has to be good enough to start from, as refining lets a plane grow into pixels its region missed, and merging joins regions split by noise.

A region becomes a candidate plane only if it passes these checks:

* It must have at least 8 pixels (4 at 2m). Regions smaller than 0.8% of the building's pixels must be mostly locally planar: on rough surfaces like vegetation and clutter, region growing finds many small regions that fit their plane no worse than real small planes do.
* It must be aligned with the building: a building face (an edge of the footprint), or a right angle to one, must be close to its aspect - or, for a pitched region, to the circular mean of its pixels' aspects from the aspect raster. A pitched region that isn't is still kept if it is at least 15 degrees steep, at least 12 pixels, and corroborated by its locally planar pixels, as footprints derived from LiDAR follow the roof's edges only roughly.
* If it fits its plane poorly (a mean absolute error over half its residual scale), its shape must be reasonable. The area of its pixels must not be much less than that of their convex hull, which rejects planes that cut across a roof and so have a u-shaped intersection with it. Its `thinness ratio` (`4 * pi * area / perimeter^2`, a standard GIS measure of sliver polygons) must not be too low, as long thin planes are no good for panels even if accurately detected.
* If it is pitched, at least half its locally planar pixels must face the same way as it (their normals within 12 degrees of its own).

### refining

Pixels are assigned to planes, or to no plane, by minimising an energy (`roof_detection/assign_pixels.py`):

* each pixel's distance from its plane, in units of the plane's residual scale (0.25m, or 0.1m for flat planes, as flat roofs are more likely to be covered in obstacles like HVAC units, pipes and parapets), capped at 4; or 2 for a pixel in no plane;
* on a flat plane, plus each pixel's mess cost (see *messy roofs*);
* plus 0.5 per metre of boundary between differently assigned pixels, so that planes are compact.

This can't be solved exactly; alpha-expansion (Boykov, Veksler & Zabih, 2001) gets within a factor of 2 of the optimum, each step being one exact min-cut. A plane may only claim pixels within 3m of the ones it has, so it can't take over a coplanar patch elsewhere on the building, and each plane keeps only its largest connected group of pixels.

Each plane is then refitted to its pixels by least squares, then again to just those within its residual scale of it, so that mixed pixels at ridges, eaves and gutters (which at 1m are a large share of a plane only a few pixels deep) don't tilt it. Assignment and refitting alternate until the assignment stops changing, or the energy falls by less than 1% in a round (at most 10 rounds).

Planes are then merged, and removed, where that lowers the energy plus a fixed cost per plane: a plane is only worth having if it explains its pixels better than the other planes would by at least that much. Only planes that are both flat, or that face within 15.5 degrees of each other, are merged; and they are merged even if it raises the energy, if the merged plane is within its residual scale of at least 80% of their pixels, as at 1m a slightly warped roof face fits two planes better than one. Refining after merging and removing can make more planes worth merging (growing them into each other, or flattening them), so merging, removing and refining repeat until nothing more is merged or removed (at most 5 times).

After that, new candidates are proposed from the pixels no plane explains well (other than those of planes just removed, which would mostly grow the same plane again, and those a flat plane would explain but for their mess cost), needing only 4 pixels, and the whole is repeated, up to twice.

Finally, each plane's fit metrics, shape ratios and aspect statistics are computed from its pixels, and its orientation from just its pixels within 0.1m of it.

### aspect and flat roofs

A plane is considered flat if its slope is 4.9 degrees or less (`FLAT_ROOF_DEGREES_THRESHOLD`).

The aspect of a pitched plane is aligned to the azimuth of a building face, or a right angle to one, within 15.5 degrees of the plane's own aspect. Faces are looked for within 1m of the plane's pixels, then 3m, then 10m. A pitched plane with no such face keeps its own aspect. The aspect of a flat plane is not meaningful, so instead it is set to the face direction (or right angle to one) closest to south, or to south if there are no faces within 10m. This is the direction rows of panels on the flat roof are assumed to face.

### messy roofs

Flat roofs are often covered in obstacles like HVAC units, pipes and rooflights, which can lead to roof planes that don't reflect the space actually available for panels (`roof_detection/mess.py`):

* Mess cost: a pixel is rough if the best 3x3 window containing it fits a plane only to within an RMS of 0.02m; clear flat roofs at 1m are mostly within 0.01m. Each pixel costs more on a flat plane the more of the pixels within 5m of it are rough: 8 times the share of them that are rough, over 0.3. Pixels within 2m of the building's edge aren't counted, as their windows often cross a parapet, gutter or wall. As this is part of the energy, the messy parts of a flat roof are left unassigned, and a flat roof that is mostly messy isn't worth a plane. A lone obstacle costs nothing around it.
* Obstacle holes: on each flat roof that is kept, its rough pixels (against the plane itself, extended over the pixels around it, so the walls and planes around it don't count) and the pixels it encloses (plant, rooflights, and the parts the mess cost left out) are cut out of its roof polygon, with a margin of 0.25m (see section 4).
* Clutter: once the planes are final, a small pitched plane within a flat roof is rejected: it is an obstacle (plant, vents, rooflights) that a plane has been fitted to, not a roof.

Any buildings where no roof planes were detected that passed the above tests are now marked as excluded.

## 4. creation of roof polygons

At this stage, for each building we now have a set of roof planes, and each roof plane is associated with a set of pixels. This needs to be converted into a polygon so that we can model PV output within each polygon (`roof_polygons/roof_polygons.py`).

Two polygons are made for each roof plane, both within the building shrunk by `min_dist_to_edge_m`:

* the **grown** polygon, a tidy shape with straight edges, which is what is output.
* the **raw** polygon: the plane's pixels, as squares, within its grown polygon, plus a neighbouring plane's pixels on its side of the line between them, close to it (see below). The plane's area, kWp and kWh are based on this.

Each grown polygon is made as follows:

1. **Lines between planes.** For each pair of planes whose pixels share an edge, a straight line between them is found. If the two planes meet along a line (a ridge, hip or valley) that follows their shared edge, it is that line: its direction comes from the planes' slopes and aspects, and its position from where it best separates their pixels. Otherwise, e.g. for planes side by side at a step, it is whichever line along either plane's grid (see below) best separates their pixels, if that follows their shared edge. A line applies only alongside the shared edge. Where three planes meet, the lines between them are moved to run to the point where all three are at the same height, so that their polygons meet cleanly there. A line whose pixels stop just short of the building's edge, e.g. a ridge at a gable wall, is run to the edge.
2. **Tiling.** Each plane's pixels are tiled on a grid aligned to its aspect, with tiles the size of a pixel, choosing which tiles to occupy by an exact min-cut: each tile costs its mismatch in area with the plane's pixels, plus a cost for each tile edge of boundary, which straightens the polygon's edges. Near a line, the plane may only occupy its own side of it, and gets its neighbour's pixels on its side; elsewhere, it may not occupy other planes' pixels.
3. **Overlaps** between polygons are split along the line between the two planes if they have one, else by which plane's pixels are nearer.
4. **Smoothing.** Boundaries that are not along a line or the building's edge are de-zigzagged, straightening the staircases left by the grid.
5. **Obstacles.** A flat roof's obstacle holes (see *messy roofs*), grown by 0.25m, are cut out of its polygon.

If a polygon has more than one part, only the largest is kept, holes and all. Planes left with no polygon are dropped.

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

Each roof plane is given a `confidence` score between 0 and 1, which is a measure of how well we know the roof surface. It is made up of 6 sub-scores, each between 0 and 1:

* `fit`: how well the plane fits its LiDAR pixels, from its mean absolute error. It is 1 at or below an error of 0.05m, falling on a log scale to 0 at 1m.
* `resolution`: the resolution of the LiDAR used, as finer LiDAR resolves roof detail better.
* `geom_agreement`: the ratio of the area of the raw roof polygon to the area of the grown roof polygon. The closer they are, the less ambiguity there is about the extent of the roof.
* `aspect`: the share of the plane's locally planar pixels (see section 3) whose own normals are within 12 degrees of the plane's. Aspect is meaningless for flat roofs and noisy for shallow ones, so this penalty is not applied at all to flat roofs and fades in linearly to full strength at 20 degrees. It isn't scored if too few of the plane's pixels are locally planar to say.
* `shape`: the thinness ratio of the plane (see section 3), relative to the minimum for a plane of its size.
* `footprint`: how far the aspect of the plane's fit is from the nearest building face direction (or right angle to one): 1 within 15.5 degrees, falling to 0 at 45. Roof detection keeps pitched planes the footprint disagrees with if their pixels corroborate them; this is what the disagreement costs. It fades in with slope as `aspect` does.

These are combined with a weighted geometric mean, leaving out any that aren't known.

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
