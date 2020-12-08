# Albion models

This repository contains code for
* Hard/soft dig modelling
* LIDAR downloading
* Heat demand modelling
* Solar PV modelling

This project uses [git-lfs](https://git-lfs.github.com/) for large file storage (currently just the thermos jar for heat demand estimation), so install that and run `git lfs pull` in the root of this repo after cloning.

This project also contains a submodule, so run `git submodule update --init --recursive` after cloning.

## Albion solar PV modelling

required inputs:
* LIDAR data for the relevant area.
* polygon that defines the boundary of the relevant area.
* OS mastermap building data

The main work is done by [PV-GIS](https://ec.europa.eu/jrc/en/PVGIS), which has an HTTP API we talk to. The API documentation is [here](https://ec.europa.eu/jrc/en/PVGIS/docs/noninteractive), with more detail on the parameters [here](https://ec.europa.eu/jrc/en/PVGIS/docs/usermanual). 

Because the PV-GIS API is rate limited to 25 requests a second, we cannot calculate the irradiation for every point in the desired area. A few things we do to cut down the amount of requests:
* Only calculate for lidar pixels that fall within building polygons.
* Rather than doing a request for each lidar pixel, attempt to find the pixels that represent a contiguous area of roof, and only do one request for that area. Our approach to this:
    * Find the aspect (compass facing) of the lidar pixel with [gdaldem aspect](https://gdal.org/programs/gdaldem.html).
    * group the aspect pixels using GDAL's [polygonize](https://gdal.org/programs/gdal_polygonize.html) tool (effectively a flood fill)
* Exclude some of the resulting roof plane polygons if they are unsuitable. The criteria for unsuitability are model parameters: `max_roof_slope_degrees`, `min_roof_area_m`, `min_roof_degrees_from_north`.

Another key component of the model is a [patched version](https://github.com/cse-bristol/320-albion-saga-gis) of [SAGA GIS](http://www.saga-gis.org/en/index.html) which we use to calculate the horizon for each pixel in the lidar data. SAGA calculates this internally but does not normally output it. It has also been patched to take a mask raster as input so that we only calculate the horizons for pixels that fall inside mastermap building polygons.

### Model parameters

* `horizon-search-radius`: Horizon search radius in metres (default 1000)
* `horizon-slices`: Horizon compass slices (default 16)
* `max-roof-slope-degrees`: Maximum roof slope for PV (default 80)
* `min-roof-area-m`: Minimum roof area m² for PV installation (default 10)
* `roof_area_percent_usable`: Percentage of a roof plane usable for mounting panels (default 75)
* `min-roof-degrees-from-north`: Minimum degree distance from North for PV (default 45)
* `flat-roof-degrees`: Angle (degrees) to mount panels on flat roofs (default 10)
* `peak-power-per-m2`: Nominal peak power (kWp) per m² of roof (default 0.120)
* `pv-tech`: PV technology (default crystSi, can be CIS or CdTe)

Increasing the `horizon-search-radius` or `horizon-slices` might slow things down a lot for large areas.

### Future improvements

* Flat roofs are complicated and there are lots of decisions to make on what to do with them - see [this article](https://www.spiritenergy.co.uk/kb-flat-roof-solar-mounting).
  * East-West mounting vs South-facing - mounting pairs of East-West facing panels means not needing to leave gaps between the rows, and can be a better choice.
  * The higher the panels are angled, the bigger the spacing between rows needs to be - so sometimes a less-than-optimal angle is still better as it enables more panels to fit the space.
  * Higher-angled panels might have problems with wind and require a strong roof to be attached to or ballasted on.
  * Installations are often limited by roof strength rather than available area.
  * Sometimes more panels can be fit on the roof if they go in parallel with the building shape, rather than facing due South.

   The current approach treats them as south-facing and has an input parameter for the angle to mount them at (default 10), and the usable area of roof is controlled by the same parameter that controls the usable area of sloped roofs: `roof_area_percent_usable` (default 75 based on the table [here](https://www.thegreenage.co.uk/how-many-solar-panels-can-i-fit-on-my-roof/), could definitely be done better - see next point). Really the usable area should decrease as the angle increases, but the exact amount would depend on how many rows the panels are mounted in.
  
* It currently assumes that panels can fill `roof_area_percent_usable` of each roof plane - However, the polygonisation process very rarely forms `n` rectangles for the `n` roof slopes of the building - things like dormer windows, uneven areas, chimneys and so on all show up, and the polygons are forced to follow the 1mx1m (or 2mx2m) boundaries of the lidar pixels, which align with the cardinal directions, while most houses do not. So it's hard to say whether statistics about how much of a roof can be used for panels also apply to the question about how much of one of these roof planes can be used for panels. 
  
* Sometimes an area we want to model might be interested in putting some PV installations in a field - so there needs to be a way of adding arbitrary extra polygons to the mask so that horizon and irradiation values are calculated for those area.

* Currently we use the built-in efficiency model in PV-GIS. This takes into account type of cell, temperature and irradiation, aging, and losses due to power transformations and cabling. These last two (aging and system losses) are rolled into an API parameter `loss`, which is a percentage with the recommended value being 14. It does not take shading into account but the horizon system should do so, especially as we currently use the highest value from all pixels in a roof plane per horizon slice, to emulate the strong negative effects of partial shading.