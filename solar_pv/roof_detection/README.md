## RANSAC for LIDAR 

Extensions to scikit-learn RANSAC implementation with additions for usage with LIDAR to detect roof planes.

#### Profiling

```shell
py-spy record --idle -F -o profile.svg -- python3 -m solar_model.solar_pv.ransac.dev_ransac
```
Produces a flamegraph SVG

#### Changes made:

* Tarsha-Kurdi, 2007 recommends rejecting planes where the (x,y) points in the plane do not form a single contiguous region of the LIDAR. This mostly helps but does exclude some valid planes where the correctly-fitted plane also happens to fit to other pixels in disconnected areas of the roof. I have modified it to allow planes where a small number of non-contiguous pixels fit, as long as the area ratio of those non-contiguous pixels to the area of the main mass of contiguous pixels is small.

* Do not optimise for number of inliers (points within `residual_threshold` distance from plane), instead optimise for lowest SD of all inliers' distance from plane (Tarsha-Kurdi, 2007). In a normal regression trying to fit as many points as possible makes sense as you want to fit one line to your whole dataset, but for roof plane fitting we know it is very likely that there will be multiple planes to fit in a given data set, so a plane fitting more points is not necessarily a better one.

* Forbidding very steep slopes (not sourced from a paper) - since we don't care about walls and the LIDAR is cropped to the building bounds the steep ones are likely to be false positives.

* Constrain the selection of the initial sample of 3 points to points whose detected aspect is close (not sourced from a paper). Aspect has already been detected by SAGA during horizon detection - it isn't perfect but it is nearly always right, which is a good enough starting point.

* Reject planes where the area of the polygon formed by the inliers in the xy plane is significantly less than the area of the convex hull of that polygon. This is intended to reject planes like the blue one in the second image below. (not sourced from a paper)

* Reject planes where the `thinness ratio` is too low - i.e the shape of the polygon is very long and thin. The `thinness ratio` is defined as `4 * pi * area / perimeter^2`, and is a standard GIS approach to detecting sliver polygons. Even if these were accurately detected roofs, they're no good for PV panels so we can safely ignore them.

#### Changes considered:

* Running it 3 times for each building and taking the best result (measured by smallest average standard deviation of dist. of inliers from plane for all roof planes found) improves the likelihood of good results, but takes that much longer. And it still sometimes finds crappy planes! There might be a better score to use than SD for 'best' result.

* Excluding very shallow planes - It did improve results before the convex hull ratio check was added, but that functioned better without excluding genuinely flat roofs.

#### Thoughts:

In general the results are better than the results of the existing polygonisation approach. However, the existing approach is fully deterministic, whereas RANSAC is not. It begins by taking 3 points at random and fitting a plane to those points. If it gets unlucky with this choice it can produce bad results. This will sometimes ocurr even with buildings it mostly gets right, but far more often with some of the more challenging roofs, like narrow terraces with shallow roofs. Larger buildings and buildings with simple roofs do not seem to go wrong.

The final 3 changes above lower the rate of bad results to an acceptable level, mainly by constraining the choice of the initial 3 points to ones that are likely to be good, and rejecting any planes that seem to cut through a roof.

There are several aspects to the quality of a roof segmentation tactic:
* Does it get the polygon bounds right? RANSAC is mostly better, but not always..
* Does it get the slope of the roof right? RANSAC is better - it nearly always detects the two opposite slopes of a building's roof as having the same degree, which the existing approach often gets wrong by even as much as 10 degrees.
* Does it get the aspect right? Both approaches tend to get the aspect right.

The 9 pointclouds used for testing (in folder `inputs`) were extracted from 1m LIDAR DSM - as recommended in Tarsha-Kurdi, 2007, as it normalises the distribution of points.
 
Processing them takes 0.6 seconds per building on average. This is a lot slower than the previous polygonisation approach taken. They could be run in parallel. There might be room for optimisation in the algorithm too. The `max_trials` variable could also be reduced, but at the cost of worse results.

#### Links:

* [Tarsha-Kurdi, 2007](https://www.isprs.org/proceedings/XXXVI/3-W52/final_papers/Tarsha-Kurdi_2007.pdf) - Hough-transform and extended RANSAC algorithms for automatic detection of 3d building roof planes from LIDAR data
* [Scikit-learn RANSAC implementation](https://scikit-learn.org/stable/modules/generated/sklearn.linear_model.RANSACRegressor.html)
* [Thinness ratio](https://gis.stackexchange.com/questions/151939/explanation-of-the-thinness-ratio-formula)
* [RANSAC for LIDAR](https://github.com/cse-bristol/ransac-lidar) - Repository where this work was started, README has some images of badly-detected roofs using a more standard RANSAC.