# Roof detection evaluation: legacy vs mincut vs segment

October 2026. This compares the three roof detection methods (`--roof-detection-method`) over one
5km OS tile: ST57se, south Bristol. It has 32,881 buildings, with 1m LiDAR, from
`~/git/uk-buildings/out/buildings_final.gpkg` and `~/git/uk-buildings/data/dsm/`.

| name | detection | polygons | run |
|---|---|---|---|
| legacy | legacy | legacy | `results-legacy-eval.gpkg`, 2026-10-01 |
| mincut | mincut | mincut | `results-mincut-eval.gpkg`, 2026-10-01 |
| segment | segment | mincut | `results-segment.gpkg`, 2026-09-30 |

All three runs used the same, uncommitted, working tree. Nothing in roof detection, polygons or PV
changed between the segment run and the other two. Earlier outputs (`results2.gpkg`,
`results-mincut-with-rejects2.gpkg`) were not used: they predate a schema change and changes to
PV area and kWh, and to the mincut code.

## Method

`solar_pv/evaluation/roof_detection.py` scores each run's output against the same LiDAR. It
takes the pixels whose centres are in each building's footprint, from the elevation the model
builds, and drops those at or below the building's max ground height. Each pixel gets the plane
whose output polygon contains its centre, and is scored against that plane:

```shell
python -m solar_pv.evaluation.roof_detection --lidar ~/git/uk-buildings/data/dsm/ --out-dir eval/ \
    segment=results-segment.gpkg mincut=results-mincut-eval.gpkg legacy=results-legacy-eval.gpkg
```

(The runs used here predate the CLI exporting ground heights, so they were scored with `--pg-uri`
pointing at a `--keep` run's database.)

The measures:

* **explained**: share of above-ground pixels within 0.25m of their plane (0.1m if it's flat).
* **MAE**: mean absolute residual of the pixels covered by a polygon.
* **energy**: what `assign_pixels` minimises (residual over scale, capped at 4; 2 per pixel in
  no plane; 0.5 per metre of boundary), plus 8 per plane as `plane_moves` charges.
* **missed planar**: m² of planar patches in pixels no plane covers, not counting those a mess
  rejection explains.
* **skew**: per plane, how far its stored orientation is from the plane implied by its own
  pixels (see `solar_pv/roof_detection/plane_skew.py` and the skew section of `documents/pv_model.md`).
* **oversplit pairs**: adjacent planes that look like one face split in two. Their own fits are
  within 5° of each other, and one plane fitted to both is within 2cm on MAE and explains under 5
  percentage points fewer of their pixels. The other measures reward splitting, so this checks for
  it. It was added after the runs below, so it isn't in their tables.

None of this is ground truth: these measures reward explaining the LiDAR, which the mess
checks deliberately don't do. Segment minimises much the same energy, so it has some home
advantage on that measure, and the plane cost doesn't fully stop splitting from being rewarded.
Explained share, MAE and skew don't favour any method.

The polygons scored are the exported (grown) roof polygons, which are what users see and what
the horizon is averaged over. kWh and area come from each plane's raw geometry: detection's
pixels clipped to the polygon. That geometry isn't exported, and it is close to the pixels the
plane was fitted to. So a polygon that spills over a neighbouring surface affects the outline
and horizon more than it affects kWh.

### Fast loop for tuning

A CLI run and evaluation of a sample takes about 15 minutes. `solar_pv/evaluation/fast_roof_detection.py`
does the same scoring in about 2 minutes on a 2,000-building sample, with no database or PV. It
dumps each building's detection input once, then runs detection and polygons in-process:

```shell
python -m solar_pv.evaluation.fast_roof_detection dump --buildings sample.gpkg --lidar ~/git/uk-buildings/data/dsm/ \
    --out sample.pkl
python -m solar_pv.evaluation.fast_roof_detection score sample.pkl --out base.pkl
python -m solar_pv.evaluation.fast_roof_detection score sample.pkl --out notrim.pkl \
    --set solar_pv.roof_detection.detect_roofs._TRIM_ROUNDS=0
python -m solar_pv.evaluation.fast_roof_detection compare base=base.pkl notrim=notrim.pkl --buildings 15507,13754
```

`--set` overrides a module constant in each worker, so a change can be tried without editing the
code. Scores match the full evaluation's, except that there's no kWh (usable area is computed as PV
computes it). Confirm a kept change with a full CLI run and evaluation, and on the full tile.

## Results

### Fit to the LiDAR

| | segment | mincut | legacy |
|---|---|---|---|
| planes (usable) | 100,758 (71,475) | 89,419 (65,492) | 92,019 (68,923) |
| explained | **66.4%** | 61.2% | 53.7% |
| MAE (m) | **0.143** | 0.155 | 0.352 |
| energy / pixel | **1.22** | 1.29 | 1.54 |
| buildings where lowest energy | **22,038** | 13,992 | 3,211 |
| missed planar (m²) | **71k** | 166k | 112k |
| usable area (m²) | 2.40M | 2.27M | 2.17M |
| kWh / year | 373 GWh | 356 GWh | 348 GWh |

By footprint size, energy / pixel (and explained share):

| | segment | mincut | legacy |
|---|---|---|---|
| < 50 m² (7,919) | **1.37** (71%) | 1.40 (66%) | 1.48 (64%) |
| 50–200 m² (22,273) | **1.35** (69%) | 1.39 (64%) | 1.55 (61%) |
| 200–1000 m² (2,183) | **1.14** (67%) | 1.20 (63%) | 1.50 (55%) |
| > 1000 m² (506) | **1.07** (61%) | 1.18 (54%) | 1.56 (39%) |

On buildings over 1,000 m², segment misses 44k m² of planar roof against mincut's 115k. Its usable
area there is 15% higher and its kWh 14% higher. That fits an earlier finding: mincut's RANSAC
(and legacy's) stops proposing planes once fewer than 25% of pixels are left, so it misses big
clean planes on large buildings.

Legacy's plane fits are fine: the median of each plane's median residual is 0.058m, against
0.053m for segment and 0.063m for mincut. But its grown polygons spill onto other surfaces. 7.7%
of the pixels its polygons cover are more than 1m off the plane, against 2.7% for the others.
That is where its high MAE comes from. Legacy also finds no planes at all on many large, clean
industrial roofs (e.g. 11022, 17170, 28821, 14913; `NO_ROOF_PLANES_DETECTED`), which is most of
its shortfall in kWh.

### Agreement between methods

| | pixel agreement | kWh correlation | median per-building kWh difference |
|---|---|---|---|
| segment–mincut | 84% | 0.974 | 4.6% |
| segment–legacy | 72% | 0.915 | 10.5% |
| mincut–legacy | 77% | 0.929 | 8.1% |

### Skew

Pixel-weighted. A plane counts as "significantly skewed" if its tilt is off by at least 2° and
the tilt error is at least 3 standard errors.

| | segment | mincut | legacy |
|---|---|---|---|
| tilt error, median / 90th percentile | **0.6° / 3.0°** | 0.7° / 3.4° | 1.0° / 6.1° |
| pixels in significantly skewed planes | **6.2%** | 7.8% | 8.0% |
| pixels in planes with >5° contamination | 18% | 18% | 39% |

On buildings over 1,000 m², legacy's 90th-percentile tilt error is 5.0°, against 1.1° for segment
and 1.5° for mincut. On buildings under 200 m², legacy has the smallest share of pixels in
significantly skewed planes (about 7%, against 9–12% for the others), but its contamination is
much higher.

**Small planes.** At 1m a plane is often only 3 or 4 pixels deep across its slope, and its ridge
and eave rows are mixed pixels: part roof, part ridge, gutter or wall. The first version of the
metric flagged many small, correct-looking roofs, for two reasons:

* Its robust fit started from least median of squares, which on so few rows finds a plane through
  two of them that fits half the pixels almost exactly, at the wrong tilt.
* Its standard error assumed independent residuals, but an offset ridge or eave row is a
  systematic error.

On a sample of small flagged mincut planes, detection's raw geometry differed from the exported
polygon by at most one pixel, and 95% of them were still flagged on the raw geometry. So the
polygon stage wasn't the cause. The fit now has to explain 3/4 of the pixels, and the standard
error is a jackknife over pixel-wide bands. That cut the share of 10–40-pixel planes flagged at 5° or more
from about 10% to about 3%, and left large planes about as before. Planes less than 3 bands deep
(about 7% of planes) get no significance at all. Even so, tilt on small planes is poorly
determined: on the small planes the first version flagged, plain least squares and the stored
plane differ by a median of 2.5°.

Planes significantly skewed by at least 5° now number 1.4k for segment, 2.3k for mincut and 3.4k
for legacy, nearly all under 40 pixels. Legacy has 709 that are rough (over 0.15m), against 32 for
segment and 45 for mincut. Rough means the polygon straddles two faces, so the implied plane is an
average of them rather than either. Those rough polygons head each method's `tilt` review list:
they're wrong outlines, not wrong orientations.

Aspect snapping moves the PV orientation by a median of under 1°. A few large usable planes are
snapped far from a tightly determined fit. For example, mincut 28821 plane 525 is snapped 53°:
its pixels say aspect 55°, PV gets 146°, on 32 MWh/year. The snap is `aspect` against
`meta['aspect_raw']`; `tilt_z` says whether the fit is tightly determined.

### Most divergent buildings

`divergent_buildings.csv` and `review.gpkg` list the top 50 buildings by three measures (143
buildings in all):

* **absolute kWh spread**: almost all large industrial roofs. E.g. 11022 (22,000 m²) gets 918 /
  598 / 0 MWh from segment / mincut / legacy, and 11036 gets 1,049 / 331 / 606 MWh with 61 / 9
  / 9 planes. Segment has the lowest energy on 40 of these 50.
* **relative kWh spread** (mean at least 1,000 kWh): small buildings that one method gives a
  usable plane and the others don't, mostly because a mess or usability check went the other
  way. Legacy has the lowest energy on 19 of these 50, its best showing.
* **least pixel agreement**: mostly one method rejecting a roof that the others keep.

`skew_review.csv` and the `skewed_plane` layers list, per method, the 100 most skewed significant
planes.

## Conclusions

1. **Segment is the best of the three on every LiDAR measure, at every building size.** It
   explains more of the LiDAR, with lower MAE and energy and less skew. The gains are biggest on
   large buildings, where it finds clean planes that RANSAC's stopping rule leaves behind. It
   gives 6% more usable area and 5% more kWh than mincut overall, and 14% more kWh on buildings
   over 1,000 m².
2. **Legacy is clearly worst, and the case for removing it holds.** Its polygons spill onto
   other surfaces, its orientations are off most often, and it misses whole large flat roofs.
   It was best on only about 10% of buildings, mostly small ones where a different keep/reject
   decision happened to suit the LiDAR.
3. **Segment and mincut mostly agree.** Pixel agreement is 84% and the median kWh difference is
   4.6%. Most of the remaining difference is in large buildings, plus keep/reject decisions on
   small ones.
4. **What these measures can't tell us.** Segment has 13% more planes than mincut, and 38% more
   usable planes on buildings over 1,000 m². The measures can't say whether those extra planes
   are real, or whether segment explains LiDAR that the mess checks would rather reject (its
   missed-planar area in mess rejections is the same as mincut's). That needs imagery review of
   the large divergent roofs before relying on segment's extra kWh.
5. **Skew is a useful extra signal, but weak on small planes.** It separates orientation errors,
   which affect kWh/kWp, from noise and clutter. It ranks the methods the same way as the other
   measures, and it picks out polygons that straddle two faces (mostly legacy's) and large snaps
   against tightly determined fits. On planes only a few pixels deep, tilt can't be determined
   much better than to a few degrees, whichever method finds them. It is stored per plane in
   `meta['skew']`. It is not part of `confidence`; `tilt_err_deg` when `tilt_z` is high would
   be the candidate once it's been checked against imagery.

**Limits:** one tile, of one (urban, mixed) area, at 1m resolution. 50cm and 2m data are untested.

## Known problem: segment's refine loop loses pixels after merges and cuts

Segment alternates refining (reassigning pixels to planes and refitting them) with merging and
removing planes, and ends on a refine. Two things made refining lose pixels that it then had to win
back, so that the result depended on which round the loop stopped in.

**Merges were undone by the next assignment.** `merge_by_energy` judges a merge by `_merged_fit`'s
plane, refitted to just the pixels it would be assigned. But `assign_pixels` refitted every plane by
least squares to all its starting pixels, so a merged plane was a worse fit in the next assignment than
the merge had assumed. In 28821, each merge roughly doubled the energy (about 4.4k to 10.4k). On the
dev sample, the first refine round after a merge lost 60k pixels in all, and 16 refine calls hit the
10-round cap while winning them back (12375 among them). Now `assign_pixels` takes the planes' own
coefficients (`coefs`), and each refine round refits them to just their pixels near them
(`_trim_plane_coefficients`). That cut the first-round loss to 12k pixels and the capped calls to one.

**Cuts.** Each refine kept only the largest 4-connected part of each plane's pixels. When one plane
grew across another and cut it in two, the smaller part was left unassigned, whatever that cost.
Re-proposal and merging would then rejoin it, and the next refine cut it off again. Now `assign_pixels`
can keep planes connected itself (`connected`): it makes an alpha-expansion only if it lowers the energy
after each plane keeps its largest part, and retries a move that cuts a plane without taking any of that
plane's pixels. So a plane is only cut in two when that's worth losing the smaller part.

Dev sample (2,037 buildings):

| | explained | MAE (m) | missed planar (m²) | mess (m²) | oversplit pairs | usable area (m²) | CPU (s) |
|---|---|---|---|---|---|---|---|
| before | 0.639 | 0.112 | 14.0k | 68.6k | 30 | 382.2k | 1670 |
| planes' own coefficients | 0.639 | 0.107 | 14.5k | 68.0k | 20 | 380.6k | 1363 |
| ... and connected | 0.634 | 0.109 | 11.9k | 74.1k | 22 | 379.3k | 1774 |

The coefficients change also cut the planes significantly skewed from 128 to 105. Keeping planes
connected loses 1.9k m² of usable area in the 77 buildings whose mess rejections changed (by at least 20
m²), and gains 0.5k m² in the rest. With fewer accidental cuts, flat planes stay whole across cluttered
areas, and `reject_mess` rejects a whole plane on its overall obstacle share or count. In 11241, a 923
m² flat plane is rejected at obstacle share 0.31, where before a 148 m² plane and separate clean pieces
were kept. In 12255, a clean 327 m² flat plane's obstacle groups go from 6 to 7, crossing the threshold
of 6.5 for its size. So the next step is for mess rejection to judge parts of planes (see
`mess-rejection-by-part.md`). `connected_mess_review.gpkg` has the 77 buildings, for imagery review.

Not yet revisited: the off-footprint limits in `_implausible` (`_MIN_OFF_FOOTPRINT_SLOPE`,
`_MIN_OFF_FOOTPRINT_PIXELS`), which keep out the shallow and small regions that most often cut flat
roofs.

Tried and reverted (October 2026, dev sample):

* Splitting each cut-off part of at least 3 pixels into its own plane, with or without merging and
  reassigning once more at the end. Splitting left coplanar pieces apart (oversplit pairs 30 to 69); a
  final merge left merged planes with pixels they no longer fitted (MAE 0.139); reassigning after that
  lost too many (missed planar 21.2k m²). Split parts were also refitted without going through
  `_implausible`.
* Returning the refine round with the lowest energy, rather than the last. Explained fell to 0.633 and
  usable area by 5k m²: the energy zigzags down between rounds, and this rolled back to earlier,
  smaller states. Without the early stop it was about neutral, at 48% more CPU. It can't be applied
  across merges either, as merges whose plane is good enough are made even if they raise the energy.

## Next steps

* Imagery review of the top of `divergent_buildings.csv` (`review.gpkg`), particularly segment's
  extra planes on large roofs: are they real faces or over-splitting?
* Look at large snaps (`aspect` against `meta['aspect_raw']`) on tightly determined fits (high
  `tilt_z`): should snapping be refused when it moves the aspect far from a well-determined fit?
* Check the remaining significantly skewed small planes against imagery, to see whether the
  metric's threshold is right.
* Once these have been reviewed, consider adding skew to `confidence`.
* Make mess rejection judge parts of planes, not whole ones (see `mess-rejection-by-part.md`), then
  revisit the off-footprint limits.

Legacy and mincut detection, and legacy polygons, have since been removed, leaving segment
detection with mincut polygons. The evaluation tools now compare runs rather than methods.
