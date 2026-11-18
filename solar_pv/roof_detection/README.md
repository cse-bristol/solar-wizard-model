## Roof detection

Finds each building's roof planes from its LiDAR pixels, following PEaRL (Propose, Expand And
Re-estimate Labels; Isack & Boykov, 2012): over-segment the pixels into candidate planes, then
alternately assign pixels to planes and refit the planes, merging and removing planes where
that lowers the energy plus a cost per plane, and proposing new candidates from the pixels no
plane explains well. See section 3 of [documents/pv_model.md](../../documents/pv_model.md) for
the method, and [GLOSSARY.md](../../GLOSSARY.md) for the terms used in the code.

* `detect_roofs.py`: the flow (`_segment_and_assign`), and loading and saving per job.
* `segment_planes.py`: local plane fits per pixel, and region growing into candidate regions.
* `assign_pixels.py`: assigning pixels to planes by alpha-expansion (multi-label min-cut).
* `plane_moves.py`: merging and removing planes by energy.
* `mess.py`: mess on roofs: the mess cost on flat planes, obstacle holes in kept flat roofs,
  and rejecting clutter once pixels are assigned.
* `footprint.py`: the building's faces, which planes' aspects are snapped to.
* `plane_metrics.py`, `plane_fit.py`: plane fits and their fit and shape measures.
* `plane_skew.py`: each final plane's orientation error against its pixels (`meta['skew']`).
* `rejections.py`: the record of planes thrown away, and why.

`solar_pv/evaluation/fast_roof_detection.py` scores detection and roof polygons over a dumped
building sample in a couple of minutes, for trying changes (see
[docs/roof-detection-evaluation.md](../../docs/roof-detection-evaluation.md)).

#### Links:

* [Isack & Boykov, 2012](https://doi.org/10.1007/s11263-011-0474-7) - Energy-based geometric multi-model fitting (PEaRL)
* [Boykov, Veksler & Zabih, 2001](https://doi.org/10.1109/34.969114) - Fast approximate energy minimization via graph cuts (alpha-expansion)
* [Thinness ratio](https://gis.stackexchange.com/questions/151939/explanation-of-the-thinness-ratio-formula)
