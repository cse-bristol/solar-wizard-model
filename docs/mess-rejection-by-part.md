# Mess rejection: judging parts of planes, not whole ones

October 2026. Ideas only; none of this has been tried yet.

## Why

`reject_mess` (`solar_pv/roof_detection/mess.py`) rejects a flat plane as `MESSY_FLAT_ROOF` on two
numbers for the whole plane: its obstacle share (≥ `MESSY_OBSTACLE_SHARE`, 0.14) or its number of
obstacle groups (≥ `max(MESSY_OBSTACLE_GROUPS, MESSY_OBSTACLE_GROUPS_PER_M2 × area)`, i.e.
`max(5, 0.02/m² × area)`). Either rejects every pixel of the plane. So:

* a large plane that is clean apart from one cluttered area is rejected whole;
* small changes to a plane near either threshold flip all of it.

This showed up when trying segment's refine loop with `assign_pixels(connected=True)` (each
alpha-expansion is judged after every plane keeps only its largest part, so a plane is only cut in two
when that's worth its cost). On the dev sample, that change's net loss of usable area all came from 77
buildings whose mess rejections changed. On the rest it gained usable area. Keeping planes whole means
flat planes now span cluttered areas, and are rejected whole:

* 11241: a 923 m² flat plane is rejected at obstacle share 0.31. Without `connected`, it was a 148 m²
  plane plus separate clean pieces that were kept. 13614 and 15744 are similar.
* 12255: a clean 327 m² flat plane (obstacle share 0.09) goes from 6 obstacle groups to 7, crossing
  `max(5, 0.02 × 327 = 6.5)`. The building mess share rising from 0.21 to 0.41 is the result of that
  flip, not its cause: the building rule (`MESSY_BUILDING_SHARE`) only applies at 0.85.

`connected_mess_review.gpkg` (repo root) has these 77 buildings, with and without `connected`. The
`new_mess` layer holds what is newly rejected as mess.

## Options

### 1. Local obstacle density, keeping the clean parts (preferred)

`plane_mess` already has each flat plane's obstacles as a pixel mask (raised and sunken enclosed
groups, residual blobs, and clutter standing on the plane) and knows where each group is. Instead of
summing them over the plane:

1. Compute the obstacle share and the density of obstacle groups over a moving window. The window must
   be about 10m across to apply the group rule at all: 0.02/m² is one group per 50 m². Use normalised
   convolution over the plane's pixels plus its obstacles, so windows at the plane's edge aren't diluted.
2. Mark as messy the plane's pixels where either local value passes today's threshold. Smooth that mask,
   with a morphological closing or a small two-label graph cut (the `assign_pixels` machinery would do),
   so the messy area is compact rather than speckled.
3. Keep each clean connected part above a minimum size as a plane, with the parent's coefficients (no
   refit, so it can't gain an orientation that wasn't checked). Record the messy part as a
   `MESSY_FLAT_ROOF` region, so the evaluation counts it as mess as it does now.
4. If too little of the plane is clean (say under half), reject it whole, as now.

The thresholds keep their meaning; they're just applied locally. "Surrounded by mess" also becomes more
precise, as only the messy parts of planes count as mess around small pitched planes.

To decide: the window size; the minimum size of a kept part; whether several clean parts become
separate planes (which overlaps with splitting planes into their parts at the end of segment, "fix 3"
in the refine-loop work) or only the largest is kept.

### 2. Split the plane, then judge each part with the existing rule

Cut each flat plane into cells, and apply today's check to each:

* a grid (about 10m) aligned to the building footprint; or
* regions grown outwards from the obstacles (watershed on distance to them).

Simpler than option 1, and it reuses everything. But where the cells fall is arbitrary, and a cell
boundary can still flip an outcome.

### 3. A continuous score instead of hard cut-offs

Combine obstacle share and groups per m² into one score, with a band either side of the threshold
rather than a single line. This doesn't save the clean part of a plane like 11241's, but it does fix
cases like 12255. It also works as the local test in option 1.

### Unchanged in all three

Clutter and surrounded-by-mess stay whole-plane checks: they only apply to pitched planes under
`CLUTTER_MAX_AREA_M2` (40 m²). The messy-building rule is unchanged, though its share would count only
the messy parts of planes.

## Next step

Prototype option 1 on the 77 review buildings: add `clean_part` and `messy_part` layers to
`connected_mess_review.gpkg` showing what each rejected flat plane would keep. Then check those against
imagery (start with 11241, 13614, 15744 and 12255) before changing `mess.py`.
