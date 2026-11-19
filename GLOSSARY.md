# Glossary

Terms of art used in the code and docs, mostly from roof detection
(`solar_pv/roof_detection/`) and roof polygons (`solar_pv/roof_polygons/`). Defaults quoted
are for 1m LiDAR. Within each section, terms are in alphabetical order.

## The methods

- **segment (roof detection)**: how roof planes are found: region growing, then refining
  PEaRL-style (see [Segment roof detection](#segment-roof-detection)). It replaced two other
  methods, *legacy* (DETSAC then RANSAC, tidied up by greedy region merging) and *mincut*
  (the same, then pixel assignment), which older docs and evaluation runs compare it with.
- **mincut (roof polygons)**: how planes are turned into polygons (`_make_roof_polygons` in
  `roof_polygons.py`), named for the min-cut in `tile_polygon` (see
  [Mincut roof polygons](#mincut-roof-polygons)). It replaced a *legacy* method that grew and
  squared up each plane's pixels.
- **min-cut**: used in two places, each an exact max-flow over a graph of pixels or tiles:
  each alpha-expansion step in `assign_pixels`, and tile occupancy in `tile_polygon`.

## General

- **aspect, `aspect_raw`**: `aspect_raw` is the azimuth (degrees clockwise from north) the
  fitted plane faces. `aspect` is that snapped to the building's faces where one is close
  (see *snapping*), and is what PV uses. Flat planes are given an aspect near south (180).
- **building faces, footprint**: the edges of the building's footprint polygon, indexed by
  `footprint.FaceIndex`. Their azimuths, and right angles to them, are the directions a plane
  can be snapped to.
- **coefficients, `coef`**: a plane is `z = x_coef·x + y_coef·y + intercept`. In segment's
  loop, `Plane.coef` is the tuple `(x_coef, y_coef, intercept)`.
- **confidence**: a 0–1 score per plane (`solar_pv/pv/confidence.py`): the weighted
  geometric mean of the known sub-scores (fit, aspect, shape, resolution, geometry agreement,
  footprint). The aspect sub-score comes from `facing_share`, not the raster aspect.
- **dev sample**: the 2,037-building sample that `fast_roof_detection` runs over in about 2
  minutes; the usual place to measure a change before a full-area run.
- **flat, `is_flat`**: a plane whose slope is at most `FLAT_ROOF_DEGREES_THRESHOLD` (4.9°).
  Panels on it are mounted as on a flat roof: tilted at `flat_roof_degrees`, facing south
  (its final `aspect`), whichever way it falls. Not to be confused with *level*.
- **level, `is_level`**: a plane whose slope is at most `LEVEL_ROOF_DEGREES_THRESHOLD` (2.5°):
  laid flat, with at most drainage falls, as opposed to a shallow pitched roof (mostly
  profiled sheeting, at about 5–6°). It decides the plane's residual scale, what it can merge
  with, what it snaps to while proposing, and which mess checks apply: in roof detection and
  mess, a "flat roof" is a level plane. Every level plane is also flat.
- **inliers, `inliers_xy`**: a plane's pixels (their x, y): those assigned to it.
- **locally planar**: a pixel whose best 3×3 window fits a plane within `_MAX_LOCAL_RMS`
  (0.1m). Its local normal is then *reliable*.
- **local fit, local normal**: per pixel, the normal and RMS of the best-fitting of the nine
  3×3 windows that contain it (`segment_planes.local_fits`), rather than the window centred on
  it: at a ridge, hip, eave or step the centred window straddles two planes, but some window
  lies wholly on the pixel's side.
- **mixed pixels**: pixels at a ridge, eave, valley or gutter whose elevation averages two
  surfaces. At 1m, on faces only a few pixels deep, they're a large share of a plane's pixels
  and tilt a least-squares fit; hence *trimming*.
- **raster aspect**: the per-pixel aspect from the aspect raster (`aspect`,
  `aspect_circ_mean`, `aspect_circ_sd`). Noisy on shallow faces, and at 1m mostly from windows
  straddling a ridge or eave, so roof detection uses it only as evidence that a plane follows
  the building, not for which way it faces.
- **rejected planes, rejection reasons**: planes thrown away are recorded with a reason
  (`rejections.py`) and written to the CLI's `rejected_roof_plane` layer, for debugging.
- **residual scale**: the distance (m) from a plane at which a pixel stops being well
  explained: `_RESIDUAL_THRESHOLD` 0.25m for pitched planes, `_LEVEL_ROOF_RESIDUAL_THRESHOLD`
  0.1m for level ones (`_residual_scale`). It's the inlier threshold, the unit of the data
  cost, and region growing's distance limit.
- **snapping, snapped aspect**: setting a plane's aspect to the nearest building-face
  direction (or right angle to one), searching for faces within 1m, then 3m, then
  `_FOOTPRINT_SEARCH_M` (10m) of its pixels (`_snapped_aspect`). A plane is *aligned* if a
  face is close to its own aspect, or (if pitched) to its pixels' mean raster aspect; it's
  only ever snapped to the former. A level plane is snapped towards south while proposing, and
  a flat one when finalised, as that's the way its panels face.
- **usable**: a roof polygon PV is modelled on: not too steep, not too north-facing, and at
  least the minimum area (`_set_usability`); otherwise `not_usable_reason` says which.

## Segment roof detection

The flow is in `_segment_and_assign` (`detect_roofs.py`); see `docs/segment-doglegs.md`.

- **candidates**: the pixels a proposal round may grow regions from: above ground, not
  explained, and not tabu.
- **energy**: what `assign_pixels` minimises: the summed data cost of every pixel, plus the
  smoothness cost of every boundary. Segment adds a label cost per plane.
- **explained**: a pixel within its plane's residual scale (data cost ≤ 1) and, if locally
  planar, whose normal is within `_MAX_NORMAL_ANGLE_DEGREES` of the plane's (`_explained`).
  The facing test stops a small plane absorbed by a bigger one, like a dormer's side, from
  counting as explained. *Explained share*, the evaluation measure, is the residual part alone.
- **facing share**: of a plane's locally planar pixels, the share whose normals are within
  12° of the plane's (`_facing_share`); None if fewer than `_MIN_FACING_PIXELS` are locally
  planar. Gates pitched regions (`NORMALS_DISAGREE`), and is stored for confidence.
- **finalise**: turning each loop-only `Plane` into a `RoofPlane` once refining is done
  (`_finalise`): full fit metrics, a fit trimmed to `_FINAL_TRIM_RESIDUAL`, facing share, and
  the snapped aspect. A pitched plane with no close face keeps its own aspect; a flat one faces
  south.
- **gates**: the hard checks a region must pass to become a candidate plane (`_propose`,
  `_implausible`): `SMALL_POOR_FIT`, `NO_CLOSE_ASPECT`, `CONVEX_HULL_RATIO`, `THINNESS_RATIO`
  and `NORMALS_DISAGREE`. They only ever see proposed regions, never the planes those become.
- **good-enough merge**: a merge of two planes accepted even though it raises the energy,
  because the merged plane explains at least `_GOOD_ENOUGH_MERGE_SHARE` (80%) of their pixels
  (is within its residual scale of them), and `plane_moves.MIN_SHARE_EACH` (50%) of each one's:
  at 1m a slightly warped face fits two planes better than one. A share rather than a mean
  cost, as a plane can average under the residual scale while fitting a large share of its
  pixels poorly; and of each plane too, or a large plane would take any small neighbour
  (plant, a roof at another level), however badly it fits it.
- **label cost**: `_LABEL_COST` (8.0), the energy a plane must save to be worth having. Not
  yet scaled with resolution.
- **merge passes**: merging and removing planes, then refining, repeated until a pass changes
  nothing, up to `_MAX_MERGE_PASSES` (5) per proposal round: refining can grow planes into each
  other, or flatten them, and so make them mergeable.
- **mergeable**: two planes that could be one roof face: both level, or facing within
  `AZIMUTH_ALIGNMENT_THRESHOLD` of each other (`_mergeable`). Only mergeable pairs are
  considered for merging.
- **off-footprint**: a region or plane not aligned with any building face. At proposal it's
  kept only if at least `_MIN_OFF_FOOTPRINT_SLOPE` (15°) steep, `_MIN_OFF_FOOTPRINT_PIXELS`
  (12) big, and corroborated by enough locally planar pixels; otherwise rejected with `NO_CLOSE_ASPECT`.
- **over-segment**: region growing deliberately finds too many regions. Each only has to be
  good enough to start from; refining lets planes grow into pixels their region missed, and
  merging rejoins regions split by noise.
- **PEaRL**: Propose, Expand And Re-estimate Labels (Isack & Boykov 2012), the scheme segment
  follows: propose candidate models, then alternate assigning data to them and refitting them,
  with a per-model label cost deciding which models survive.
- **`Plane`**: the loop-only plane type (`plane_moves.Plane`): coefficients, `is_level`,
  provenance, its pixels when last refitted, and `proposed`, a snapshot of the metrics it was
  proposed with (for rejection records). Nothing else is computed until *finalise*.
- **plane moves**: changes to the set of planes that pixel assignment can't make itself,
  judged by energy plus label cost (`plane_moves.py`): *merge* (`merge_by_energy`, greedily,
  best gain first) and *remove* (`remove_unprofitable`, least profitable first).
- **profit**: how much more a plane's pixels would cost on their next best option (a
  neighbouring plane, or no plane) than on it. A plane whose profit is under the label cost is
  removed as `UNPROFITABLE`.
- **proposal, re-proposal**: growing regions and turning those that pass the gates into
  planes (`_propose`). The first proposal uses all above-ground pixels and needs regions of 8
  px; up to `_MAX_PROPOSAL_ROUNDS` (2) re-proposals grow from the pixels still not explained,
  and need only `_MIN_REPROPOSED_PIXELS` (4), as the energy then decides whether they're
  worth keeping.
- **refine**: alternating pixel assignment and refitting until labels stop changing, the
  energy falls by less than `_MIN_REFINE_ENERGY_FALL` (1%) a round, or after
  `_MAX_REFINE_ROUNDS` (10) (`_refine`, `_reassign`). Not a descent method, as the refit
  doesn't minimise the energy.
- **region, region growing**: a connected set of pixels grown from a seed over 4-neighbours
  that lie within the residual scale of the region's plane (refitted as it grows) and, if their
  normal is reliable, whose normal is within 12° of the region's (`segment_planes.py`). The
  distance test separates parallel planes at different heights and stops at ridges; the normal
  test stops at shallow creases.
- **seed**: the pixel a region starts from: the best-fitting (lowest local RMS) pixel not yet
  in a region. Pixels with no reliable normal are seeds of last resort.
- **small pixels**: 0.8% of the building's pixels (`small_pixels`). Regions smaller than this
  must be mostly locally planar, or are rejected as `SMALL_POOR_FIT`: on rough surfaces
  (vegetation, clutter) growing finds many small regions that fit no worse than real small
  planes do.
- **tabu**: pixels of planes removed since the last proposal are left out of the next
  proposal's candidates (`removed`), as a region regrown from them is mostly removed again.
  Planes can still grow into them while refining.
- **trimmed fit, trimming**: a least-squares fit, then refitted (up to `_TRIM_ROUNDS`, 3) to
  just the pixels within the residual scale of it, if at least `_MIN_TRIM_SHARE` (half) of
  them are (`_fit`). Keeps mixed pixels from tilting the plane. *Finalise* trims tighter, to
  `_FINAL_TRIM_RESIDUAL` (0.1m).

## Pixel assignment

`assign_pixels.py`, used when refining.

- **alpha-expansion**: the move `assign_pixels` makes: for one label α, an exact min-cut
  decides which pixels switch to α. Cycling through every label until none lowers the energy
  gets within a factor of 2 of the optimum (Boykov, Veksler & Zabih 2001).
- **allowed**: a plane may only claim pixels within `max_dist_m` (3m) of the pixels it starts
  with, so it can't take over a coplanar patch elsewhere on the building.
- **connected**: each plane keeps only its largest 4-connected group of pixels, and a move is
  only made if it lowers the energy after that. So a plane is only cut in two if that's worth
  losing the smaller part.
- **data cost**: a pixel's cost on a plane: its elevation residual over the plane's residual
  scale, capped at `RESIDUAL_CAP` (4). In no plane, it costs `NONE_COST` (2).
- **NONE, `labels_nodata`**: the label for a pixel in no plane (-1).
- **Potts model**: the energy's form: a fixed smoothness cost for each pair of 4-neighbours
  with different labels, however different.
- **smoothness**: `SMOOTHNESS_PER_M` (0.5) per metre of boundary between labels, so scaled by
  1/resolution per pixel edge.

## Mess

`mess.py`; see also `docs/mess-detection-rework.md`.

- **clutter**: a small (< `CLUTTER_MAX_AREA_M2`, 40m²) pitched plane within a flat roof: an
  obstacle (plant, vents, rooflights) a plane has been fitted to, not a roof. Rejected once
  the planes are final (`reject_clutter`; `CLUTTER`).
- **mess cost**: mess as part of the detection energy (`mess.MessCost`; `MESS_WEIGHT` 8, 0
  turns it off): each pixel's extra cost on a flat plane, `MESS_WEIGHT` × the share of rough
  pixels within `MESS_WINDOW_M` (5m) over `MESS_FREE_SHARE` (0.3). Pixels within
  `MESS_EDGE_M` (2m) of the building's edge don't count, and the share is of at least
  `MESS_MIN_COUNTED` (0.5) of a window. Assigning pixels leaves a flat roof's messy parts
  unassigned (`MESSY_PART`), and a mostly messy roof is emptied or removed.
- **messy flat roof**: a flat plane emptied or removed for its mess cost (`MESSY_FLAT_ROOF`).
- **messy part**: pixels of a flat roof left out for mess, but not a whole plane: those
  unassigned because they cost more on the flat plane that explains them than unassigned,
  for their mess cost; and a kept flat roof's own pixels cut out as obstacle holes.
  Recorded as `MESSY_PART`.
- **obstacle holes**: on each flat roof that's kept, its rough pixels (against the plane,
  extended over the pixels around it, and not within `MESS_EDGE_M` of the building's edge)
  and the pixels it encloses (`mess.obstacle_holes`; `MESS_HOLES`, on). Where they cut its
  pixels in pieces (4-connected) of at least `min_pixels`, each piece is a plane, with the
  orientation of the whole, and the rest of its pixels go to the nearest piece
  (`detect_roofs._split_at_obstacles`). They are cut out of its roof polygon with a margin of
  `OBSTACLE_SETBACK_M` (0.25m) (`roof_polygons._cut_obstacles`), as holes; where that still
  cuts the polygon in pieces, only the largest is kept.
- **rough pixel**: a pixel whose local fit (see **locally planar**) has an RMS over
  `MESS_ROUGH_RMS` (0.02m). On clear flat roofs at 1m the median is about 0.005m, and 6% of
  pixels are rough.

## Mincut roof polygons

`_make_roof_polygons` in `roof_polygons.py`; see `docs/mincut-polygon-tuning.md`
and `docs/mincut-polygon-junctions.md`. Distances are mostly in **tiles**, one tile being one
pixel's width (`tile_m`).

- **additions**: a neighbour's pixels on a plane's side of a line between them, within the
  line's zone, which are added to the plane's region (`_exclusions`).
- **arc**: a piece of the noded polygon boundaries between two junctions; the unit
  de-zigzagging works on, so neighbours stay consistent.
- **core**: a strip `CORE_HALF_WIDTH_TILES` (2) either side of a line, alongside its shared
  edge, within which the line alone decides which of the two planes owns what, whichever plane
  the pixels were assigned to. Outside it, a plane's own eroded region is never excluded. Core
  pixels on a plane's side count towards its raw area (`_raw_regions`), and are filled into
  its polygon (*core fill*) so tiling doesn't leave notches along the line.
- **de-zigzagging, smoothing**: straightening pixel staircases in the boundaries (`_smooth`):
  segments along the building outline or a line, and any at least `SMOOTH_MIN_KEPT_TILES`
  (2.5) long, are kept; between them each segment is replaced by its midpoint, then
  Douglas-Peucker-simplified to `SMOOTH_TOLERANCE_TILES` (0.5). Polygons are rebuilt from the
  faces of the smoothed arcs.
- **direction sensitivity**: how many degrees a meeting line turns if either plane's aspect is
  5° or its slope 2° out (`_direction_sensitivity`). A line is *stable* if that's at most
  `MAX_DIRECTION_SENSITIVITY_DEG` (10°): ridges and hips are; same-facing planes at a step
  aren't.
- **exclusions**: per plane, the areas it may not occupy (`_exclusions`): other planes'
  regions eroded by half a tile, and the far side of each line it has, within the line's zone
  and core.
- **legal area**: where a plane's polygon may go: the shrunk building minus its exclusions,
  but with its own side of each of its lines' cores excluded only by other planes
  (`_legal_area`).
- **line**: a straight boundary between two touching planes, found per pair (`_find_lines`):
  a meeting line if one follows their shared edge, else a step line. It applies only alongside
  the shared edge plus `LINE_END_TILES` (0.5) past each end, unless it runs to a node or wall
  point. `_Line` while being found and moved, `_MeetingLine` (zone, side, core) once
  restricted.
- **meeting line**: the line where two planes meet, i.e. their intersection
  (`_intersection_line`): direction from the planes' gradients (from the snapped aspect),
  offset from the best separation of their pixels (`_fit_line`). Ridges, hips and valleys.
- **mismatch**: an evaluation measure: the share of polygon area not covered by the plane's
  own pixels.
- **node**: where three planes, two of whose pairs have meeting lines, are all at the same
  height (`_join_at_nodes`, `_triple_point`): the lines around that junction are moved
  parallel to run to it, so the polygons meet in a clean Y. Only placed if well conditioned
  (`NODE_MAX_SENSITIVITY_TILES`) and close to the lines' ends (`NODE_MAX_SHIFT_TILES`,
  `NODE_MAX_PAST_END_TILES`, `NODE_MAX_SHORT_TILES`). Nodes on the same line end are merged
  where four or more planes meet. Also called a *triple point*.
- **on-line test, strict tests**: whether a line follows the shared edge
  (`_follows_shared_edge`, `_fit_line`): the shared pixels span at least
  `MEETING_MIN_SHARED_TILES` (3) along it, `MEETING_MIN_ON_LINE` (90%) of them are within
  `MEETING_MAX_LINE_DIST_TILES` (1.5) of it, and at most `MEETING_MAX_MISCLASSIFIED` (10%) are
  clearly on the wrong side. Stable meeting lines get a looser on-line test
  (`STABLE_MAX_LINE_DIST_TILES` 2, `STABLE_MIN_ON_LINE` 80%).
- **owned pixels, `owned_xy`**: a plane's own pixels (`inliers_xy`), before lines give it
  any of its neighbours'.
- **perimeter λ**: `PERIMETER_LAMBDA` (0.6), the min-cut cost per tile edge of boundary in
  `tile_polygon`, in tiles of area. Higher gives fewer notches but polygons that spread over
  pixel-less area and drop thin arms.
- **raw area, `roof_geom_raw_27700`**: the plane's pixels as squares within its polygon, plus
  core pixels on its side of its lines. The plane's area, kWp, kWh and minimum-area check are
  based on this, not on `roof_geom_27700`.
- **region**: a plane's pixels as a union of pixel squares (`_pixel_region`).
- **sector fill**: after tiling, each sector between two consecutive lines at a node, within a
  tile of it, is given to the plane both lines bound (`_fill_nodes`), as tiling drops the tips
  of narrow sectors.
- **shared pixels, shared edge**: of two touching planes, the pixels of each that are
  edge-adjacent to the other. Planes separated by unassigned pixels, or touching only at a
  corner, share nothing.
- **shrunk building**: the building footprint buffered inwards by `min_dist_to_edge_m`; the
  outer limit of every polygon.
- **side, `side_1`**: the half-plane of a line's zone on the first plane's side of it.
- **split overlaps**: making the polygons disjoint after tiling (`_split_overlaps`): an
  overlap goes to each side of the pair's line if they have one, else to the plane with the
  nearer pixel. Done on one noding of all boundaries, so neighbours share edges exactly.
- **step line**: for planes that touch without meeting along their intersection, e.g. side by
  side at a step: a line along one of either plane's grid axes, whichever separates their
  pixels with fewest wrong (`_step_line`). Accepted with the strict tests only; otherwise
  treated like a meeting line.
- **tile, tiling, tile-quantize**: `tile_polygon` (`occupancy.py`) chooses which cells of a
  grid aligned to the plane's aspect are occupied, by an exact min-cut: each tile's cost is its
  area mismatch with the region (measured within the legal area), plus perimeter λ per tile
  edge of boundary. The grid's phase is searched too. Ported from the ukbf regularizer.
- **wall point**: the end of a line extended to the shrunk building's edge, when its shared
  pixels stop within `WALL_MAX_TILES` (1.5; `MEETING_WALL_MAX_TILES`, 3, for meeting lines such
  as hips) of it with nothing in the way (`_extend_to_walls`), e.g. a ridge to a gable wall.
  Rejected if either plane carries on across the extension (`_wrong_side_past_end`).
- **zig-zag**: an evaluation measure: a boundary's length over its length after
  Douglas-Peucker simplification at 0.75 tiles. 1.0 is straight; a 45° staircase is about 1.4.
  Measured separately for *shared* edges (with another plane) and *free* edges (next to
  nothing).
- **zone**: where a line applies: within 2 tiles of its shared pixels, and alongside them
  rather than past their ends (`_restrict`). Near a node it's clipped to the sectors either
  side of the line (`_clip_at_nodes`).

## Evaluation

`solar_pv/evaluation/`; see `docs/roof-detection-evaluation.md`. None of these measures is
ground truth, apart from the clear flat roof labels: judge differences by imagery too.

- **clear flat roof**: flat roof free of anything you wouldn't put a panel over, labelled by
  hand from imagery, with no margins (`roof_labels.py`; see `docs/mess-detection-rework.md`).
  Runs' usable flat roof polygons are scored against it on precision, recall and IoU.
- **explained share**: the share of above-ground pixels within the residual scale of their
  plane.
- **fast loop**: `fast_roof_detection`, which runs detection and polygons in-process over a
  dumped sample; `--set module.CONSTANT=value` overrides module constants.
- **missed planar**: m² of planar patches in pixels no plane covers, not counting those a mess
  rejection explains.
- **over-split pairs**: adjacent planes that look like one face split in two: their fits are
  within 5° of each other, and one plane fitted to both is within 2cm on MAE and explains
  under 5 percentage points fewer of their pixels.
- **skew, tilt error, contamination**: how far a plane's stored orientation is from the plane
  implied by the pixels its polygon covers (`plane_skew.py`, stored in `meta['skew']`).
  *Contamination* is the angle between that robust plane and a plain least-squares fit to the
  same pixels: pixels of a neighbouring face or clutter. A plane is *significantly skewed* if
  its tilt is off by at least 2° and by at least 3 standard errors.
