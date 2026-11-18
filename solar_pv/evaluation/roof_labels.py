# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
"""
Clear flat roof labelled by hand from imagery, for scoring mess detection against (see
docs/mess-detection-rework.md).

    # pick flat-roofed buildings from a fast_roof_detection sample: those that scored runs
    # most disagree on, and some at random; and write GeoPackages to label them in
    python -m solar_pv.evaluation.roof_labels sample roofdet_eval/dev_sample.pkl \\
        --runs now=now.pkl holes=holes.pkl --out roofdet_eval/flat_roof_labels.gpkg

    # score runs against the labels (only the labelled buildings are run)
    python -m solar_pv.evaluation.fast_roof_detection score roofdet_eval/dev_sample.pkl \\
        --labels roofdet_eval/flat_roof_labels.gpkg --out now_labelled.pkl

The labels GeoPackage has:

* `building`: each sampled building's footprint, in labelling order (`seq`), with fields to
  fill in: `done` (1 once labelled), `confidence` (0-1, or high, medium or low: how well the
  imagery shows the roof), `mess_kind` and `notes`.
* `clear`: the clear flat roof - flat, and free of plant, rooflights, pipes, vents and the
  like, with no margin around them. It starts as each building's footprint, to be cut down:
  cut out obstacles as holes, and split off and delete pitched or cluttered parts. A building
  with none has no features here.
* `strata`: why each building was sampled (`disagree` or `random`), kept apart so the
  labelling is blind to it.

The pixels GeoPackage (`<out>_pixels.gpkg`) has each building's LiDAR pixels, with their
height above ground and local relief (height above the median of the 5m around them), which
shows low obstacles. It can be remade, so needn't be kept.

Runs are scored on their usable flat roof polygons against the clear flat roof; and, as
imagery can't tell a flat roof from a very shallow pitched one, on their usable polygons up to
`LOW_SLOPE_MAX_DEG` too.
"""
import argparse
import logging
import pickle
from typing import Dict, List, Tuple

import numpy as np
import shapely
from osgeo import ogr
from scipy import ndimage

from solar_pv.evaluation.roof_detection import _Gpkg, _pixel_square
from solar_pv.roof_detection.assign_pixels import grid_index

ogr.UseExceptions()

# A building is a candidate if it has at least this much above-ground pixel area this
# flat, by the LiDAR slope.
CANDIDATE_MIN_FLAT_M2 = 40
CANDIDATE_MAX_SLOPE_DEG = 5
# Larger buildings take too long to label.
CANDIDATE_MAX_FOOTPRINT_M2 = 4000
# A building's runs disagree if their usable flat areas span at least this much.
MIN_DISAGREEMENT_M2 = 20
LOCAL_RELIEF_WINDOW_M = 5
# Buildings labelled with less confidence than this aren't scored.
MIN_CONFIDENCE = 0.5
_CONFIDENCE_WORDS = {"high": 1.0, "medium": 0.7, "low": 0.3}
# Imagery can't tell a flat roof from a very shallow pitched one, so runs are also scored on
# their usable polygons up to this slope.
LOW_SLOPE_MAX_DEG = 10
# Thresholds on a building's clear or model area for it to have any.
NONE_M2 = 5
SOME_M2 = 10


# --- sample ---

def flat_m2(building: dict, resolution_metres: float) -> float:
    """Above-ground pixel area whose LiDAR slope is flat."""
    ground = building["max_ground_height"]
    flat = sum(1 for px in building["pixels"]
               if px["slope"] is not None and px["slope"] <= CANDIDATE_MAX_SLOPE_DEG
               and (ground is None or px["elevation"] > ground))
    return flat * resolution_metres ** 2


def _flat_usable(r: dict) -> float:
    m = r["methods"]["run"]
    return m["usable_area"] * m["flat_share"]


def pick(data: dict, runs: List[List[dict]], n_disagree: int, n_random: int,
         seed: int = 0) -> List[Tuple[str, str, float]]:
    """
    (building id, stratum, spread) for the sampled buildings, in labelling order. Of the
    candidates (see `CANDIDATE_*`), the `n_disagree` whose runs' usable flat areas span most,
    relative to their flat area, and `n_random` of the rest.
    """
    res = data["resolution"]
    by_run = [{r["building_id"]: r for r in rs if r["methods"]} for rs in runs]
    candidates = {}
    for building_id, building in data["buildings"].items():
        if building["polygon"].area > CANDIDATE_MAX_FOOTPRINT_M2 or not all(building_id in b for b in by_run):
            continue
        flat = flat_m2(building, res)
        if flat >= CANDIDATE_MIN_FLAT_M2:
            areas = [_flat_usable(b[building_id]) for b in by_run]
            candidates[building_id] = (flat, max(areas) - min(areas))

    disagree = sorted((b for b, (_, spread) in candidates.items() if spread >= MIN_DISAGREEMENT_M2),
                      key=lambda b: (-candidates[b][1] / candidates[b][0], str(b)))[:n_disagree]
    rest = sorted(set(candidates) - set(disagree), key=str)
    rng = np.random.default_rng(seed)
    chosen = [(b, "disagree") for b in disagree] + \
             [(rest[i], "random") for i in rng.choice(len(rest), min(n_random, len(rest)), replace=False)]
    order = rng.permutation(len(chosen))
    return [(chosen[i][0], chosen[i][1], candidates[chosen[i][0]][1]) for i in order]


def write_labels(path: str, data: dict, picked: List[Tuple[str, str, float]]) -> None:
    R, I, S = ogr.OFTReal, ogr.OFTInteger, ogr.OFTString
    out = _Gpkg(path)
    out.layer("building", ogr.wkbPolygon, [("building_id", S), ("seq", I), ("done", I), ("confidence", S),
                                           ("mess_kind", S), ("notes", S)])
    out.layer("clear", ogr.wkbMultiPolygon, [("building_id", S)])
    strata = out.ds.CreateLayer("strata", geom_type=ogr.wkbNone)
    for name, t in (("building_id", S), ("stratum", S), ("spread_m2", R)):
        strata.CreateField(ogr.FieldDefn(name, t))
    for seq, (building_id, stratum, spread) in enumerate(picked, 1):
        footprint = data["buildings"][building_id]["polygon"]
        out.add("building", footprint, building_id=str(building_id), seq=seq, done=0)
        out.add("clear", footprint, building_id=str(building_id))
        f = ogr.Feature(strata.GetLayerDefn())
        f.SetField("building_id", str(building_id))
        f.SetField("stratum", stratum)
        f.SetField("spread_m2", float(spread))
        strata.CreateFeature(f)
    out.close()


def write_pixels(path: str, data: dict, building_ids: List[str]) -> None:
    R, S = ogr.OFTReal, ogr.OFTString
    res = data["resolution"]
    out = _Gpkg(path)
    out.layer("pixel", ogr.wkbPolygon, [("building_id", S), ("elevation", R), ("height_above_ground", R),
                                        ("local_relief", R), ("slope", R)])
    for building_id in building_ids:
        building = data["buildings"][building_id]
        pixels = building["pixels"]
        xyz = np.array([(px["x"], px["y"], px["elevation"]) for px in pixels])
        relief = local_relief(xyz, res)
        ground = building["max_ground_height"]
        for px, r in zip(pixels, relief):
            out.add("pixel", _pixel_square(px["x"], px["y"], res), building_id=str(building_id),
                    elevation=float(px["elevation"]), local_relief=float(r), slope=px["slope"],
                    height_above_ground=None if ground is None else float(px["elevation"] - ground))
    out.close()


def local_relief(xyz: np.ndarray, resolution_metres: float) -> np.ndarray:
    """Each pixel's height above the median of the `LOCAL_RELIEF_WINDOW_M` around it."""
    grid, rows, cols = grid_index(xyz[:, :2], resolution_metres)
    z = np.full(grid.shape, np.nan)
    z[rows, cols] = xyz[:, 2]
    _, (nearest_rows, nearest_cols) = ndimage.distance_transform_edt(np.isnan(z), return_indices=True)
    window = max(3, int(round(LOCAL_RELIEF_WINDOW_M / resolution_metres)) | 1)
    median = ndimage.median_filter(z[nearest_rows, nearest_cols], size=window, mode='nearest')
    return xyz[:, 2] - median[rows, cols]


# --- score ---

def load_labels(path: str) -> Dict[str, dict]:
    """Per labelled building id: its clear flat roof, confidence, and stratum."""
    ds = ogr.Open(path)
    labels = {}
    for f in ds.GetLayerByName("building"):
        if f.GetField("done") == 1:
            labels[f.GetField("building_id")] = {"clear": [], "confidence": _confidence(f.GetField("confidence")),
                                                 "stratum": None}
    for f in ds.GetLayerByName("clear"):
        label = labels.get(f.GetField("building_id"))
        geom = f.GetGeometryRef()
        if label is not None and geom is not None:
            label["clear"].append(shapely.from_wkb(bytes(geom.ExportToWkb())))
    for f in ds.GetLayerByName("strata"):
        label = labels.get(f.GetField("building_id"))
        if label is not None:
            label["stratum"] = f.GetField("stratum")
    for label in labels.values():
        label["clear"] = shapely.make_valid(shapely.union_all(label["clear"])) if label["clear"] \
            else shapely.Polygon()
    return labels


def _confidence(value) -> float:
    """A labelled confidence as 0-1; none given is full confidence."""
    if value is None or str(value).strip() == "":
        return 1.0
    word = _CONFIDENCE_WORDS.get(str(value).strip().lower())
    return word if word is not None else float(value)


def score_against_label(footprint, polygons: List[dict], label: dict) -> dict:
    """
    A run's usable flat roof polygons (`model_m2`, `hit_m2`), and its usable polygons up to
    `LOW_SLOPE_MAX_DEG` (`low_model_m2`, `low_hit_m2`), against a building's clear flat roof,
    within its footprint.
    """
    clear = label["clear"].intersection(footprint)
    out = {"clear_m2": clear.area, "confidence": label["confidence"], "stratum": label["stratum"]}
    for prefix, keep in (("", lambda p: p["is_flat"]), ("low_", lambda p: p["slope"] <= LOW_SLOPE_MAX_DEG)):
        model = shapely.union_all([p["roof_geom_27700"] for p in polygons if p["usable"] and keep(p)])
        model = model.intersection(footprint)
        out[prefix + "model_m2"] = model.area
        out[prefix + "hit_m2"] = model.intersection(clear).area
    return out


def scored(ms: List[dict]) -> List[dict]:
    return [m for m in ms if m["confidence"] >= MIN_CONFIDENCE]


def summarise(rows: List[Tuple[str, dict]], label: str, prefix: str = "") -> List[dict]:
    """
    Per run, over its (run, label score) rows that are scored: of its usable flat polygons,
    or with `prefix` "low_", of its usable polygons up to `LOW_SLOPE_MAX_DEG`.
    """
    out = []
    for run in dict.fromkeys(run for run, _ in rows):
        ms = [dict(m, model_m2=m[prefix + "model_m2"], hit_m2=m[prefix + "hit_m2"])
              for m in scored([m for r, m in rows if r == run])]
        if not ms:
            continue
        clear, model, hit = (sum(m[k] for m in ms) for k in ("clear_m2", "model_m2", "hit_m2"))
        out.append({
            "buildings": label, "polygons": "low slope" if prefix else "flat", "run": run, "n": len(ms),
            "clear_m2": round(clear), "model_m2": round(model), "hit_m2": round(hit),
            # of the model's usable flat roof, the share that is clear; of the clear, the share it finds
            "precision": round(hit / model, 3) if model else None,
            "recall": round(hit / clear, 3) if clear else None,
            "iou": round(hit / (clear + model - hit), 3) if clear + model - hit else None,
            # buildings with usable flat roof but no clear flat roof, and the other way round
            "false_usable": sum(m["clear_m2"] < NONE_M2 and m["model_m2"] >= SOME_M2 for m in ms),
            "missed": sum(m["clear_m2"] >= SOME_M2 and m["model_m2"] < NONE_M2 for m in ms),
        })
    return out


# --- main ---

def main():
    from solar_pv.evaluation.fast_roof_detection import _load_run

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("sample", help="pick buildings to label, and write GeoPackages to label them in")
    p.add_argument("sample", help="a fast_roof_detection sample")
    p.add_argument("--runs", nargs="+", required=True, help="name=scores.pkl: scored runs of the sample")
    p.add_argument("--out", required=True, help="the labels GeoPackage")
    p.add_argument("--disagree", type=int, default=60)
    p.add_argument("--random", type=int, default=40)
    p.add_argument("--seed", type=int, default=0)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s")

    with open(args.sample, "rb") as f:
        data = pickle.load(f)
    runs = [_load_run(s.split("=", 1)[1]) for s in args.runs]
    picked = pick(data, runs, args.disagree, args.random, args.seed)
    write_labels(args.out, data, picked)
    pixels_path = args.out[:-len(".gpkg")] + "_pixels.gpkg" if args.out.endswith(".gpkg") else args.out + "_pixels"
    write_pixels(pixels_path, data, [b for b, _, _ in picked])
    logging.info(f"Wrote {len(picked)} buildings to {args.out}, and their pixels to {pixels_path}")


if __name__ == "__main__":
    main()
