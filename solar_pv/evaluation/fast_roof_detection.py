# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
"""
A fast loop for tuning roof detection: runs detection and roof polygons over a sample of
buildings in-process, and scores the polygons as `solar_pv.evaluation.roof_detection` does.
No database, PV or CLI run is needed once the sample is dumped.

    # once per sample: run the model's setup stages, and save each building's detection input
    python -m solar_pv.evaluation.fast_roof_detection dump --buildings sample.gpkg --lidar tiles/ \\
        --out sample.pkl

    # score a method over it (the polygon method is legacy for legacy, otherwise mincut)
    python -m solar_pv.evaluation.fast_roof_detection score sample.pkl --method mincut --out base.pkl
    python -m solar_pv.evaluation.fast_roof_detection score sample.pkl --method mincut --out notrim.pkl \\
        --set solar_pv.roof_detection.detect_roofs._TRIM_ROUNDS=0

    # compare scored runs of the same sample (and, with --buildings, per building)
    python -m solar_pv.evaluation.fast_roof_detection compare base=base.pkl notrim=notrim.pkl

`--set module.NAME=value` overrides a module-level constant in each worker, for trying a
change without editing the code. Each building's RANSAC is seeded as the model seeds it.

It differs from the full evaluation in that: there's no kWh or kWp (usable area is
computed as PV computes it); the pixels are the ones the model loads, not re-read from the
LiDAR; and the polygons are scored straight from detection, not from a CLI GeoPackage.
"""
import argparse
import ast
import importlib
import inspect
import logging
import os
import pickle
import tempfile
import time
import zlib
from concurrent.futures import ProcessPoolExecutor
from typing import List, Tuple
from unittest import mock

import numpy as np
import shapely

from solar_pv import model_solar_pv as msp
from solar_pv.cli import read_buildings, discover_lidar
from solar_pv.ephemeral_postgres import ephemeral_postgres
from solar_pv.evaluation.roof_detection import MESS_REASONS, SIZES, building_context, score_planes, \
    plane_skews, summarise, summarise_skew, markdown_table
from solar_pv.pv.aggregate_pixel_results import roof_and_panel_area
from solar_pv.roof_detection import detect_roofs
from solar_pv.roof_polygons.roof_polygons import create_roof_polygons, _pixel_region

# the model's defaults for the parameters roof polygons take:
_POLYGON_PARAMS = {name: inspect.signature(msp.model_solar_pv).parameters[name].default
                   for name in ("max_roof_slope_degrees", "min_roof_area_m", "min_roof_degrees_from_north",
                                "flat_roof_degrees", "min_dist_to_edge_m")}


# --- dump ---

class _Dumped(Exception):
    pass


def dump(buildings_path: str, lidar_paths: List[str], out: str, id_field: str = None,
         height_field: str = None, pg_uri: str = None) -> None:
    """Run the model up to roof detection, and pickle each building's detection input."""
    def capture(pg_uri, job_id, resolution_metres, **_):
        ids = [building_id for building_id, _ in detect_roofs._buildings_with_areas(pg_uri, job_id)]
        buildings = {}
        for i in range(0, len(ids), 500):
            buildings.update(detect_roofs._load(pg_uri, job_id, ids[i:i + 500]))
        with open(out, "wb") as f:
            pickle.dump({"resolution": resolution_metres, "buildings": buildings}, f)
        logging.info(f"Dumped {len(buildings)} buildings to {out}")
        raise _Dumped()

    def run(pg_uri):
        work = tempfile.mkdtemp(prefix="fast_roofdet_")
        try:
            msp.model_solar_pv(pg_uri=pg_uri, job_id=0, root_solar_dir=os.path.join(work, "solar"),
                               lidar_dir=os.path.join(work, "lidar"),
                               buildings=read_buildings(buildings_path, id_field, height_field),
                               lidar_tiles=discover_lidar(lidar_paths))
        except _Dumped:
            pass

    # model_solar_pv checks this is set, but the dump stops before PV:
    os.environ.setdefault("PVGIS_DATA_TAR_FILE_DIR", "")
    # stopping at detect_roofs keeps the model's own setup stages, rather than a copy of them
    with mock.patch.object(msp, "detect_roofs", capture):
        if pg_uri:
            run(pg_uri)
        else:
            with ephemeral_postgres() as uri:
                run(uri)


# --- score ---

_DATA = None


def _init_worker(sample_path: str, settings: List[Tuple[str, object]]) -> None:
    global _DATA
    with open(sample_path, "rb") as f:
        _DATA = pickle.load(f)
    for target, value in settings:
        module, name = target.rsplit(".", 1)
        setattr(importlib.import_module(module), name, value)


def _score_building(job) -> dict:
    building_id, method, polygon_method = job
    building = _DATA["buildings"][building_id]
    res = _DATA["resolution"]
    footprint = building["polygon"]
    out = {"building_id": building_id, "footprint_m2": footprint.area, "methods": {}, "skew": []}

    start = time.process_time()
    rejected = []
    random_state = np.random.RandomState(zlib.crc32(str(building_id).encode()))
    found = detect_roofs._detect_building_roof_planes(building, building_id, res, method=method,
                                                      random_state=random_state, rejected=rejected)
    pixels_xyz = detect_roofs._pixels_xyz(building["pixels"])
    polygons = create_roof_polygons(building_id, footprint, found, resolution_metres=res, method=polygon_method,
                                    rejected=rejected, pixels_xyz=pixels_xyz, **_POLYGON_PARAMS) if found else []
    out["seconds"] = time.process_time() - start

    xyz = pixels_xyz[shapely.contains_xy(footprint, pixels_xyz[:, 0], pixels_xyz[:, 1])]
    if len(xyz) < 4:
        return out
    ground = building["max_ground_height"]
    above = xyz[:, 2] > ground if ground is not None else np.ones(len(xyz), bool)
    if not above.any():
        return out
    out["n_above"] = int(above.sum())

    planes = [{"geom": p["roof_geom_27700"], "coef": (p["x_coef"], p["y_coef"], p["intercept"]),
               "is_flat": bool(p["is_flat"]), "usable": bool(p["usable"]), "kwh": 0.0, "kwp": 0.0,
               "area": roof_and_panel_area(p["roof_geom_raw_27700"].area, p["slope"], p["panel_slope"],
                                           p["is_flat"])[0]}
              for p in polygons]
    mess = [_pixel_region(r["inliers_xy"], res) for r in rejected
            if r["rejection_reason"] in MESS_REASONS and len(r["inliers_xy"]) > 0]
    metrics, labels, _ = score_planes(xyz, above, res, planes, mess, building_context(xyz, above, res))
    metrics["mess_m2"] = sum(len(r["inliers_xy"]) for r in rejected
                             if r["rejection_reason"] in MESS_REASONS) * res * res
    out["methods"]["run"] = metrics
    out["skew"] = [dict(skew, footprint_m2=footprint.area) for _, skew in plane_skews(xyz, above, labels, planes, res)]
    return out


def score(sample_path: str, method: str, polygon_method: str, settings: List[Tuple[str, object]],
          workers: int) -> List[dict]:
    _init_worker(sample_path, [])
    # biggest first, so a large building doesn't hold up the end of the run
    ids = sorted(_DATA["buildings"], key=lambda b: -len(_DATA["buildings"][b]["pixels"]))
    with ProcessPoolExecutor(workers, initializer=_init_worker, initargs=(sample_path, settings)) as ex:
        return list(ex.map(_score_building, [(b, method, polygon_method) for b in ids], chunksize=4))


# --- compare ---

def _summarise(rs: List[dict], names: List[str], label: str) -> List[dict]:
    rows = summarise(rs, names, label)
    for row in rows:
        row["mess_m2"] = sum(r["methods"][row["method"]]["mess_m2"] for r in rs)
        del row["kwp"], row["mwh"]
    return rows


def compare(runs: List[Tuple[str, List[dict]]], building_ids: List[str] = None) -> str:
    """Summary tables for the named runs, over the buildings all of them scored."""
    names = [name for name, _ in runs]
    by_run = [{r["building_id"]: r for r in rs if r["methods"]} for _, rs in runs]
    common = set.intersection(*[set(b) for b in by_run])
    merged, skew_rows = [], []
    for building_id in sorted(common, key=str):
        first = by_run[0][building_id]
        merged.append({"building_id": building_id, "footprint_m2": first["footprint_m2"],
                       "n_above": first["n_above"],
                       "methods": {name: rs[building_id]["methods"]["run"] for name, rs in zip(names, by_run)}})
        for name, rs in zip(names, by_run):
            skew_rows += [dict(s, method=name) for s in rs[building_id]["skew"]]

    summary = _summarise(merged, names, "all")
    skew_summary = summarise_skew(skew_rows, names, "all")
    for label, lo, hi in SIZES:
        sub = [r for r in merged if lo <= r["footprint_m2"] < hi]
        if sub:
            summary += _summarise(sub, names, label)
            skew_summary += summarise_skew([s for s in skew_rows if lo <= s["footprint_m2"] < hi], names, label)
    seconds = [{"run": name, "cpu_seconds": round(sum(r.get("seconds", 0) for r in rs.values()))}
               for name, rs in zip(names, by_run)]
    tables = [markdown_table(summary), markdown_table(skew_summary), markdown_table(seconds)]

    if building_ids:
        rows = []
        for building_id in building_ids:
            for name, rs in zip(names, by_run):
                m = rs.get(building_id, {}).get("methods", {}).get("run")
                if m is not None:
                    rows.append({"building_id": building_id, "run": name, "planes": m["planes"],
                                 "usable_planes": m["usable_planes"], "usable_area": m["usable_area"],
                                 "explained": m["explained"], "energy_px": m["energy"] / rs[building_id]["n_above"],
                                 "mess_m2": m["mess_m2"], "oversplit_pairs": m["oversplit_pairs"]})
        tables.append(markdown_table(rows))
    return "\n\n".join(tables)


# --- main ---

def _setting(s: str) -> Tuple[str, object]:
    target, value = s.split("=", 1)
    try:
        return target, ast.literal_eval(value)
    except (ValueError, SyntaxError):
        return target, value


def _load_run(path: str) -> List[dict]:
    with open(path, "rb") as f:
        return pickle.load(f)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("dump", help="save a building sample's detection input")
    p.add_argument("--buildings", required=True)
    p.add_argument("--lidar", nargs="+", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--id-field")
    p.add_argument("--height-field")
    p.add_argument("--pg-uri", help="use this database rather than a throwaway one")

    p = sub.add_parser("score", help="score a detection method over a dumped sample")
    p.add_argument("sample")
    p.add_argument("--method", required=True, choices=["legacy", "mincut", "segment"])
    p.add_argument("--polygons", choices=["legacy", "mincut"],
                   help="polygon method (default: legacy for legacy detection, otherwise mincut)")
    p.add_argument("--set", dest="settings", action="append", default=[], type=_setting,
                   metavar="module.NAME=value")
    p.add_argument("--out", help="save the per-building scores here, for compare")
    p.add_argument("--workers", type=int, default=max(os.cpu_count() - 1, 1))

    p = sub.add_parser("compare", help="compare scored runs")
    p.add_argument("runs", nargs="+", help="name=scores.pkl")
    p.add_argument("--buildings", help="comma-separated building ids to show individually")

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s")

    if args.command == "dump":
        dump(args.buildings, args.lidar, args.out, args.id_field, args.height_field, args.pg_uri)
    elif args.command == "score":
        polygon_method = args.polygons or ("legacy" if args.method == "legacy" else "mincut")
        rs = score(args.sample, args.method, polygon_method, args.settings, args.workers)
        if args.out:
            with open(args.out, "wb") as f:
                pickle.dump(rs, f)
        print(compare([(args.method, rs)]))
    else:
        runs = [(name, _load_run(path)) for name, path in (s.split("=", 1) for s in args.runs)]
        print(compare(runs, args.buildings.split(",") if args.buildings else None))


if __name__ == "__main__":
    main()
