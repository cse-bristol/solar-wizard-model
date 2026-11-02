# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
"""
Score roof polygons, rather than the planes they're made from: how cleanly neighbouring
polygons meet, and how closely they follow their planes' pixels. A fast loop for tuning the
roof polygon stage: detection's planes are saved once, and each run reruns only the polygons.

    # once: save detection's planes, from a fast_roof_detection sample...
    python -m solar_pv.evaluation.roof_polygons planes --sample sample.pkl --method segment \\
        --out planes.pkl
    # ...or from a job's schema (e.g. a CLI run with --pg-uri and --keep)
    python -m solar_pv.evaluation.roof_polygons planes --pg-uri postgresql://... --job-id 0 \\
        --out planes.pkl

    # score runs of the polygon stage over them, and compare
    python -m solar_pv.evaluation.roof_polygons score planes.pkl --out base.pkl --gpkg review.gpkg
    python -m solar_pv.evaluation.roof_polygons score planes.pkl --out lam1.pkl --gpkg review.gpkg \\
        --set solar_pv.roof_polygons.roof_polygons.PERIMETER_LAMBDA=1
    python -m solar_pv.evaluation.roof_polygons compare base=base.pkl lam1=lam1.pkl

A job's schema only has the planes that got a polygon, so planes the polygon stage dropped
in that run are missing from it.

`--set module.NAME=value` overrides a module-level constant in each worker. `--gpkg` adds a
layer of the run's polygons to a GeoPackage, named after the run (`--name`, by default the
`--out` file's name), and a `<name>_triple` layer of its triple points.

The measures (see `polygon_metrics`):
  triples sharing a point    triple points (see `triple_points`) where the three planes'
                             polygons have a point in common, and the mean distance from
                             that to the triple point
  holes                      holes in the union of a building's polygons, over 0.01 tiles²
  touching pairs unshared    pairs of planes whose pixels touch along an edge, but whose
                             polygons share less than 0.1 tiles of boundary
  edges reaching the wall    shared boundaries of at least a tile that reach the building's
                             shrunk edge
  vertices per polygon
  zig-zag, shared / free     boundary length over its length simplified to 0.75 tiles, for
                             boundary shared with another polygon / neither shared nor on the
                             building's edge
  raw area                   the planes' raw geometry, which their kWp and kWh are from
  mismatch                   share of polygon area outside its raw geometry
  overlap                    area in more than one polygon (should be 0)
None of these is ground truth; they say how tidy the polygons are, and the polygons' fit to
the LiDAR is `fast_roof_detection`'s and `roof_detection`'s job.
"""
import argparse
import copy
import logging
import os
import pickle
import time
import traceback
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from itertools import combinations
from typing import Dict, List, Optional, Tuple

import numpy as np
import shapely
from osgeo import ogr, osr
from shapely.geometry import CAP_STYLE, JOIN_STYLE, Point

from solar_pv.evaluation import fast_roof_detection
from solar_pv.evaluation.fast_roof_detection import _POLYGON_PARAMS, _setting, apply_settings
from solar_pv.roof_polygons import roof_polygons as rp

ogr.UseExceptions()
osr.UseExceptions()

# the key each plane's index in the building is kept under, through the polygon stage:
_INDEX = "eval_plane_index"
# lengths and areas below these are noding noise rather than a shared edge or hole:
_EPS_M = 1e-3
_HOLE_MIN_TILES2 = 0.01
_UNSHARED_MAX_TILES = 0.1
_EDGE_MIN_TILES = 1
_ZIGZAG_TOLERANCE_TILES = 0.75

Polygons = List[Tuple[int, object, object, bool]]


# --- planes ---

def _shrunk(footprint, min_dist_to_edge_m: float):
    """The building's edge as the polygon stage keeps polygons inside it."""
    return footprint.buffer(-min_dist_to_edge_m, cap_style=CAP_STYLE.square, join_style=JOIN_STYLE.mitre)


def triple_points(footprint, planes: List[dict], res: float,
                  min_dist_to_edge_m: float) -> List[Tuple[Tuple[int, int, int], Tuple[float, float]]]:
    """
    Where three planes meet, as (plane indices, point): each pair's pixels touch, at least
    two pairs along a line where they meet rather than a step line (see `rp._find_lines`),
    and the point where all three are at the same height (`rp._triple_point`) is
    well-conditioned and inside the building. These are found once, when the planes are
    saved, so that every run is scored at the same triple points.
    """
    heights = [rp._mean_height(p) for p in planes]
    if len(planes) < 3 or any(h is None for h in heights):
        return []
    owned = [p["inliers_xy"] for p in planes]
    regions = [rp._pixel_region(o, res) for o in owned]
    lines, touching = rp._find_lines(planes, owned, regions, res)
    gradients = [rp._gradient(p) for p in planes]
    origin = np.mean([m for m, _ in heights], axis=0)
    inside = _shrunk(footprint, min_dist_to_edge_m).buffer(res)

    meeting = defaultdict(set)
    for (i, j), line in lines.items():
        if line.meets:
            meeting[i].add(j)
            meeting[j].add(i)
    triples = sorted({tuple(sorted((i, j, k))) for i in meeting for j, k in combinations(sorted(meeting[i]), 2)})
    out = []
    for triple in triples:
        pairs = list(combinations(triple, 2))
        if any(pair not in touching or (pair in lines and not lines[pair].meets) for pair in pairs):
            continue
        meets = [pair for pair in pairs if pair in lines]
        point = rp._triple_point(planes, gradients, heights, origin, meets[:2], res)
        if point is not None and inside.contains(Point(point)):
            out.append((triple, (float(point[0]), float(point[1]))))
    return out


def _sample_planes(building_id: str, method: str) -> Tuple[str, Optional[dict]]:
    found, _, pixels_xyz = fast_roof_detection.detect(building_id, method)
    if not found:
        return building_id, None
    footprint = fast_roof_detection._DATA["buildings"][building_id]["polygon"]
    res = fast_roof_detection._DATA["resolution"]
    return building_id, {"footprint": footprint, "planes": found, "pixels_xyz": pixels_xyz,
                         "triples": triple_points(footprint, found, res, _POLYGON_PARAMS["min_dist_to_edge_m"])}


def planes_from_sample(sample_path: str, method: str, workers: int) -> dict:
    """Run detection over a `fast_roof_detection` sample, and keep each building's planes."""
    fast_roof_detection._init_worker(sample_path, [])
    data = fast_roof_detection._DATA
    ids = sorted(data["buildings"], key=lambda b: -len(data["buildings"][b]["pixels"]))
    with ProcessPoolExecutor(workers, initializer=fast_roof_detection._init_worker,
                             initargs=(sample_path, [])) as ex:
        found = dict(ex.map(_sample_planes, ids, [method] * len(ids), chunksize=4))
    return {"resolution": data["resolution"], "detection": method,
            "buildings": {b: v for b, v in found.items() if v is not None}}


def _resolution(planes: List[dict]) -> float:
    """The pixel spacing, from the planes' pixel centres."""
    gaps = [np.diff(np.unique(p["inliers_xy"][:, 0])) for p in planes]
    return float(min(g.min() for g in gaps if len(g) > 0))


def _with_triples(job) -> Tuple[str, dict]:
    building_id, building, res = job
    building["triples"] = triple_points(building["footprint"], building["planes"], res,
                                      _POLYGON_PARAMS["min_dist_to_edge_m"])
    return building_id, building


def planes_from_job(pg_uri: str, job_id: int, workers: int) -> dict:
    """Each building's planes from a job's schema."""
    import psycopg2
    from solar_pv import tables
    schema = tables.schema(job_id)
    with psycopg2.connect(pg_uri) as conn, conn.cursor() as cur:
        cur.execute(f"""
            SELECT b.building_id, ST_AsBinary(b.geom_27700) FROM {schema}.{tables.BUILDINGS_TABLE} b
            WHERE EXISTS (SELECT FROM {schema}.{tables.ROOF_POLYGON_TABLE} r WHERE r.building_id = b.building_id)""")
        footprints = {b: shapely.from_wkb(bytes(g)) for b, g in cur.fetchall()}
        cur.execute(f"""
            SELECT building_id, slope, aspect, is_flat, x_coef, y_coef, intercept, inliers_xy
            FROM {schema}.{tables.ROOF_POLYGON_TABLE} ORDER BY roof_plane_id""")
        planes = defaultdict(list)
        for building_id, slope, aspect, is_flat, x_coef, y_coef, intercept, inliers_xy in cur:
            planes[building_id].append({
                "building_id": building_id, "slope": slope, "aspect": aspect, "is_flat": is_flat,
                "x_coef": x_coef, "y_coef": y_coef, "intercept": intercept,
                "inliers_xy": np.array(inliers_xy, dtype=float).reshape(-1, 2)})
    res = _resolution(next(iter(planes.values())))
    jobs = [(b, {"footprint": footprints[b], "planes": ps}, res) for b, ps in planes.items()]
    with ProcessPoolExecutor(workers) as ex:
        buildings = dict(ex.map(_with_triples, jobs, chunksize=32))
    return {"resolution": res, "detection": f"job {job_id}", "buildings": buildings}


# --- metrics ---

def _vertices(geom) -> int:
    return sum(len(ring.coords) - 1 for part in shapely.get_parts(geom) if part.geom_type == "Polygon"
               for ring in (part.exterior, *part.interiors))


def _length_and_simplified(lines, tolerance: float) -> Tuple[float, float]:
    parts = [g for g in shapely.get_parts(lines) if g.geom_type == "LineString" and g.length > _EPS_M]
    return (sum(p.length for p in parts),
            sum(shapely.simplify(p, tolerance, preserve_topology=False).length for p in parts))


def polygon_metrics(footprint, planes: List[dict], polygons: Polygons, triples: list, res: float,
                    min_dist_to_edge_m: float) -> Tuple[dict, List[Optional[float]]]:
    """
    One building's measures, as sums over it (see the module docstring), and for each of its
    `triples`, the distance from its point to where its three planes' polygons meet (None if
    they don't). `polygons` are (plane index, polygon, raw geometry, usable).
    """
    m = dict(buildings=1, polygons=len(polygons), usable=0, vertices=0, area=0.0, raw_area=0.0, mismatch=0.0,
             overlap=0.0, shared_len=0.0, shared_simplified=0.0, free_len=0.0, free_simplified=0.0,
             triples=len(triples), triples_shared=0, triple_dist=0.0, holes=0, hole_area=0.0,
             touching_pairs=0, touching_unshared=0, shared_edges=0, shared_edges_to_wall=0)
    edge = _shrunk(footprint, min_dist_to_edge_m).boundary
    edge_zone = edge.buffer(_EPS_M)
    geoms = [g for _, g, _, _ in polygons]
    boundaries = [g.boundary for g in geoms]
    for k, (_, g, raw, usable) in enumerate(polygons):
        m["usable"] += int(bool(usable))
        m["vertices"] += _vertices(g)
        m["area"] += g.area
        m["raw_area"] += raw.area
        m["mismatch"] += g.symmetric_difference(raw).area
        m["overlap"] += sum(g.intersection(geoms[j]).area for j in range(k + 1, len(geoms)))
        inner = boundaries[k].difference(edge_zone)
        others = [b for j, b in enumerate(boundaries) if j != k]
        if others:
            others_zone = shapely.union_all(others).buffer(_EPS_M)
            shared, free = inner.intersection(others_zone), inner.difference(others_zone)
        else:
            shared, free = shapely.GeometryCollection(), inner
        # shared boundary is counted once from each side, which the ratio doesn't mind:
        a, b = _length_and_simplified(shared, _ZIGZAG_TOLERANCE_TILES * res)
        m["shared_len"] += a
        m["shared_simplified"] += b
        a, b = _length_and_simplified(free, _ZIGZAG_TOLERANCE_TILES * res)
        m["free_len"] += a
        m["free_simplified"] += b

    if geoms:
        for part in shapely.get_parts(shapely.union_all(geoms)):
            for ring in getattr(part, "interiors", []):
                area = shapely.Polygon(ring).area
                if area > _HOLE_MIN_TILES2 * res * res:
                    m["holes"] += 1
                    m["hole_area"] += area

    by_plane = {k: g for k, g, _, _ in polygons}
    triple_dist = []
    for triple, point in triples:
        gs = [by_plane.get(k) for k in triple]
        common = None if any(g is None for g in gs) else gs[0].intersection(gs[1]).intersection(gs[2])
        if common is None or common.is_empty:
            triple_dist.append(None)
            continue
        triple_dist.append(common.distance(Point(point)))
        m["triples_shared"] += 1
        m["triple_dist"] += triple_dist[-1]

    owned = [p["inliers_xy"] for p in planes]
    regions = [rp._pixel_region(o, res) for o in owned]
    for a, b in combinations(range(len(planes)), 2):
        if a not in by_plane or b not in by_plane or regions[a].distance(regions[b]) > 1e-6:
            continue
        # touching along an edge, not just at a corner:
        if not shapely.dwithin(regions[b], shapely.points(owned[a]), res / 2 + 1e-6).any():
            continue
        m["touching_pairs"] += 1
        shared = by_plane[a].boundary.intersection(by_plane[b].boundary)
        if shared.length < _UNSHARED_MAX_TILES * res:
            m["touching_unshared"] += 1
        elif shared.length >= _EDGE_MIN_TILES * res:
            m["shared_edges"] += 1
            m["shared_edges_to_wall"] += int(shared.distance(edge) < _EPS_M)
    return m, triple_dist


# --- score ---

_PLANES = None
_POLYGON_METHOD = None


def _init_worker(planes_path: str, polygon_method: str, settings: List[Tuple[str, object]]) -> None:
    global _PLANES, _POLYGON_METHOD
    with open(planes_path, "rb") as f:
        _PLANES = pickle.load(f)
    _POLYGON_METHOD = polygon_method
    apply_settings(settings)


def _score_building(building_id: str) -> dict:
    building = _PLANES["buildings"][building_id]
    res = _PLANES["resolution"]
    planes = copy.deepcopy(building["planes"])
    for k, p in enumerate(planes):
        p[_INDEX] = k
    out = {"building_id": building_id, "footprint_m2": building["footprint"].area}
    start = time.process_time()
    try:
        made = rp.create_roof_polygons(building_id, building["footprint"], planes, resolution_metres=res,
                                       method=_POLYGON_METHOD, pixels_xyz=building.get("pixels_xyz"),
                                       **_POLYGON_PARAMS)
    except Exception:
        out["error"] = traceback.format_exc()
        return out
    out["seconds"] = time.process_time() - start
    # legacy polygons can merge planes, keeping one of their indices:
    out["polygons"] = [(p.get(_INDEX, -1), p["roof_geom_27700"], p["roof_geom_raw_27700"], bool(p["usable"]))
                       for p in made]
    out["metrics"], out["triple_dist"] = polygon_metrics(
        building["footprint"], building["planes"], out["polygons"], building["triples"], res,
        _POLYGON_PARAMS["min_dist_to_edge_m"])
    return out


def score(planes_path: str, polygon_method: str, settings: List[Tuple[str, object]], workers: int) -> List[dict]:
    with open(planes_path, "rb") as f:
        buildings = pickle.load(f)["buildings"]
    # biggest first, so a large building doesn't hold up the end of the run
    ids = sorted(buildings, key=lambda b: -len(buildings[b]["planes"]))
    with ProcessPoolExecutor(workers, initializer=_init_worker,
                             initargs=(planes_path, polygon_method, settings)) as ex:
        return list(ex.map(_score_building, ids, chunksize=8))


def write_layers(path: str, name: str, rs: List[dict], planes: dict) -> None:
    """Add (or replace) the run's polygon and triple point layers in a GeoPackage."""
    ds = ogr.Open(path, 1) if os.path.exists(path) else ogr.GetDriverByName("GPKG").CreateDataSource(path)
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(27700)
    R, I, S = ogr.OFTReal, ogr.OFTInteger, ogr.OFTString
    layers = {}
    for layer, gtype, fields in [
            (name, ogr.wkbMultiPolygon, [("building_id", S), ("plane", I), ("usable", I), ("raw_area", R),
                                         ("vertices", I)]),
            (f"{name}_triple", ogr.wkbPoint, [("building_id", S), ("planes", S), ("shared", I),
                                                ("dist", R)])]:
        if ds.GetLayerByName(layer) is not None:
            ds.DeleteLayer(layer)
        layers[layer] = ds.CreateLayer(layer, srs, gtype)
        for field, t in fields:
            layers[layer].CreateField(ogr.FieldDefn(field, t))

    def add(layer, geom, **attrs):
        lyr = layers[layer]
        f = ogr.Feature(lyr.GetLayerDefn())
        for k, v in attrs.items():
            if v is not None:
                f.SetField(k, v)
        f.SetGeometry(ogr.CreateGeometryFromWkb(shapely.to_wkb(geom)))
        lyr.CreateFeature(f)

    ds.StartTransaction()
    for r in rs:
        building_id = r["building_id"]
        for k, g, raw, usable in r.get("polygons", []):
            add(name, shapely.MultiPolygon(shapely.get_parts(g)), building_id=building_id, plane=int(k),
                usable=int(usable), raw_area=float(raw.area), vertices=_vertices(g))
        for (triple, point), dist in zip(planes["buildings"][building_id]["triples"], r.get("triple_dist", [])):
            add(f"{name}_triple", Point(point), building_id=building_id, planes=",".join(map(str, triple)),
                shared=int(dist is not None), dist=dist)
    ds.CommitTransaction()
    ds = None


# --- compare ---

def _rows(totals: List[dict]) -> List[Tuple[str, List[str]]]:
    """The comparison table's rows, for runs' summed metrics."""
    def each(f):
        return [f(t) for t in totals]

    def ratio(a, b):
        return lambda t: t[a] / t[b] if t[b] else float("nan")

    base_raw = totals[0]["raw_area"]
    return [
        ("buildings", each(lambda t: f"{t['buildings']:,}")),
        ("polygons (usable)", each(lambda t: f"{t['polygons']:,} ({t['usable']:,})")),
        ("triples whose 3 polygons share a point", each(lambda t: f"{ratio('triples_shared', 'triples')(t):.0%}")),
        ("…mean distance from that point to the triple point", each(
            lambda t: f"{ratio('triple_dist', 'triples_shared')(t):.2f}m")),
        ("holes between polygons", each(lambda t: f"{t['holes']:,}")),
        ("touching pairs with no shared edge", each(lambda t: f"{ratio('touching_unshared', 'touching_pairs')(t):.1%}")),
        ("shared edges (≥ 1 tile) reaching the building edge", each(
            lambda t: f"{ratio('shared_edges_to_wall', 'shared_edges')(t):.0%}")),
        ("vertices per polygon", each(lambda t: f"{ratio('vertices', 'polygons')(t):.2f}")),
        ("zig-zag, shared / free", each(lambda t: f"{ratio('shared_len', 'shared_simplified')(t):.3f} / "
                                                  f"{ratio('free_len', 'free_simplified')(t):.3f}")),
        ("raw area", [f"{base_raw:,.0f} m²"] + [f"{t['raw_area'] / base_raw - 1:+.1%}" for t in totals[1:]]),
        ("mismatch", each(lambda t: f"{ratio('mismatch', 'area')(t):.2%}")),
        ("overlap", each(lambda t: f"{t['overlap']:.1f} m²")),
    ]


def compare(runs: List[Tuple[str, List[dict]]], building_ids: List[str] = None) -> str:
    """A table of the named runs' measures, over the buildings every run scored; and with
    `building_ids`, one for each of those buildings."""
    names = [name for name, _ in runs]
    by_run = [{r["building_id"]: r for r in rs if "metrics" in r} for _, rs in runs]
    common = set.intersection(*[set(b) for b in by_run])

    def table(ids, title):
        totals = [{k: sum(rs[b]["metrics"][k] for b in ids) for k in rs[next(iter(ids))]["metrics"]}
                  for rs in by_run]
        lines = [f"| {title} | " + " | ".join(names) + " |", "|---|" + "---|" * len(names)]
        lines += [f"| {label} | " + " | ".join(cells) + " |" for label, cells in _rows(totals)]
        return "\n".join(lines)

    failed = [[r["building_id"] for r in rs if "error" in r] for _, rs in runs]
    tables = [table(common, "")] if common else []
    tables.append("| | " + " | ".join(names) + " |\n|---|" + "---|" * len(names) + "\n"
                  + "| failed buildings | " + " | ".join(f"{len(f):,}" for f in failed) + " |\n"
                  + "| CPU seconds | " + " | ".join(f"{sum(r.get('seconds', 0) for r in rs.values()):,.0f}"
                                                   for rs in by_run) + " |")
    for building_id in building_ids or []:
        if building_id in common:
            tables.append(table([building_id], building_id))
        else:
            tables.append(f"{building_id}: not scored by every run")
    for name, f in zip(names, failed):
        if f:
            tables.append(f"{name} failed on: {', '.join(map(str, f[:20]))}{' ...' if len(f) > 20 else ''}")
    return "\n\n".join(tables)


# --- main ---

def _load(path: str):
    with open(path, "rb") as f:
        return pickle.load(f)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    workers = max(os.cpu_count() - 1, 1)

    p = sub.add_parser("planes", help="save detection's planes, for scoring polygons over")
    source = p.add_mutually_exclusive_group(required=True)
    source.add_argument("--sample", help="a fast_roof_detection sample to run detection over")
    source.add_argument("--pg-uri", help="a database with a job's schema to read planes from")
    p.add_argument("--method", choices=["legacy", "mincut", "segment"], help="detection method, with --sample")
    p.add_argument("--job-id", type=int, default=0)
    p.add_argument("--out", required=True)
    p.add_argument("--workers", type=int, default=workers)

    p = sub.add_parser("score", help="run the polygon stage over saved planes, and score it")
    p.add_argument("planes")
    p.add_argument("--polygons", choices=["legacy", "mincut"], default="mincut")
    p.add_argument("--set", dest="settings", action="append", default=[], type=_setting,
                   metavar="module.NAME=value")
    p.add_argument("--out", help="save the per-building scores and polygons here, for compare")
    p.add_argument("--gpkg", help="add the run's polygons and triple points to this GeoPackage")
    p.add_argument("--name", help="the run's layer name in --gpkg (default: --out's file name)")
    p.add_argument("--workers", type=int, default=workers)

    p = sub.add_parser("compare", help="compare scored runs")
    p.add_argument("runs", nargs="+", help="name=scores.pkl")
    p.add_argument("--buildings", help="comma-separated building ids to show individually")

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s")

    if args.command == "planes":
        if args.sample:
            if not args.method:
                parser.error("--sample needs --method")
            planes = planes_from_sample(args.sample, args.method, args.workers)
        else:
            planes = planes_from_job(args.pg_uri, args.job_id, args.workers)
        with open(args.out, "wb") as f:
            pickle.dump(planes, f, protocol=5)
        logging.info(f"Saved {len(planes['buildings'])} buildings' planes, with "
                     f"{sum(len(b['triples']) for b in planes['buildings'].values())} triple points, to {args.out}")
    elif args.command == "score":
        name = args.name or (os.path.splitext(os.path.basename(args.out))[0] if args.out else None)
        if args.gpkg and not name:
            parser.error("--gpkg needs --name or --out")
        rs = score(args.planes, args.polygons, args.settings, args.workers)
        if args.out:
            with open(args.out, "wb") as f:
                pickle.dump(rs, f, protocol=5)
        if args.gpkg:
            write_layers(args.gpkg, name, rs, _load(args.planes))
        print(compare([(name or args.polygons, rs)]))
    else:
        runs = [(name, _load(path)) for name, path in (s.split("=", 1) for s in args.runs)]
        print(compare(runs, args.buildings.split(",") if args.buildings else None))


if __name__ == "__main__":
    main()
