# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
"""
Score CLI result GeoPackages from different runs (e.g. before and after a change to roof
detection or roof polygons) against the same LiDAR, and find the buildings and roof planes
they most disagree on.

    python -m solar_pv.evaluation.roof_detection --lidar tiles/ --out-dir eval/ \\
        base=results-base.gpkg new=results-new.gpkg

The runs must be over the same buildings. Each run's output roof polygons are laid over
the model's pixels (centres in the building footprint, from the elevation the model would
build from `--lidar`); a pixel takes the plane whose polygon contains its centre, and is
scored against that plane - so this measures the polygons the model outputs, not detection's
internal pixel assignment (kWh and area are from the planes' raw geometry, which isn't
exported). Pixels at or below the building's max ground height are left out, which needs the
ground heights the CLI exports in `pv_building`; for older outputs without them, pass the
`--pg-uri` of a database holding the job's schema (a CLI run with `--keep`).

Per building and run:
  coverage, explained   shares of above-ground pixels in a polygon / within 0.25m (0.1m on a
                        flat plane) of it
  mae                   mean |residual| over covered pixels
  energy                as `assign_pixels` minimises (capped residual over scale, 2 per pixel
                        in no plane, 0.5/m of boundary) plus 8 per plane, as `plane_moves`
                        charges: lower is better. The per-plane cost stops it rewarding
                        over-splitting, but only partly; and roof detection minimises much
                        the same energy, so it favours runs of it over other ways of
                        finding planes
  missed_planar_m2      planar patches (MAE < 0.08m, >= 10 px) in pixels no plane covers, and
                        that no mess rejection (MESS_REASONS) explains
  oversplit_pairs       adjacent planes that one plane would fit nearly as well (see
                        `oversplit_pairs`): the measures above reward splitting, this doesn't
Per plane, the skew against its pixels (`solar_pv.roof_detection.plane_skew`).

Neither set of numbers is ground truth: they reward explaining the LiDAR, which mess
rejections deliberately don't. Use the review GeoPackage to check divergences against
imagery.

Writes to --out-dir: summary.csv, pairs.csv, divergent_buildings.csv, skew_summary.csv,
plane_skew.csv, skew_review.csv, and review.gpkg (layers: building - every building with its
metrics; plane_<run> and pixel - for the divergent buildings; skewed_plane and
skewed_plane_pixel - for the most skewed planes).
"""
import argparse
import csv
import logging
import os
import tempfile
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from os.path import join
from typing import Dict, List, Optional

import numpy as np
import shapely
from osgeo import gdal, ogr, osr
from shapely import wkb

from solar_pv import gdal_helpers
from solar_pv.cli import discover_lidar
from solar_pv.lidar.lidar_selector import select_lidar
from solar_pv.roof_detection.plane_skew import plane_skew
from solar_pv.roof_detection.assign_pixels import grid_index, _neighbour_pairs, RESIDUAL_CAP, NONE_COST, \
    SMOOTHNESS_PER_M
from solar_pv.roof_detection.segment_planes import segment_planes, local_fits

gdal.UseExceptions()
ogr.UseExceptions()
osr.UseExceptions()

LABEL_COST = 8.0
MESS_REASONS = {"MESSY_FLAT_ROOF", "CLUTTER", "SURROUNDED_BY_MESS", "MESSY_BUILDING", "OBSTACLES", "HIGH_MAE"}
PRE_DETECTION_EXCLUSIONS = {"OUTDATED_LIDAR_COVERAGE", "NO_LIDAR_COVERAGE", "TOO_SMALL"}
SIZES = [("<50m2", 0, 50), ("50-200m2", 50, 200), ("200-1000m2", 200, 1000), (">1000m2", 1000, np.inf)]
METRICS = ["planes", "usable_planes", "coverage", "explained", "mae", "energy", "missed_planar_m2",
           "oversplit_pairs", "usable_area", "kwp", "kwh"]


def _scale(is_flat: bool) -> float:
    return 0.1 if is_flat else 0.25


def _normal(x_coef: float, y_coef: float) -> np.ndarray:
    n = np.array([-x_coef, -y_coef, 1.0])
    return n / np.linalg.norm(n)


# --- loading ---

def load_method(path: str) -> dict:
    """{planes: {building_id: [plane]}, mess: {building_id: [geom]}, buildings: {building_id:
    (geom, max ground height, exclusion reason)}} from a CLI result GeoPackage."""
    src = osr.SpatialReference(); src.ImportFromEPSG(4326)
    src.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    dst = osr.SpatialReference(); dst.ImportFromEPSG(27700)
    to_27700 = osr.CoordinateTransformation(src, dst)
    ds = ogr.Open(path)

    lyr = ds.GetLayer("pv_roof_plane")
    defn = lyr.GetLayerDefn()
    has_usable = defn.GetFieldIndex("usable") >= 0
    planes = defaultdict(list)
    for f in lyr:
        g = f.GetGeometryRef()
        if g is None:
            continue
        g = g.Clone()
        g.Transform(to_27700)
        planes[f.GetField("building_id")].append({
            "id": f.GetField("roof_plane_id"), "geom": wkb.loads(bytes(g.ExportToWkb())),
            "coef": (f.GetField("x_coef"), f.GetField("y_coef"), f.GetField("intercept")),
            "is_flat": bool(f.GetField("is_flat")), "slope": f.GetField("slope"), "aspect": f.GetField("aspect"),
            "usable": bool(f.GetField("usable")) if has_usable else True,
            "reason": f.GetField("not_usable_reason") if has_usable else None,
            "area": f.GetField("area") or 0.0, "kwh": f.GetField("kwh_year") or 0.0, "kwp": f.GetField("kwp") or 0.0,
        })

    mess = defaultdict(list)
    if ds.GetLayerByName("rejected_roof_plane") is not None:
        for f in ds.GetLayer("rejected_roof_plane"):
            g = f.GetGeometryRef()
            if g is not None and f.GetField("rejection_reason") in MESS_REASONS:
                mess[f.GetField("building_id")].append(wkb.loads(bytes(g.ExportToWkb())))

    lyr = ds.GetLayer("pv_building")
    has_ground = lyr.GetLayerDefn().GetFieldIndex("max_ground_height") >= 0
    buildings = {}
    for f in lyr:
        g = f.GetGeometryRef()
        buildings[f.GetField("building_id")] = (
            wkb.loads(bytes(g.ExportToWkb())) if g is not None else None,
            f.GetField("max_ground_height") if has_ground else None,
            f.GetField("exclusion_reason"))
    return {"planes": dict(planes), "mess": dict(mess), "buildings": buildings, "has_ground": has_ground}


def _ground_heights_from_db(pg_uri: str, job_id: int) -> Dict[str, Optional[float]]:
    import psycopg2
    from solar_pv import tables
    with psycopg2.connect(pg_uri) as conn, conn.cursor() as cur:
        cur.execute(f"SELECT building_id, max_ground_height FROM {tables.schema(job_id)}.{tables.BUILDINGS_TABLE}")
        return dict(cur.fetchall())


def build_elevation(lidar_paths: List[str], bounds, out_dir: str) -> str:
    """The elevation the model would build from these tiles over `bounds`: the same
    resolution choice and newest/highest-res merge."""
    tiles = discover_lidar(lidar_paths)
    paths, _ = select_lidar(tiles, bounds, bounds, out_dir)
    vrt = join(out_dir, "elevation.vrt")
    gdal_helpers.create_vrt(paths, vrt)
    return vrt


# --- per building ---

_ELEVATION = None


def _building_pixels(elevation_path: str, footprint) -> np.ndarray:
    global _ELEVATION
    if _ELEVATION is None:
        _ELEVATION = gdal.Open(elevation_path)
    gt = _ELEVATION.GetGeoTransform()
    res = gt[1]
    minx, miny, maxx, maxy = footprint.bounds
    c0 = max(int(np.floor((minx - gt[0]) / res)), 0)
    c1 = min(int(np.ceil((maxx - gt[0]) / res)), _ELEVATION.RasterXSize)
    r0 = max(int(np.floor((maxy - gt[3]) / gt[5])), 0)
    r1 = min(int(np.ceil((miny - gt[3]) / gt[5])), _ELEVATION.RasterYSize)
    if c1 <= c0 or r1 <= r0:
        return np.zeros((0, 3))
    band = _ELEVATION.GetRasterBand(1)
    z = band.ReadAsArray(c0, r0, c1 - c0, r1 - r0).astype(float)
    cc, rr = np.meshgrid(np.arange(c0, c1), np.arange(r0, r1))
    x = gt[0] + (cc + 0.5) * res
    y = gt[3] + (rr + 0.5) * gt[5]
    ok = shapely.contains_xy(footprint, x, y) & np.isfinite(z)
    nodata = band.GetNoDataValue()
    if nodata is not None:
        ok &= z != nodata
    return np.column_stack([x[ok], y[ok], z[ok]])


def _label(xyz: np.ndarray, planes: List[dict]):
    """Plane index per pixel (-1 for none) and its residual; a pixel in several polygons takes
    the best-fitting."""
    labels = np.full(len(xyz), -1)
    best = np.full(len(xyz), np.inf)
    residual = np.full(len(xyz), np.nan)
    for k, p in enumerate(planes):
        inside = shapely.contains_xy(p["geom"], xyz[:, 0], xyz[:, 1])
        if not inside.any():
            continue
        xc, yc, ic = p["coef"]
        r = xyz[:, 2] - (xyz[:, 0] * xc + xyz[:, 1] * yc + ic)
        cost = np.minimum(np.abs(r) / _scale(p["is_flat"]), RESIDUAL_CAP)
        better = inside & (cost < best)
        labels[better] = k
        best[better] = cost[better]
        residual[better] = r[better]
    return labels, residual


def _agreement(la: np.ndarray, lb: np.ndarray, mask: np.ndarray) -> float:
    """Share of `mask` pixels two labellings agree on, matching each of a's planes to the plane
    of b it overlaps most (and no plane to no plane)."""
    a, b = la[mask], lb[mask]
    if len(a) == 0:
        return float("nan")
    pairs, counts = np.unique(np.column_stack([a, b]), axis=0, return_counts=True)
    best = {}
    for (x, y), n in zip(map(tuple, pairs), counts):
        if x != -1 and y != -1:
            best[x] = max(best.get(x, 0), n)
    return (sum(best.values()) + np.count_nonzero((a == -1) & (b == -1))) / len(a)


def _missed_planar(xyz, free, mess, local, res):
    """m2 of planar patches among the `free` pixels, not / mostly in mess rejections."""
    missed = missed_mess = 0
    if free.sum() < 10 or local is None:
        return 0.0, 0.0
    fidx = np.flatnonzero(free)
    seg = segment_planes(xyz[fidx], res, min_pixels=10, local=(local[0][fidx], local[1][fidx]))
    for s in set(seg.tolist()) - {-1}:
        idx = fidx[seg == s]
        A = np.column_stack([xyz[idx, :2] - xyz[idx, :2].mean(0), np.ones(len(idx))])
        coef, *_ = np.linalg.lstsq(A, xyz[idx, 2], rcond=None)
        if np.abs(xyz[idx, 2] - A @ coef).mean() < 0.08:
            if mess[idx].mean() > 0.5:
                missed_mess += len(idx)
            else:
                missed += len(idx)
    return missed * res * res, missed_mess * res * res


def _lstsq_plane(xyz: np.ndarray):
    """Least-squares plane (x_coef, y_coef, intercept about the centroid) and residuals."""
    A = np.column_stack([xyz[:, :2] - xyz[:, :2].mean(0), np.ones(len(xyz))])
    coef, *_ = np.linalg.lstsq(A, xyz[:, 2], rcond=None)
    return coef, xyz[:, 2] - A @ coef


def oversplit_pairs(xyz: np.ndarray, labels: np.ndarray, planes: List[dict], lo: np.ndarray,
                    hi: np.ndarray) -> List[dict]:
    """
    Adjacent planes that look like one face split in two: their own least-squares fits are
    within 5° of each other, and one plane fitted to both pixel sets is within 2cm on MAE and
    explains under 5 percentage points fewer of their pixels. The measures above reward
    splitting (it lowers residuals for only `LABEL_COST` a plane), so this checks for it.

    Each pair is the two planes' indices (`p` < `q`), the angle between their fits, how much
    worse the MAE of one plane fitted to both is, and the share of their pixels it explains
    fewer of.
    """
    a, b = labels[lo], labels[hi]
    touching = (a >= 0) & (b >= 0) & (a != b)
    pairs = set(zip(np.minimum(a[touching], b[touching]).tolist(), np.maximum(a[touching], b[touching]).tolist()))
    found = []
    for p, q in sorted(pairs):
        in_p, in_q = labels == p, labels == q
        if in_p.sum() < 3 or in_q.sum() < 3:
            continue
        cp, rp = _lstsq_plane(xyz[in_p])
        cq, rq = _lstsq_plane(xyz[in_q])
        _, ru = _lstsq_plane(xyz[in_p | in_q])
        n = (in_p | in_q).sum()
        angle = np.degrees(np.arccos(np.clip(abs(_normal(*cp[:2]) @ _normal(*cq[:2])), 0, 1)))
        separate_mae = (np.abs(rp).sum() + np.abs(rq).sum()) / n
        scale = _scale(planes[p]["is_flat"] and planes[q]["is_flat"])
        explained_loss = ((np.abs(rp) <= scale).sum() + (np.abs(rq) <= scale).sum() - (np.abs(ru) <= scale).sum()) / n
        mae_loss = np.abs(ru).mean() - separate_mae
        if angle < 5 and mae_loss < 0.02 and explained_loss < 0.05:
            found.append({"p": int(p), "q": int(q), "angle_deg": float(angle), "mae_loss_m": float(mae_loss),
                          "explained_loss": float(explained_loss)})
    return found


def building_context(xyz: np.ndarray, above: np.ndarray, res: float) -> dict:
    """What scoring needs about a building's pixels, whatever the planes."""
    grid, _, _ = grid_index(xyz[:, :2], res)
    lo, hi = _neighbour_pairs(grid)
    return {"lo": lo, "hi": hi, "both_above": above[lo] & above[hi],
            "local": local_fits(xyz, res) if above.sum() >= 8 else None}


def score_planes(xyz: np.ndarray, above: np.ndarray, res: float, planes: List[dict], mess_geoms: list,
                 ctx: dict):
    """
    One method's metrics for a building, and its pixel labels and residuals (see `_label`).
    `planes` are dicts with geom (the output polygon), coef, is_flat, usable, area, kwh and kwp;
    `mess_geoms` are its mess rejections' geometries.
    """
    lo, hi = ctx["lo"], ctx["hi"]
    n_above = int(above.sum())
    labels, residual = _label(xyz, planes)
    covered = labels >= 0
    scale = np.array([_scale(p["is_flat"]) for p in planes] + [1.0])[labels]
    cost = np.where(above, NONE_COST, 0.0)
    cost[covered] = np.minimum(np.abs(residual[covered]) / scale[covered], RESIDUAL_CAP)
    explained = covered & (np.abs(np.nan_to_num(residual, nan=np.inf)) <= scale)
    planes_on_pixels = len(set(labels[covered].tolist()))
    smooth = SMOOTHNESS_PER_M / res * np.count_nonzero((labels[lo] != labels[hi]) & ctx["both_above"])

    mess = np.zeros(len(xyz), bool)
    for g in mess_geoms:
        mess |= shapely.contains_xy(g, xyz[:, 0], xyz[:, 1])
    missed, missed_mess = _missed_planar(xyz, above & ~covered, mess, ctx["local"], res)

    usable = [p for p in planes if p["usable"]]
    usable_area = sum(p["area"] for p in usable)
    metrics = {
        "planes": len(planes),
        "usable_planes": len(usable),
        "coverage": float((covered & above).sum() / n_above),
        "explained": float((explained & above).sum() / n_above),
        "mae": float(np.nanmean(np.abs(residual[covered]))) if covered.any() else float("nan"),
        "covered_px": int(covered.sum()),
        "data_cost": float(cost.sum()),
        "smooth_cost": float(smooth),
        "energy": float(cost.sum()) + float(smooth) + LABEL_COST * planes_on_pixels,
        "missed_planar_m2": missed,
        "missed_mess_m2": missed_mess,
        "oversplit_pairs": len(oversplit_pairs(xyz, np.where(above, labels, -1), planes, lo, hi)),
        "usable_area": float(usable_area),
        "kwh": float(sum(p["kwh"] for p in usable)),
        "kwp": float(sum(p["kwp"] for p in usable)),
        "flat_share": float(sum(p["area"] for p in usable if p["is_flat"]) / max(usable_area, 1e-9)),
    }
    return metrics, labels, residual


def plane_skews(xyz: np.ndarray, above: np.ndarray, labels: np.ndarray, planes: List[dict], res: float):
    """(plane index, `plane_skew` dict) for each plane its skew can be determined for."""
    out = []
    for k, p in enumerate(planes):
        skew = plane_skew(xyz[(labels == k) & above], p["coef"], res)
        if skew is not None:
            out.append((k, skew))
    return out


def evaluate_building(job) -> dict:
    """Metrics for one building under each method; with `keep`, also its pixels and labels."""
    elevation_path, building_id, footprint, ground, by_method, mess_by_method, keep = job
    xyz = _building_pixels(elevation_path, footprint)
    out = {"building_id": building_id, "footprint_m2": footprint.area, "methods": {}, "skew": []}
    if len(xyz) < 4:
        return out
    res = abs(_ELEVATION.GetGeoTransform()[1])
    above = xyz[:, 2] > ground if ground is not None else np.ones(len(xyz), bool)
    n_above = int(above.sum())
    if n_above == 0:
        return out
    out["n_above"] = n_above
    ctx = building_context(xyz, above, res)
    lo, hi = ctx["lo"], ctx["hi"]

    labels_by, normals_by = {}, {}
    for m, planes in by_method.items():
        out["methods"][m], labels, _ = score_planes(xyz, above, res, planes, mess_by_method.get(m, []), ctx)
        labels_by[m] = labels
        normals_by[m] = np.array([_normal(*p["coef"][:2]) for p in planes] + [[np.nan] * 3])[labels]
        for k, skew in plane_skews(xyz, above, labels, planes, res):
            p = planes[k]
            out["skew"].append(dict(skew, building_id=building_id, method=m, plane=k, roof_plane_id=p["id"],
                                    is_flat=int(p["is_flat"]), usable=int(p["usable"]), kwh=p["kwh"],
                                    stored_slope=p["slope"], stored_aspect=p["aspect"],
                                    footprint_m2=footprint.area))

    methods = list(by_method)
    out["pairs"] = {}
    for i in range(len(methods)):
        for j in range(i + 1, len(methods)):
            a, b = methods[i], methods[j]
            agreement = 0.5 * (_agreement(labels_by[a], labels_by[b], above)
                               + _agreement(labels_by[b], labels_by[a], above))
            both = (labels_by[a] >= 0) & (labels_by[b] >= 0) & above
            angle = float("nan")
            if both.any():
                dots = np.clip(np.abs(np.sum(normals_by[a][both] * normals_by[b][both], axis=1)), 0, 1)
                angle = float(np.degrees(np.arccos(dots)).mean())
            out["pairs"][f"{a}~{b}"] = {"agreement": agreement, "normal_angle": angle}
    if keep:
        out.update(xyz=xyz, above=above, labels=labels_by)
    return out


# --- summaries ---

def _pixel_weighted_quantile(v, w, q):
    order = np.argsort(v)
    cw = np.cumsum(w[order]) / w.sum()
    return float(v[order][min(np.searchsorted(cw, q), len(v) - 1)])


def summarise(rs: List[dict], methods: List[str], label: str) -> List[dict]:
    n_above = np.array([r["n_above"] for r in rs], float)
    rows = []
    for m in methods:
        def col(f):
            return np.array([r["methods"][m][f] for r in rs], float)
        covered, mae = col("covered_px"), col("mae")
        rows.append({
            "set": label, "method": m, "buildings": len(rs),
            "planes": int(col("planes").sum()), "usable_planes": int(col("usable_planes").sum()),
            "coverage": (col("coverage") * n_above).sum() / n_above.sum(),
            "explained": (col("explained") * n_above).sum() / n_above.sum(),
            "mae_m": np.nansum(mae * covered) / max(covered[~np.isnan(mae)].sum(), 1),
            "energy_px": col("energy").sum() / n_above.sum(),
            "data_px": col("data_cost").sum() / n_above.sum(),
            "smooth_px": col("smooth_cost").sum() / n_above.sum(),
            "missed_planar_m2": col("missed_planar_m2").sum(),
            "missed_in_mess_m2": col("missed_mess_m2").sum(),
            "oversplit_pairs": int(col("oversplit_pairs").sum()),
            "usable_area_m2": col("usable_area").sum(),
            "kwp": col("kwp").sum(), "mwh": col("kwh").sum() / 1000,
            "flat_area_share": (col("flat_share") * col("usable_area")).sum() / max(col("usable_area").sum(), 1),
            "no_usable_planes": int((col("usable_planes") == 0).sum()),
        })
    energy = np.array([[r["methods"][m]["energy"] for m in methods] for r in rs])
    wins = np.isclose(energy, energy.min(axis=1, keepdims=True), rtol=1e-9, atol=1e-6)
    sole = wins & (wins.sum(axis=1, keepdims=True) == 1)
    for i, row in enumerate(rows):
        row["energy_wins"] = int(wins[:, i].sum())
        row["energy_sole_wins"] = int(sole[:, i].sum())
    return rows


def summarise_pairs(rs: List[dict]) -> List[dict]:
    n_above = np.array([r["n_above"] for r in rs], float)
    out = []
    for pair in rs[0]["pairs"]:
        a, b = pair.split("~")
        agreement = np.array([r["pairs"][pair]["agreement"] for r in rs])
        angle = np.array([r["pairs"][pair]["normal_angle"] for r in rs])
        ka = np.array([r["methods"][a]["kwh"] for r in rs])
        kb = np.array([r["methods"][b]["kwh"] for r in rs])
        mean = (ka + kb) / 2
        rel = (np.abs(ka - kb) / np.maximum(mean, 1))[mean >= 500]
        out.append({"pair": pair,
                    "px_agreement": np.nansum(agreement * n_above) / n_above[~np.isnan(agreement)].sum(),
                    "buildings_agree_lt_80pc": int((agreement < 0.8).sum()),
                    "mean_normal_angle_deg": float(np.nanmean(angle)),
                    "kwh_corr": float(np.corrcoef(ka, kb)[0, 1]),
                    "median_rel_kwh_diff": float(np.median(rel)),
                    "buildings_kwh_diff_gt_25pc": int((rel > 0.25).sum()),
                    "total_mwh_diff": float((ka - kb).sum() / 1000)})
    return out


def summarise_skew(rows: List[dict], methods: List[str], label: str) -> List[dict]:
    out = []
    for m in methods:
        rs = [x for x in rows if x["method"] == m]
        if not rs:
            continue
        px = np.array([x["pixels"] for x in rs], float)
        tilt = np.array([x["tilt_err_deg"] for x in rs])
        # no tilt_z: too few bands across the slope to tell, so not significant
        tilt_z = np.array([x["tilt_z"] if x["tilt_z"] is not None else 0.0 for x in rs])
        offset = np.abs([x["offset_m"] for x in rs])
        contamination = np.array([x["contamination_deg"] for x in rs])
        significant = (tilt_z >= 3) & (tilt >= 2)
        row = {"set": label, "method": m, "planes": len(rs),
               "tilt_med_deg": _pixel_weighted_quantile(tilt, px, .5),
               "tilt_p90_deg": _pixel_weighted_quantile(tilt, px, .9),
               "sig_skewed_planes": int(significant.sum()),
               "sig_skewed_px_share": float(px[significant].sum() / px.sum()),
               "offset_med_m": _pixel_weighted_quantile(offset, px, .5),
               "contam_med_deg": _pixel_weighted_quantile(contamination, px, .5),
               "contam_gt_5deg_px_share": float(px[contamination > 5].sum() / px.sum())}
        out.append(row)
    return out


def _divergence(r: dict, methods: List[str]) -> None:
    kwh = np.array([r["methods"][m]["kwh"] for m in methods])
    energy = np.array([r["methods"][m]["energy"] for m in methods]) / r["n_above"]
    r["kwh_spread"] = float(kwh.max() - kwh.min())
    r["kwh_rel_spread"] = float((kwh.max() - kwh.min()) / max(kwh.mean(), 1))
    r["min_agreement"] = float(np.nanmin([v["agreement"] for v in r["pairs"].values()] or [1.0]))
    r["best_energy"] = methods[int(energy.argmin())]


def divergent(rs: List[dict], methods: List[str], n: int) -> Dict[str, str]:
    """{building_id: the review lists it is in}: the top `n` by absolute kWh spread, relative
    kWh spread (mean >= 1000 kWh) and least pixel agreement (>= 40 px)."""
    lists = {
        "kwh_abs": sorted(rs, key=lambda r: -r["kwh_spread"]),
        "kwh_rel": sorted([r for r in rs if np.mean([r["methods"][m]["kwh"] for m in methods]) >= 1000],
                          key=lambda r: -r["kwh_rel_spread"]),
        "structure": sorted([r for r in rs if r["n_above"] >= 40], key=lambda r: (r["min_agreement"], -r["n_above"])),
    }
    review = defaultdict(list)
    for name, ranked in lists.items():
        for rank, r in enumerate(ranked[:n]):
            review[r["building_id"]].append(f"{name}#{rank + 1}")
    return {k: ",".join(v) for k, v in review.items()}


def skew_review(rows: List[dict], methods: List[str], n: int) -> List[dict]:
    """Per method, the `n` most skewed significant planes."""
    review = []
    for m in methods:
        tilted = [x for x in rows if x["method"] == m and x["pixels"] >= 20 and (x["tilt_z"] or 0) >= 3]
        for rank, x in enumerate(sorted(tilted, key=lambda x: -x["tilt_err_deg"])[:n]):
            review.append(dict(x, list=f"tilt#{rank + 1}"))
    return review


# --- output ---

def write_csv(rows: List[dict], path: str) -> None:
    if not rows:
        return
    fields = list(dict.fromkeys(k for r in rows for k in r))
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)


def markdown_table(rows: List[dict]) -> str:
    keys = list(dict.fromkeys(k for r in rows for k in r))

    def cell(v):
        if isinstance(v, float):
            return "" if np.isnan(v) else (f"{v:.3f}" if abs(v) < 10 else f"{v:,.0f}")
        return "" if v is None else str(v)
    lines = ["| " + " | ".join(keys) + " |", "|" + "---|" * len(keys)]
    lines += ["| " + " | ".join(cell(r.get(k)) for k in keys) + " |" for r in rows]
    return "\n".join(lines)


class _Gpkg:
    def __init__(self, path: str):
        srs = osr.SpatialReference()
        srs.ImportFromEPSG(27700)
        self.srs = srs
        self.ds = ogr.GetDriverByName("GPKG").CreateDataSource(path)
        self.layers = {}

    def layer(self, name: str, gtype, fields):
        lyr = self.ds.CreateLayer(name, self.srs, gtype)
        for f, t in fields:
            lyr.CreateField(ogr.FieldDefn(f, t))
        self.layers[name] = lyr

    def add(self, name: str, geom, **attrs):
        lyr = self.layers[name]
        f = ogr.Feature(lyr.GetLayerDefn())
        for k, v in attrs.items():
            if v is not None and not (isinstance(v, float) and not np.isfinite(v)):
                f.SetField(k, v)
        g = ogr.CreateGeometryFromWkb(shapely.to_wkb(geom))
        if lyr.GetGeomType() == ogr.wkbMultiPolygon:
            g = ogr.ForceToMultiPolygon(g)
        f.SetGeometry(g)
        lyr.CreateFeature(f)

    def close(self):
        self.ds = None


def _pixel_square(x, y, res):
    return shapely.box(x - res / 2, y - res / 2, x + res / 2, y + res / 2)


def write_review(path: str, rs: List[dict], review: Dict[str, str], detail: Dict[str, dict],
                 skewed: List[dict], methods: List[str], data: Dict[str, dict], res: float) -> None:
    R, I, S = ogr.OFTReal, ogr.OFTInteger, ogr.OFTString
    out = _Gpkg(path)
    out.layer("building", ogr.wkbMultiPolygon,
              [("building_id", S), ("footprint_m2", R), ("n_above", I), ("review", S), ("best_energy", S),
               ("kwh_spread", R), ("kwh_rel_spread", R), ("min_agreement", R)]
              + [(f"{m}_{k}", R) for m in methods for k in METRICS + ["energy_px"]])
    for m in methods:
        out.layer(f"plane_{m}", ogr.wkbMultiPolygon,
                  [("building_id", S), ("roof_plane_id", I), ("slope", R), ("aspect", R), ("is_flat", I),
                   ("usable", I), ("not_usable_reason", S), ("area", R), ("kwh", R), ("px", I), ("px_mae", R)])
    out.layer("pixel", ogr.wkbPolygon, [("building_id", S), ("z", R), ("above", I)]
              + [(f"{m}_plane", I) for m in methods] + [(f"{m}_resid", R) for m in methods])
    skew_fields = [k for k in (skewed[0] if skewed else {}) if k not in ("building_id", "method", "list")]
    out.layer("skewed_plane", ogr.wkbMultiPolygon, [("building_id", S), ("method", S), ("list", S)]
              + [(k, R) for k in skew_fields])
    out.layer("skewed_plane_pixel", ogr.wkbPolygon, [("building_id", S), ("method", S), ("roof_plane_id", I),
                                                     ("z", R), ("resid_stored", R), ("resid_implied", R)])

    footprints = data[methods[0]]["buildings"]
    for r in rs:
        bid = r["building_id"]
        out.add("building", footprints[bid][0], building_id=bid, footprint_m2=r["footprint_m2"], n_above=r["n_above"],
                review=review.get(bid), best_energy=r["best_energy"], kwh_spread=r["kwh_spread"],
                kwh_rel_spread=r["kwh_rel_spread"], min_agreement=r["min_agreement"],
                **{f"{m}_{k}": float(r["methods"][m][k]) for m in methods for k in METRICS},
                **{f"{m}_energy_px": r["methods"][m]["energy"] / r["n_above"] for m in methods})

    for bid in review:
        d = detail[bid]
        xyz = d["xyz"]
        resid = {}
        for m in methods:
            labels = d["labels"][m]
            r = np.full(len(xyz), np.nan)
            for k, p in enumerate(data[m]["planes"].get(bid, [])):
                idx = labels == k
                xc, yc, ic = p["coef"]
                r[idx] = xyz[idx, 2] - (xyz[idx, 0] * xc + xyz[idx, 1] * yc + ic)
                out.add(f"plane_{m}", p["geom"], building_id=bid, roof_plane_id=p["id"], slope=p["slope"],
                        aspect=p["aspect"], is_flat=int(p["is_flat"]), usable=int(p["usable"]),
                        not_usable_reason=p["reason"], area=p["area"], kwh=p["kwh"], px=int(idx.sum()),
                        px_mae=float(np.abs(r[idx]).mean()) if idx.any() else None)
            resid[m] = r
        for i in range(len(xyz)):
            out.add("pixel", _pixel_square(xyz[i, 0], xyz[i, 1], res), building_id=bid, z=float(xyz[i, 2]),
                    above=int(d["above"][i]), **{f"{m}_plane": int(d["labels"][m][i]) for m in methods},
                    **{f"{m}_resid": float(resid[m][i]) for m in methods})

    lists = defaultdict(list)
    for x in skewed:
        lists[(x["building_id"], x["method"], x["plane"])].append(x)
    for (bid, m, k), xs in lists.items():
        x = xs[0]
        p = data[m]["planes"][bid][k]
        out.add("skewed_plane", p["geom"], building_id=bid, method=m, list=",".join(v["list"] for v in xs),
                **{f: float(x[f]) for f in skew_fields if x[f] is not None})
        d = detail[bid]
        idx = (d["labels"][m] == k) & d["above"]
        pts = d["xyz"][idx]
        xc, yc, ic = p["coef"]
        r = pts[:, 2] - (pts[:, 0] * xc + pts[:, 1] * yc + ic)
        # the implied plane, from its orientation and offset:
        ix, iy = _coef_from_slope_aspect(x["implied_slope"], x["implied_aspect"])
        cx, cy = pts[:, 0].mean(), pts[:, 1].mean()
        stored_at_centre = cx * xc + cy * yc + ic
        implied = stored_at_centre + x["offset_m"] + (pts[:, 0] - cx) * ix + (pts[:, 1] - cy) * iy
        for (px, py, pz), a, b in zip(pts, r, pts[:, 2] - implied):
            out.add("skewed_plane_pixel", _pixel_square(px, py, res), building_id=bid, method=m,
                    roof_plane_id=p["id"], z=float(pz), resid_stored=float(a), resid_implied=float(b))
    out.close()


def _coef_from_slope_aspect(slope: float, aspect: float):
    g = np.tan(np.radians(slope))
    a = np.radians(aspect)
    # downslope is (sin a, cos a), and the gradient points upslope:
    return -g * np.sin(a), -g * np.cos(a)


# --- main ---

def _jobs(elevation, todo, footprints, ground, data, methods, keep):
    for bid in todo:
        yield (elevation, bid, footprints[bid][0], ground.get(bid),
               {m: data[m]["planes"].get(bid, []) for m in methods},
               {m: data[m]["mess"].get(bid, []) for m in methods}, keep)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("results", nargs="+", help="name=results.gpkg, one per method")
    parser.add_argument("--lidar", nargs="+", required=True, help="the LiDAR the runs used (files or dirs)")
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--pg-uri", help="database with the job's schema, for ground heights the "
                                         "GeoPackages don't have")
    parser.add_argument("--job-id", type=int, default=0)
    parser.add_argument("--review", type=int, default=50, help="buildings per divergence list")
    parser.add_argument("--skew-review", type=int, default=100, help="planes per skew list per method")
    parser.add_argument("--workers", type=int, default=max(os.cpu_count() - 2, 1))
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s")
    os.makedirs(args.out_dir, exist_ok=True)

    methods_paths = dict(s.split("=", 1) for s in args.results)
    methods = list(methods_paths)
    logging.info(f"Loading {', '.join(methods)}...")
    with ProcessPoolExecutor(len(methods)) as ex:
        data = dict(zip(methods, ex.map(load_method, methods_paths.values())))

    footprints = data[methods[0]]["buildings"]
    ground_source = next((m for m in methods if data[m]["has_ground"]), None)
    if args.pg_uri:
        ground = _ground_heights_from_db(args.pg_uri, args.job_id)
    elif ground_source:
        ground = {bid: b[1] for bid, b in data[ground_source]["buildings"].items()}
    else:
        parser.error("no GeoPackage has ground heights (from an older CLI?): pass --pg-uri")
    todo = [bid for bid, (geom, _, excl) in footprints.items()
            if geom is not None and excl not in PRE_DETECTION_EXCLUSIONS]

    work = tempfile.mkdtemp(prefix="roofdet_eval_")
    all_bounds = shapely.bounds([footprints[b][0] for b in todo])
    bounds = (*all_bounds[:, :2].min(axis=0), *all_bounds[:, 2:].max(axis=0))
    elevation = build_elevation(args.lidar, bounds, work)
    res = abs(gdal.Open(elevation).GetGeoTransform()[1])

    logging.info(f"Evaluating {len(todo)} buildings...")
    results = []
    with ProcessPoolExecutor(args.workers) as ex:
        for i, r in enumerate(ex.map(evaluate_building, _jobs(elevation, todo, footprints, ground, data, methods, False),
                                     chunksize=50)):
            results.append(r)
            if i % 5000 == 0:
                logging.info(f"{i} / {len(todo)}")
    rs = [r for r in results if r["methods"]]
    for r in rs:
        _divergence(r, methods)
    skew_rows = [x for r in rs for x in r["skew"]]

    summary = summarise(rs, methods, "all")
    skew_summary = summarise_skew(skew_rows, methods, "all")
    for name, lo, hi in SIZES:
        sub = [r for r in rs if lo <= r["footprint_m2"] < hi]
        if sub:
            summary += summarise(sub, methods, name)
            skew_summary += summarise_skew([x for x in skew_rows if lo <= x["footprint_m2"] < hi], methods, name)
    write_csv(summary, join(args.out_dir, "summary.csv"))
    write_csv(summarise_pairs(rs), join(args.out_dir, "pairs.csv"))
    write_csv(skew_summary, join(args.out_dir, "skew_summary.csv"))
    write_csv(skew_rows, join(args.out_dir, "plane_skew.csv"))

    review = divergent(rs, methods, args.review)
    by_id = {r["building_id"]: r for r in rs}
    rows = []
    for bid, lists in review.items():
        r = by_id[bid]
        row = {"building_id": bid, "review": lists, "footprint_m2": r["footprint_m2"], "best_energy": r["best_energy"],
               "kwh_spread": r["kwh_spread"], "kwh_rel_spread": r["kwh_rel_spread"], "min_agreement": r["min_agreement"]}
        for m in methods:
            mm = r["methods"][m]
            row.update({f"{m}_kwh": mm["kwh"], f"{m}_planes": mm["planes"], f"{m}_usable": mm["usable_planes"],
                        f"{m}_energy_px": mm["energy"] / r["n_above"], f"{m}_explained": mm["explained"],
                        f"{m}_mae": mm["mae"]})
        rows.append(row)
    write_csv(sorted(rows, key=lambda r: -r["kwh_spread"]), join(args.out_dir, "divergent_buildings.csv"))
    skewed = skew_review(skew_rows, methods, args.skew_review)
    write_csv(skewed, join(args.out_dir, "skew_review.csv"))

    # pixels and labels again, for just the buildings the review GeoPackage shows them for:
    detail_ids = sorted(set(review) | {x["building_id"] for x in skewed})
    logging.info(f"Writing review GeoPackage ({len(detail_ids)} buildings in detail)...")
    with ProcessPoolExecutor(args.workers) as ex:
        detail = {r["building_id"]: r for r in ex.map(
            evaluate_building, _jobs(elevation, detail_ids, footprints, ground, data, methods, True))}
    write_review(join(args.out_dir, "review.gpkg"), rs, review, detail, skewed, methods, data, res)

    print(markdown_table(summary))
    print()
    print(markdown_table(summarise_pairs(rs)))
    print()
    print(markdown_table(skew_summary))


if __name__ == "__main__":
    main()
