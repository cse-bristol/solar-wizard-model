# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
"""
How to mount panels on a roof, in general (docs/flat-roofs-and-mounting.md): a one-off study.

For each way a roof could carry panels - flush with it, in rows tilted towards the building
face nearest south, or in east-west pairs on a flat roof - this finds the yield per kWp, from
the model's own PV code (`run_pv.compute_pv_flat`) at a few UK locations, and the panel area
per m² of roof footprint, from the layout's geometry. Their product is the energy per m² of
roof. Rows are spaced as the model spaces them (no shading while the sun is above
FLAT_ROOF_ROW_SPACING_SUN_ELEVATION_DEGREES in the direction they face), allowing for the
roof's fall. The row in front, or the next east-west pair, is part of each panel's horizon,
averaged over points up the panel; the model itself leaves it out.

A roof is described by its slope and aspect, which for a flat roof stands for the building's
orientation: rows face the building face (or right angle to one) nearest south, and
east-west pairs face at right angles to that.

    python -m solar_pv.evaluation.mounting_study --met-tar pvgis_data_uk.tar --out mounting.csv
"""
import argparse
import csv
import math
from typing import Callable, Dict, List, Optional, Tuple

import numpy as np

from solar_pv.constants import FLAT_ROOF_ROW_SPACING_SUN_ELEVATION_DEGREES, SYSTEM_LOSS
from solar_pv.evaluation.roof_detection import markdown_table
from solar_pv.paths import RESOURCES_DIR
from solar_pv.pv.met_data import MetData
from solar_pv.pv.run_pv import compute_pv_flat, _load_coeffs

# EPSG:27700 points, south to north:
LOCATIONS = {
    "penzance": (147000, 30000),
    "london": (530000, 180000),
    "bristol": (359000, 172000),
    "birmingham": (407000, 286000),
    "manchester": (384000, 398000),
    "newcastle": (425000, 564000),
    "edinburgh": (325000, 673000),
    "inverness": (266000, 845000),
}
REFERENCE_LOCATION = "bristol"


def _band(lo: float, hi: float, elevation: float) -> Callable[[np.ndarray], np.ndarray]:
    return lambda az: np.where((az >= lo) & (az <= hi), elevation, 0.0)


# Horizons around the roof (elevation in degrees by compass azimuth), standing in for what
# the LiDAR horizon would show:
SITE_HORIZONS = {
    "open": lambda az: np.zeros_like(az),
    "all round 10": lambda az: np.full_like(az, 10.0),
    "all round 20": lambda az: np.full_like(az, 20.0),
    "south 25": _band(135, 225, 25.0),
}
REFERENCE_HORIZON = "open"

FLUSH_SLOPES = [0, 2.5, 5, 7.5, 10, 12.5, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70]
ROW_SLOPES = [0, 2.5, 5, 7.5, 10, 12.5, 15]
ASPECTS = list(range(0, 360, 15))
# The model's default `flat_roof_degrees`:
TILT = 10.0
# Points up each panel the row horizon is sampled at, as fractions of its length:
PANEL_POINTS = (0.1, 0.3, 0.5, 0.7, 0.9)
# East-west pairs: the length of a panel up its slope (m, a module in landscape), and the gaps
# between pairs (m) to try:
EW_PANEL_M = 1.1
EW_GAPS_M = (0.1, 0.3, 0.6)
# Roofs up to this slope are flat (FLAT_ROOF_DEGREES_THRESHOLD, as planned) and can't carry
# flush panels:
FLAT_MAX_SLOPE = 2.5
PEAK_POWER_PER_M2 = 0.2
FLOORS = (0.6, 0.7, 0.75, 0.8, 0.85, 0.9)
BREAKEVENS = (0.3, 0.4, 0.5, 0.6, 0.7, 0.75, 0.8, 0.85)
HORIZON_STEP_DEG = 10.0
_ELEVATION_M = 50.0


def fall_towards(slope: float, roof_aspect: float, azimuth: float) -> float:
    """How far (degrees) the roof falls in the direction `azimuth`: negative if it rises."""
    return math.degrees(math.atan(math.tan(math.radians(slope)) * math.cos(math.radians(roof_aspect - azimuth))))


def nearest_south_face(roof_aspect: float) -> float:
    """Of the roof's aspect and the right angles to it, the one nearest south."""
    return min(((roof_aspect + turn) % 360 for turn in (0, 90, 180, 270)), key=lambda a: abs(a - 180))


def panel_orientation(slope: float, roof_aspect: float, facing: float, tilt: float) -> Tuple[float, float]:
    """
    The slope and aspect of panels in rows facing `facing`, on frames that tilt them at `tilt`
    if the roof only falls in the direction they face (see `fall_towards`). On a roof that also
    falls across them, they follow it across. A flat roof counts as level.
    """
    if slope <= FLAT_MAX_SLOPE:
        return tilt, facing
    s, a, f = (math.radians(v) for v in (slope, roof_aspect, facing))
    roof = np.array([math.sin(s) * math.sin(a), math.sin(s) * math.cos(a), math.cos(s)])
    ahead = np.array([math.sin(f), math.cos(f), 0.0])
    ahead = ahead - (ahead @ roof) * roof
    ahead /= np.linalg.norm(ahead)
    lift = math.radians(tilt - fall_towards(slope, roof_aspect, facing))
    normal = math.cos(lift) * roof + math.sin(lift) * ahead
    return math.degrees(math.acos(normal[2])), math.degrees(math.atan2(normal[0], normal[1])) % 360


def row_pitch(tilt: float, fall: float, sun: float = FLAT_ROOF_ROW_SPACING_SUN_ELEVATION_DEGREES) -> Optional[float]:
    """
    The horizontal distance between rows of panels one unit long, tilted at `tilt`, on a roof
    falling at `fall` in the direction they face, so the row in front only shades the next
    while the sun there is below `sun`. None if rows can't be laid there: the roof falls more
    steeply than the panels are tilted, or rises more steeply than `sun`.
    """
    t, d, e = (math.radians(v) for v in (tilt, fall, sun))
    if fall >= tilt or e + d <= 0:
        return None
    return (math.sin(t) + math.cos(t) * math.tan(e)) / (math.tan(e) + math.tan(d))


def row_horizon(tilt: float, fall: float, pitch: float, up: float, relative_az: np.ndarray) -> np.ndarray:
    """
    The elevation (degrees) of the top of the row in front, from the point `up` (a fraction)
    of the way up a panel, in directions `relative_az` (degrees) from the way it faces. Rows
    are long enough to count as endless.
    """
    t, d = math.radians(tilt), math.radians(fall)
    dx = pitch - math.cos(t) + up * math.cos(t)
    dz = math.sin(t) - pitch * math.tan(d) - up * math.sin(t)
    cos = np.cos(np.radians(relative_az))
    return np.where(cos > 0, np.degrees(np.arctan(np.maximum(dz, 0) * np.maximum(cos, 0) / dx)), 0.0)


def ew_panel_area(tilt: float, gap_m: float, panel_m: float = EW_PANEL_M) -> float:
    """Panel area per unit of footprint of east-west pairs tilted at `tilt`, `gap_m` apart."""
    return 2 * panel_m / (2 * panel_m * math.cos(math.radians(tilt)) + gap_m)


def ew_horizon(tilt: float, gap_m: float, up: float, relative_az: np.ndarray, panel_m: float = EW_PANEL_M) -> np.ndarray:
    """As `row_horizon`, for a panel in an east-west pair: the ridge of the next pair."""
    t = math.radians(tilt)
    dx = gap_m + panel_m * math.cos(t) * (1 + up)
    dz = panel_m * math.sin(t) * (1 - up)
    cos = np.cos(np.radians(relative_az))
    return np.where(cos > 0, np.degrees(np.arctan(dz * np.maximum(cos, 0) / dx)), 0.0)


def _relative(az: np.ndarray, facing: float) -> np.ndarray:
    return (az - facing + 180) % 360 - 180


class _Batch:
    """Panels to run through the PV model, each a sample of one layout on one roof."""

    def __init__(self, compass_az: np.ndarray):
        self.az = compass_az
        self.slopes: List[float] = []
        self.aspects: List[float] = []
        self.horizons: List[np.ndarray] = []
        self.keys: List[tuple] = []

    def add(self, key: tuple, tilt: float, facing: float, horizon: np.ndarray) -> None:
        self.keys.append(key)
        self.slopes.append(tilt)
        # 0 means flat to the PV model:
        self.aspects.append(0.0 if tilt == 0 else (facing % 360 or 360.0))
        self.horizons.append(horizon)


def _layouts(site: np.ndarray, batch: _Batch, horizon_name: str) -> Dict[tuple, dict]:
    """Add each layout on each roof to `batch`; return each (layout, slope, aspect[, gap]) key's
    panel area per m² of footprint and the way its panels face."""
    layouts = {}
    for slope in FLUSH_SLOPES:
        for aspect in ASPECTS:
            key = ("flush", slope, aspect, None)
            layouts[key] = {"panel_area": 1 / math.cos(math.radians(slope)), "panel_slope": slope,
                            "panel_aspect": aspect}
            batch.add((horizon_name,) + key, slope, aspect, site)
    for slope in ROW_SLOPES:
        for aspect in ASPECTS:
            facing = nearest_south_face(aspect)
            fall = 0.0 if slope <= FLAT_MAX_SLOPE else fall_towards(slope, aspect, facing)
            pitch = row_pitch(TILT, fall)
            if pitch is None:
                continue
            key = ("rows", slope, aspect, None)
            panel_slope, panel_aspect = panel_orientation(slope, aspect, facing, TILT)
            layouts[key] = {"panel_area": 1 / pitch, "panel_slope": panel_slope, "panel_aspect": panel_aspect,
                            "fall": fall}
            for up in PANEL_POINTS:
                rows = row_horizon(TILT, fall, pitch, up, _relative(batch.az, panel_aspect))
                batch.add((horizon_name,) + key, panel_slope, panel_aspect, np.maximum(site, rows))
    for slope in [s for s in ROW_SLOPES if s <= FLAT_MAX_SLOPE]:
        for aspect in ASPECTS:
            south = nearest_south_face(aspect)
            for gap in EW_GAPS_M:
                key = ("east-west", slope, aspect, gap)
                layouts[key] = {"panel_area": ew_panel_area(TILT, gap), "panel_slope": TILT, "panel_aspect": south}
                for facing in (south - 90, south + 90):
                    for up in PANEL_POINTS:
                        pair = ew_horizon(TILT, gap, up, _relative(batch.az, facing))
                        batch.add((horizon_name,) + key, TILT, facing, np.maximum(site, pair))
    return layouts


def run(met_tar: str) -> List[dict]:
    """Per location, site horizon, layout and roof: yield per kWp (before system losses), panel
    area and energy per m² of footprint, and yield as a share of the best flush orientation's
    there with an open horizon."""
    coeffs = _load_coeffs("cSi", RESOURCES_DIR)
    n_dir = int(round(360 / HORIZON_STEP_DEG))
    # horizon directions are CCW from East:
    compass_az = (90 - np.arange(n_dir) * HORIZON_STEP_DEG) % 360
    out = []
    for location, (x, y) in LOCATIONS.items():
        batch = _Batch(compass_az)
        layouts = {}
        for horizon_name, horizon in SITE_HORIZONS.items():
            layouts[horizon_name] = _layouts(horizon(compass_az), batch, horizon_name)
        n = len(batch.keys)
        gt = (float(x), 1.0, 0.0, float(y), 0.0, -1.0)
        _, kwh = compute_pv_flat(np.zeros(n, dtype=int), np.zeros(n, dtype=int),
                                 np.array(batch.slopes), np.array(batch.aspects), np.full(n, _ELEVATION_M),
                                 np.radians(np.array(batch.horizons)), gt, HORIZON_STEP_DEG,
                                 MetData(met_tar, gt, (1, 1)), coeffs)
        samples: Dict[tuple, List[float]] = {}
        for key, k in zip(batch.keys, kwh):
            samples.setdefault(key, []).append(float(k))
        # shading counts against a roof, so yields are shares of the best with an open horizon:
        best = max(np.mean(samples[(REFERENCE_HORIZON,) + key])
                   for key in layouts[REFERENCE_HORIZON] if key[0] == "flush")
        for horizon_name, by_key in layouts.items():
            for key, layout in by_key.items():
                kwh_kwp = float(np.mean(samples[(horizon_name,) + key]))
                out.append({"location": location, "horizon": horizon_name, "layout": key[0],
                            "slope": key[1], "aspect": key[2], "gap_m": key[3],
                            "panel_slope": layout["panel_slope"], "panel_aspect": layout["panel_aspect"],
                            "fall": layout.get("fall"),
                            "kwh_per_kwp": kwh_kwp, "yield_share": kwh_kwp / best,
                            "panel_area": layout["panel_area"],
                            "kwh_per_m2": kwh_kwp * layout["panel_area"] * PEAK_POWER_PER_M2 * (1 - SYSTEM_LOSS)})
    return out


# --- summaries ---

def _from_north(aspect: float) -> float:
    return min(aspect, 360 - aspect)


def _select(rs: List[dict], **match) -> List[dict]:
    return [r for r in rs if all(r[k] == v for k, v in match.items())]


def candidates(rs: List[dict], slope: float, aspect: float, gap_m: float = 0.3) -> List[dict]:
    """The layouts a roof could carry: rows; on a flat roof east-west pairs, else flush panels."""
    return [r for r in rs if r["slope"] == slope and r["aspect"] == aspect
            and (r["layout"] == "rows"
                 or (r["layout"] == "flush" and slope > FLAT_MAX_SLOPE)
                 or (r["layout"] == "east-west" and r["gap_m"] == gap_m and slope <= FLAT_MAX_SLOPE))]


def choose(rs: List[dict], slope: float, aspect: float, floor: float, gap_m: float = 0.3) -> Optional[dict]:
    """The layout with the most energy per m² of roof whose yield is at least `floor` of the best."""
    ok = [r for r in candidates(rs, slope, aspect, gap_m) if r["yield_share"] >= floor]
    return max(ok, key=lambda r: r["kwh_per_m2"]) if ok else None


def value(r: dict, breakeven: float) -> float:
    """Net value per m² of roof, in units of the best panel's: what each panel earns above
    `breakeven` (a yield as a share of the best), times panel area."""
    return r["panel_area"] * (r["yield_share"] - breakeven)


def choose_by_value(rs: List[dict], slope: float, aspect: float, breakeven: float,
                    gap_m: float = 0.3) -> Optional[dict]:
    """The layout with the most net value per m² of roof, if any has some."""
    best = max(candidates(rs, slope, aspect, gap_m), key=lambda r: value(r, breakeven), default=None)
    return best if best is not None and value(best, breakeven) > 0 else None


_CODES = {"flush": "F", "rows": "R", "east-west": "E", None: "-"}
MAP_SLOPES = ROW_SLOPES + [20, 30, 45]


def _code(choice: Optional[dict]) -> str:
    return _CODES[choice["layout"] if choice else None]


def _map(rs: List[dict], rule: Callable, threshold: float) -> List[dict]:
    rows = []
    for slope in MAP_SLOPES:
        row = {"slope": slope}
        for aspect in [a for a in ASPECTS if a <= 180]:
            row[str(aspect)] = _code(rule(rs, slope, aspect, threshold))
        rows.append(row)
    return rows


def _lookup_cost(ref: List[dict], here: List[dict], rule: Callable, threshold: float) -> Tuple[int, int, float]:
    """
    Using the choices made for `ref` on the roofs of `here`: how many roofs' layouts differ;
    how many of those are usable in one and not the other; and, over roofs usable in both,
    the share of the net value at `threshold` (see `value`) lost by using `ref`'s layout.
    """
    index = {(r["layout"], r["slope"], r["aspect"], r["gap_m"]): r for r in here}
    differ = flips = 0
    best = lost = 0.0
    for slope in MAP_SLOPES:
        for aspect in ASPECTS:
            mine, theirs = rule(here, slope, aspect, threshold), rule(ref, slope, aspect, threshold)
            differ += _code(mine) != _code(theirs)
            flips += (mine is None) != (theirs is None)
            if mine is not None and theirs is not None:
                used = index[(theirs["layout"], slope, aspect, theirs["gap_m"])]
                best += value(mine, threshold)
                lost += value(mine, threshold) - value(used, threshold)
    return differ, flips, lost / best if best else 0.0


def summarise(rs: List[dict]) -> str:
    ref = _select(rs, location=REFERENCE_LOCATION, horizon=REFERENCE_HORIZON)
    tables = []

    rows = []
    for location in LOCATIONS:
        here = _select(rs, location=location, horizon=REFERENCE_HORIZON, layout="flush")
        best = max(here, key=lambda r: r["kwh_per_kwp"])
        flat = _select(here, slope=0, aspect=0)[0]
        rows.append({"location": location, "best kWh/kWp": best["kwh_per_kwp"],
                     "best at": f"{best['slope']}° {best['aspect']}°", "horizontal": flat["yield_share"]})
    tables.append(("Best flush orientation, and horizontal panels' share of it (open horizon)", rows))

    half = [a for a in ASPECTS if a <= 180]
    rows = []
    for slope in FLUSH_SLOPES:
        row = {"slope": slope}
        for aspect in half:
            share = [r["yield_share"] for r in _select(rs, horizon=REFERENCE_HORIZON, layout="flush",
                                                       slope=slope, aspect=aspect)]
            row[str(aspect)] = f"{_select(ref, layout='flush', slope=slope, aspect=aspect)[0]['yield_share']:.2f}" \
                               f" ({max(share) - min(share):.2f})"
        rows.append(row)
    tables.append((f"Flush yield as a share of the best, by slope and aspect, at {REFERENCE_LOCATION} "
                   f"(spread over locations)", rows))

    rows = []
    for slope in FLUSH_SLOPES[1:]:
        row = {"slope": slope}
        for floor in FLOORS:
            ok = [_from_north(a) for a in ASPECTS
                  if _select(ref, layout="flush", slope=slope, aspect=a)[0]["yield_share"] >= floor]
            row[f"floor {floor}"] = "none" if not ok else ("any" if min(ok) == 0 else f"≥{min(ok)}°")
        rows.append(row)
    tables.append((f"Flush panels with at least a given yield share: how far from north they must face, at "
                   f"{REFERENCE_LOCATION} (the current rule is ≥45° at any slope)", rows))

    rows = []
    for slope in ROW_SLOPES:
        for aspect in (0, 45, 90, 135, 180):
            horizontal = _select(ref, layout="flush", slope=0, aspect=0) if slope == 0 and aspect == 0 else []
            for r in candidates(ref, slope, aspect) + horizontal:
                rows.append({"slope": slope, "aspect": aspect, "layout": r["layout"],
                             "panel slope": r["panel_slope"], "panel aspect": r["panel_aspect"], "fall": r["fall"],
                             "yield share": r["yield_share"], "panel m²/m²": r["panel_area"],
                             "kWh/m²": r["kwh_per_m2"]})
    tables.append((f"Layouts on shallow roofs at {REFERENCE_LOCATION}, open horizon (east-west with "
                   f"0.3m gaps; flush on a 0° roof for reference only)", rows))

    rows = []
    for gap in EW_GAPS_M:
        for aspect in (0, 15, 30, 45):
            r = _select(ref, layout="east-west", slope=0, aspect=aspect, gap_m=gap)[0]
            south = _select(ref, layout="rows", slope=0, aspect=aspect)[0]
            rows.append({"gap m": gap, "building turned": aspect, "panel m²/m²": r["panel_area"],
                         "yield share": r["yield_share"], "kWh/m²": r["kwh_per_m2"],
                         "rows kWh/m²": south["kwh_per_m2"], "rows yield share": south["yield_share"]})
    tables.append((f"East-west pairs against south rows on a flat roof, at {REFERENCE_LOCATION}", rows))

    tables.append((f"Layout with the most energy per m² of roof, yield share at least 0.8, at "
                   f"{REFERENCE_LOCATION}, open horizon (F flush, R rows, E east-west, - none)",
                   _map(ref, choose, 0.8)))
    for breakeven in (0.5, 0.7, 0.75, 0.8):
        tables.append((f"Layout with the most net value per m² of roof, break-even yield share {breakeven}, "
                       f"at {REFERENCE_LOCATION}, open horizon", _map(ref, choose_by_value, breakeven)))

    rows = []
    n_roofs = len(MAP_SLOPES) * len(ASPECTS)
    for name, rule, thresholds in (("floor", choose, FLOORS), ("break-even", choose_by_value, BREAKEVENS)):
        for threshold in thresholds:
            row = {"rule": f"{name} {threshold}"}
            costs = [_lookup_cost(ref, _select(rs, location=location, horizon=REFERENCE_HORIZON), rule, threshold)
                     for location in LOCATIONS if location != REFERENCE_LOCATION]
            row["locations"] = _cost_cell(costs, rule is choose_by_value)
            for horizon in SITE_HORIZONS:
                if horizon != REFERENCE_HORIZON:
                    here = _select(rs, location=REFERENCE_LOCATION, horizon=horizon)
                    row[horizon] = _cost_cell([_lookup_cost(ref, here, rule, threshold)], rule is choose_by_value)
            rows.append(row)
    tables.append((f"Choosing by {REFERENCE_LOCATION}'s open-horizon layouts instead: roofs (of {n_roofs}) "
                   f"whose layout differs / of those, usable in only one / net value lost where usable in "
                   f"both. Locations: the worst of the others, open horizon; horizons: at {REFERENCE_LOCATION}",
                   rows))

    return "\n\n".join(f"### {title}\n\n{markdown_table(t)}" for title, t in tables)


def _cost_cell(costs: List[Tuple[int, int, float]], with_value: bool) -> str:
    differ, flips, lost = max(costs, key=lambda c: (c[2], c[0]))
    return f"{differ} / {flips}" + (f" / {lost:.1%}" if with_value else "")


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--met-tar", required=True, help="pvgis_data_uk.tar")
    p.add_argument("--out", help="write every result here as CSV")
    args = p.parse_args()
    rs = run(args.met_tar)
    if args.out:
        with open(args.out, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(rs[0].keys()))
            w.writeheader()
            w.writerows(rs)
    print(summarise(rs))


if __name__ == "__main__":
    main()
