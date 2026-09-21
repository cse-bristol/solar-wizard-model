# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
"""
Standalone command-line entrypoint for the solar PV model.

Reads buildings from a vector file (any OGR format - GPKG, Shapefile, GeoJSON,
...) and elevation from LiDAR rasters on disk, runs the model, and writes the
two result tables out as layers of a single GeoPackage. With no --pg-uri it
spins up a throwaway PostGIS cluster for the run.

    python -m solar_pv --buildings x.gpkg --lidar tiles/ --out results.gpkg
"""
import argparse
import logging
import os
import tempfile
from typing import List, Optional

from osgeo import ogr, osr
from psycopg2.sql import SQL, Literal

from solar_pv.buildings import BuildingInput
from solar_pv.db_funcs import command_to_gpkg, connection
from solar_pv.ephemeral_postgres import ephemeral_postgres
from solar_pv.lidar.lidar import LidarTile
from solar_pv.model_solar_pv import model_solar_pv

_LIDAR_EXTENSIONS = (".tif", ".tiff", ".vrt")

# CLI tuning options, mapped to model_solar_pv kwargs. Defaults live on
# model_solar_pv - options left unset here are simply not passed, so there is one
# source of truth for each default.
_TUNING_OPTIONS = {
    "horizon_search_radius": int,
    "horizon_slices": int,
    "max_roof_slope_degrees": int,
    "min_roof_area_m": int,
    "min_roof_degrees_from_north": int,
    "flat_roof_degrees": int,
    "peak_power_per_m2": float,
    "pv_tech": str,
    "min_dist_to_edge_m": float,
}

def read_buildings(path: str,
                   id_field: Optional[str] = None,
                   height_field: Optional[str] = None) -> List[BuildingInput]:
    """
    Read buildings from an OGR vector file into BuildingInput objects, reprojecting
    the geometry to EPSG:27700 (the file's own SRS is honoured).

    building_id comes from `id_field` (or the feature id if unset); height from
    `height_field` (or None). Only the first layer is read.
    """
    ogr.UseExceptions()
    datasource = ogr.Open(path)
    if datasource is None:
        raise ValueError(f"Could not open buildings file {path}")

    layer = datasource.GetLayer(0)
    src_srs = layer.GetSpatialRef()
    if src_srs is None:
        raise ValueError(f"Buildings file {path} has no spatial reference system")

    dst_srs = osr.SpatialReference()
    dst_srs.ImportFromEPSG(27700)
    # x=easting, y=northing on both sides regardless of each CRS's declared axis order:
    src_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    dst_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    transform = osr.CoordinateTransformation(src_srs, dst_srs)

    buildings = []
    for feature in layer:
        geom = feature.GetGeometryRef()
        if geom is None:
            continue
        geom = geom.Clone()
        geom.Transform(transform)

        building_id = str(feature.GetField(id_field)) if id_field \
            else str(feature.GetFID())
        height = feature.GetField(height_field) if height_field else None

        buildings.append(BuildingInput(
            building_id=building_id, geom_27700=geom.ExportToWkt(), height=height))

    return buildings


def discover_lidar(paths: List[str]) -> List[LidarTile]:
    """
    Turn the --lidar arguments (raster files, or directories of them) into
    LidarTile objects. The selector derives each tile's resolution from the raster
    itself, so no per-file metadata is needed here.
    """
    tiles = []
    for path in paths:
        if os.path.isdir(path):
            for name in sorted(os.listdir(path)):
                if name.lower().endswith(_LIDAR_EXTENSIONS):
                    tiles.append(LidarTile(filename=os.path.join(path, name)))
        else:
            tiles.append(LidarTile(filename=path))
    return tiles


def export_results(pg_uri: str, job_id: int, out: str) -> None:
    """Write models.pv_building and models.pv_roof_plane for this job as two
    layers of the GeoPackage at `out`."""
    with connection(pg_uri) as pg_conn:
        err = command_to_gpkg(
            pg_conn, pg_uri, out, "pv_building",
            SQL("SELECT * FROM models.pv_building WHERE job_id = {job_id}"),
            src_srs=27700, dst_srs=27700, overwrite=True,
            job_id=Literal(job_id))
        if err:
            raise RuntimeError(f"Failed to export pv_building: {err}")

        err = command_to_gpkg(
            pg_conn, pg_uri, out, "pv_roof_plane",
            SQL("SELECT * FROM models.pv_roof_plane WHERE job_id = {job_id}"),
            src_srs=4326, dst_srs=4326, append=True,
            job_id=Literal(job_id))
        if err:
            raise RuntimeError(f"Failed to export pv_roof_plane: {err}")


def check_proj_datumgrid(pg_uri: str) -> None:
    """
    Check postGIS can do accurate 4326->27700 transforms, i.e. its proj has the
    OSTN15 datum grids.
    """
    with connection(pg_uri) as pg_conn, pg_conn.cursor() as cursor:
        cursor.execute("""
            SELECT (ABS(ST_X(p) - 292184.870542716) + ABS(ST_Y(p) - 168003.465539408)) > 1E-3
            FROM (
                SELECT ST_Transform(
                    'POINT(-3.55128349240 51.40078220140)',
                    '+proj=longlat +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +no_defs',
                    '+proj=tmerc +lat_0=49 +lon_0=-2 +k=0.9996012717 +x_0=400000 +y_0=-100000 +ellps=airy +nadgrids=@OSTN15_NTv2_OSGBtoETRS.gsb +units=m +no_defs'
                ) p
            ) a
            """)
        misconfigured = cursor.fetchone()[0]
    if misconfigured:
        raise EnvironmentError(
            "postGIS proj is missing the OSTN15 datum grids, so 27700 transforms "
            "would be wrong. Run inside the project's nix-shell, or install the "
            "proj datum grids (see README).")


def _parse_args(argv: List[str]):
    parser = argparse.ArgumentParser(
        prog="solar_pv", description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)

    parser.add_argument("--buildings", required=True,
                        help="Vector file of building footprints (any OGR format)")
    parser.add_argument("--lidar", required=True, nargs="+",
                        help="LiDAR raster files (GeoTIFF/VRT) or directories of them")
    parser.add_argument("--out", required=True,
                        help="Output GeoPackage path (two layers: pv_building, pv_roof_plane)")

    parser.add_argument("--pg-uri", dest="pg_uri", default=None,
                        help="postGIS connection URI; if omitted a throwaway "
                             "cluster is started for the run")
    parser.add_argument("--id-field", dest="id_field", default=None,
                        help="Buildings field to use as building_id (default: feature id)")
    parser.add_argument("--height-field", dest="height_field", default=None,
                        help="Buildings field to use as height (default: none)")
    parser.add_argument("--job-id", dest="job_id", type=int, default=0,
                        help="Job id the result rows are keyed on (default: 0)")
    parser.add_argument("--work-dir", dest="work_dir", default=None,
                        help="Directory for the run's temporary files "
                             "(default: a new temp directory)")
    parser.add_argument("--keep", action="store_true",
                        help="Keep the schema, temp files, and (if ephemeral) the "
                             "database, for inspection")

    for name, cast in _TUNING_OPTIONS.items():
        parser.add_argument(f"--{name.replace('_', '-')}", dest=name,
                            type=cast, default=None,
                            help=f"Override the model's default {name}")

    args = parser.parse_args(argv)
    if args.work_dir is None:
        args.work_dir = tempfile.mkdtemp(prefix="solar_pv_")
    return args


def _run(pg_uri: str, args, buildings: List[BuildingInput],
         lidar_tiles: List[LidarTile]) -> None:
    check_proj_datumgrid(pg_uri)

    tuning = {name: getattr(args, name) for name in _TUNING_OPTIONS
              if getattr(args, name) is not None}

    model_solar_pv(
        pg_uri=pg_uri,
        root_solar_dir=args.work_dir,
        lidar_dir=args.work_dir,
        job_id=args.job_id,
        buildings=buildings,
        lidar_tiles=lidar_tiles,
        debug_mode=args.keep,
        **tuning)

    export_results(pg_uri, args.job_id, args.out)
    logging.info(f"Wrote results to {args.out}")


def main(argv: List[str] = None) -> None:
    args = _parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s")

    buildings = read_buildings(args.buildings, args.id_field, args.height_field)
    logging.info(f"Read {len(buildings)} buildings from {args.buildings}")
    lidar_tiles = discover_lidar(args.lidar)
    logging.info(f"Found {len(lidar_tiles)} LiDAR tiles")

    if args.pg_uri:
        _run(args.pg_uri, args, buildings, lidar_tiles)
    else:
        with ephemeral_postgres(keep=args.keep) as pg_uri:
            _run(pg_uri, args, buildings, lidar_tiles)


if __name__ == "__main__":
    main()
