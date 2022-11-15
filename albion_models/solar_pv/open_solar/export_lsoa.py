from albion_models.db_funcs import command_to_gpkg
from albion_models.solar_pv.open_solar.tippecanoe import cmd_tippecanoe

_LSOA = "lsoa"


def export(pg_conn, pg_uri: str, gpkg_fname: str):
    if command_to_gpkg(
        pg_conn, pg_uri, gpkg_fname, _LSOA,
        src_srs=4326, dst_srs=4326,
        overwrite=True,
        command=
        "SELECT "
        " lsoa_2011, "
        " name, "
        " geom_4326, "
        " ST_AsGeoJSON(geom_4326) AS geom_str "
        "FROM census_boundaries.lsoa"
    ) is not None:
        raise RuntimeError(f"Error running ogr2ogr")

    cmd_tippecanoe(gpkg_fname, _LSOA)
