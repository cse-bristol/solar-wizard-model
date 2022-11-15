from albion_models.db_funcs import command_to_gpkg
from albion_models.solar_pv.open_solar.tippecanoe import cmd_tippecanoe

_LA = "la"


def export(pg_conn, pg_uri: str, gpkg_fname: str):
    if command_to_gpkg(
        pg_conn, pg_uri, gpkg_fname, _LA,
        src_srs=4326, dst_srs=4326,
        overwrite=True,
        command=
        "SELECT "
        " la_code, "
        " name, "
        " geom_4326, "
        " ST_AsGeoJSON(geom_4326) as geom_str "
        "FROM boundaryline.local_authority"
    ) is not None:
        raise RuntimeError(f"Error running ogr2ogr")

    cmd_tippecanoe(gpkg_fname, _LA)
