# Solar wizard PV model

The rooftop solar PV suitability model backing [solarwizard.org.uk](https://solarwizard.org.uk).

## Dependencies and setup

The main entrypoint of the model is the function `model_solar_pv` in module `solar_pv.model_solar_pv`.

The model has the following software dependencies:
* various python libraries (see `requirements.txt`, can also be installed using nix - see `default.nix`)
* [postgreSQL](https://www.postgresql.org/) and [postGIS](https://postgis.net/)
* [PVMAPS](https://joint-research-centre.ec.europa.eu/pvgis-online-tool/pvgis-data-download/pvmaps_en) (a GRASS GIS plugin written in C) - this can be installed using `default.nix` using nix

The model has the following data dependencies:
* building footprint geometries
* LiDAR elevation rasters
* irradiation raster data, which can be downloaded from the PVMAPS link above, or from https://re.jrc.ec.europa.eu/pvmaps/pvgis_data.tar

We have tried to make the model as independent as possible from our internal infrastructure where it runs, but this has not been our main priority when developing and you may find things that don't work, or design decisions that don't make sense when viewed without the context of knowing how we run the model.

### postgres and postGIS

You will need a postGIS (postgres version >= 12; postGIS version >=3) database with some Albion data in it (see below).

The postGIS install will also need to have access to some proj datum grids, which are used for more accurate transformations between long/lat and easting/northing.

To test your postGIS install, try the following:
```sql
SELECT  ST_AsText(
            ST_Transform(
                'POINT(-3.55128349240 51.40078220140)',
                '+proj=longlat +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +no_defs',
                '+proj=tmerc +lat_0=49 +lon_0=-2 +k=0.9996012717 +x_0=400000 +y_0=-100000 +ellps=airy +nadgrids=@OSTN15_NTv2_OSGBtoETRS.gsb +units=m +no_defs'
            )
        )
;
                st_astext                 
------------------------------------------
 POINT(292184.870542716 168003.465539408)     -- Test point: 292184.870 168003.465
```
(test source: https://gis.stackexchange.com/a/396980)

If the `proj` being used by postGIS doesn't have the datum grids, the result will be way off. It will only be correct if the file `OSTN15_NTv2_OSGBtoETRS.gsb` is in the directory indicated by the environment variable `PROJ_LIB` (or its default location of `/usr/share/proj` or `/usr/local/share/proj`, depending on distro, if `PROJ_LIB` is unset). If your proj version is < 7, this file is found in the project [proj-datumgrid](https://github.com/OSGeo/proj-datumgrid). If it is >=7, it is found in the project [proj-data](https://github.com/OSGeo/PROJ-data). Run `SELECT PostGIS_PROJ_Version();` To get the proj version.

The postGIS in some distro package managers (e.g. debian-based ones) do include the datum grids by default; however the nixpkgs postGIS does not.

There will need to be at least the following table in the postGIS install:

* schema: `mastermap`
* table: `building_27700`
* columns: `toid TEXT`, `geom_27700 geometry(Polygon,27700)`. It can have others but these are the only ones required.

Optionally, this table will also be used if present. It is only used to burn in buildings missing from the LiDAR as obstacles to be used when detecting the horizon profiles of present buildings.

* schema: `mastermap`
* table: `height`
* columns: `toid TEXT`, `abs_hmax`, `abs_h2`. It can have others but these are the only ones required.

We use the unique building ID (TOID) and building footprint geometry from OS mastermap (hence the table names) - however this is not necessarily required: as long as the polygons align properly with the LiDAR used, any building geometry and height data could be used. TOIDs are open-licensed data but height and geometry are not.

### LiDAR

The model can use LiDAR in geoTIFF format at resolutions 50cm, 1m or 2m. 1m is ideal as 2m is too low-resolution to pick up many features and 50cm increases the time taken to fit planes to LiDAR. The model expects LiDAR tiles to be pre-loaded as out-of-band rasters into postGIS in the tables `models.lidar_50cm`, `models.lidar_1m`, and `models.lidar_2m`. 

Two Python modules are included which perform this task in different ways - see `solar_pv.lidar.bulk_lidar_client` and `solar_pv.lidar.defra_lidar_api_client`, but as long as the LiDAR ends up in the right tables any other method is fine too.

### Environment variables

* `LIDAR_DIR` - dir to store downloaded LiDAR tiles and tiles that have been extracted and processed from the raw format
* `BULK_LIDAR_DIR` - This can be ignored unless using `solar_pv.lidar.bulk_lidar_client` to load LiDAR. This is the directory containing bulk LiDAR for England, Scotland and Wales. This should have the following directory structure:
  * 206817_LIDAR_Comp_DSM
    * LIDAR-DSM-50CM-ENGLAND-EA
    * LIDAR-DSM-1M-ENGLAND-EA
    * LIDAR-DSM-2M-ENGLAND-EA
  * scotland
  * wales
* `SOLAR_DIR` - dir to store outputs and intermediate stages of solar PV modelling. Final outputs are in the database.
* `PVGIS_DATA_TAR_FILE_DIR` - The directory containing the `pvgis_data.tar` file
* `PVGIS_GRASS_DBASE_DIR` - where to create the GRASS dbase for PVMAPS
* `USE_LIDAR_FROM_API` - This can be ignored unless using `solar_pv.lidar.bulk_lidar_client` to load LiDAR. If set to a value Python will coerce to True, allow falling back to the DEFRA LiDAR API if relevant LiDAR tiles are not found in the bulk LiDAR. This can be left unset, in which case the API will never be used.

## Tests

Some of the tests require the PVMAPS irradiation data.
* Download the 10GB tar file from https://re.jrc.ec.europa.eu/pvmaps/pvgis_data.tar and either place it at, or symlink to it from, `test_data/pvmaps/pvgis_data_dir/pvgis_data.tar`
* run `python3 -m unittest`

## Development

`default.nix` contains the dependencies required to setup a dev environment. The `default.nix` in this repo also has the dependencies required for the repositories `320-albion` and `320-albion-webapp`.

Run `nix-build default.nix -o nixenv` to create something equivalent to a python virtualenv that you can tell your IDE to use as a virtualenv. 

I also start pycharm from a nix-shell (command plain `nix-shell`, will use `default.nix`) using `/opt/pycharm-community-2020.3.3/bin/pycharm.sh  > /dev/null 2>&1 &` so that it can understand the dependencies properly. This isn't ideal as you have to rebuild nixenv and restart the nix-shell and pycharm whenever you change a dependency, but that doesn't happen often.