# Albion models

This repository contains code for
* Hard/soft dig modelling
* LIDAR downloading
* Heat demand modelling
* Solar PV modelling

This project uses [git-lfs](https://git-lfs.github.com/) for large file storage (currently just the thermos jar for heat demand estimation), so install that and run `git lfs pull` in the root of this repo after cloning.

See also [320-albion-import](https://github.com/cse-bristol/320-albion), [320-albion-webapp](https://github.com/cse-bristol/320-albion-webapp). This repository is a git submodule of `320-albion-webapp`.

I'm not totally happy with the split across 320-albion-webapp and 320-albion-models - currently the database schema is handled by 320-albion-webapp which is sometimes annoying. It's possible that they shouldn't actually be separate things.

## Development

`default.nix` contains the dependencies required to setup a dev environment. The `default.nix` in this repo also has the dependencies required for the repositories `320-albion` and `320-albion-webapp`.

Run `nix-build default.nix -o nixenv` to create something equivalent to a python virtualenv that you can tell your IDE to use as a virtualenv. 

I also start pycharm from a nix-shell (command plain `nix-shell`, will use `default.nix`) using `/opt/pycharm-community-2020.3.3/bin/pycharm.sh  > /dev/null 2>&1 &` so that it can understand the dependencies properly. This isn't ideal as you have to rebuild nixenv and restart the nix-shell and pycharm whenever you change a dependency, but that doesn't happen often.

## Dependencies

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

It seems that the postGIS in my distro package manager (debian-based) does include the datum grids by default; however the nixpkgs postGIS does not. See [shared-pg](https://github.com/cse-bristol/shared-pg/blob/master/machine-configuration.nix#L10-L14) for an example of how to get a nixpkgs postGIS to detect the datum grids. I don't know if the docker postGIS includes them or not.

### Albion dependencies

At least some bits of the albion database must have been created locally using scripts from [320-albion-import](https://github.com/cse-bristol/320-albion). Currently, the bits required by all are:
* local dev setup: `database/local-dev-setup.sql` - run manually
* basic setup: `database/setup.db.sql` - will be run whenever any import job is run, or can be run manually

Each model also needs some bits of Albion at least partially loaded:

#### solar PV:
* OSMM
* OSMM heights

#### heat demand:
* buildings aggregate

#### hard/soft dig:
* OSMM highways
* OS Greenspace
* OS natural land

## Environment variables

* `LIDAR_DIR` - dir to store downloaded LiDAR tiles and tiles that have been extracted and processed from the raw format
* `BULK_LIDAR_DIR` - directory containing bulk LiDAR for England, Scotland and Wales. This should have the following directory structure:
  * 206817_LIDAR_Comp_DSM
    * LIDAR-DSM-50CM-ENGLAND-EA
    * LIDAR-DSM-1M-ENGLAND-EA
    * LIDAR-DSM-2M-ENGLAND-EA
  * scotland
  * wales
* `HEAT_DEMAND_DIR` - dir to store intermediate stages of heat demand modelling. Final outputs are in the database.
* `SOLAR_DIR` - dir to store outputs and intermediate stages of solar PV modelling. Final outputs are in the database.
* `PVGIS_DATA_DIR` - The directory containing the `pvgis_data.tar` file
* `PVGIS_GRASS_DBASE` - where to create the GRASS dbase for PVMAPS
* `SMTP_FROM` - (optional) email to send notifications from
* `SMTP_PASS` - (optional) password for email to send notifications from
* `EMAIL_TO_NOTIFY_ON_FAILURE` - (optional) email to notify if jobs or result extraction fails
