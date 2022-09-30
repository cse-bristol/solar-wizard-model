# Albion models

This repository contains code for
* Hard/soft dig modelling
* LIDAR downloading
* Heat demand modelling
* Solar PV modelling

This project uses [git-lfs](https://git-lfs.github.com/) for large file storage (currently just the thermos jar for heat demand estimation), so install that and run `git lfs pull` in the root of this repo after cloning.

This project also contains a submodule, so run `git submodule update --init --recursive` after cloning.

See also [320-albion-import](https://github.com/cse-bristol/320-albion), [320-albion-webapp](https://github.com/cse-bristol/320-albion-webapp). This repository is a git submodule of `320-albion-webapp`.

I'm not totally happy with the split across 320-albion-webapp and 320-albion-models - currently the database schema is handled by 320-albion-webapp which is sometimes annoying. It's possible that they shouldn't actually be separate things.

## Development

`default.nix` contains the dependencies required to setup a dev environment. The `default.nix` in this repo also has the dependencies required for the repositories `320-albion` and `320-albion-webapp`.

Run `nix-build default.nix -o nixenv` to create something equivalent to a python virtualenv that you can tell your IDE to use as a virtualenv. 

I also start pycharm from a nix-shell (command plain `nix-shell`, will use `default.nix`) using `/opt/pycharm-community-2020.3.3/bin/pycharm.sh  > /dev/null 2>&1 &` so that it can understand the dependencies properly. This isn't ideal as you have to rebuild nixenv and restart the nix-shell and pycharm whenever you change a dependency, but that doesn't happen often.

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
