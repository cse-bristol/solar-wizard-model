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