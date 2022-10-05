test_data setup
===============
In test_data/pvgis_data_dir:
Put (or symlink) pvgis_data.tar in this dir (from https://re.jrc.ec.europa.eu/pvmaps/pvgis_data.tar)

Running pytests with pycharm
============================
- Use 320-albion/dev-shell.nix with nix-shell
- Find python location from nix with whereis python
- Set python location in pycharm project
- Right clicking on test_pvmaps_real_data.py and test_pvmaps_test_data.py, do "modify run configuration" and
set additional arguments to "--capture=no --log-cli-level=INFO"
- Run tests by right clicking on test_pvmaps_real_data.py or test_pvmaps_test_data.py


