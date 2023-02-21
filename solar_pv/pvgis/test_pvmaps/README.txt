test_data setup
===============
In test_data/pvmaps/pvgis_data_dir:
Put (or symlink) pvgis_data.tar in this dir (from https://re.jrc.ec.europa.eu/pvmaps/pvgis_data.tar)

Running pytests with pycharm
============================
- Use 320-albion/dev/shell.nix with nix-shell:
    cd 320-albion/dev
    nix-shell
- Link to Python location is created at 320-albion/dev/python
- Set this python location in pycharm project
- Run tests by right clicking on test_pvmaps_real_data.py or test_pvmaps_test_data.py
