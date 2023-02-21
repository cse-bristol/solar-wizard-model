# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
from os.path import abspath, dirname, join

current_dir = abspath(dirname(__file__))

SRC_DIR = abspath(current_dir)
PROJECT_ROOT = abspath(join(SRC_DIR, ".."))
BIN_DIR = abspath(join(PROJECT_ROOT, "bin"))
SQL_DIR = abspath(join(PROJECT_ROOT, "database"))
TEST_DATA = abspath(join(PROJECT_ROOT, "testdata"))
RESOURCES_DIR = abspath(join(PROJECT_ROOT, "resources"))
