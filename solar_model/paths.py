from os.path import abspath, dirname, join

current_dir = abspath(dirname(__file__))

SRC_DIR = abspath(current_dir)
PROJECT_ROOT = abspath(join(SRC_DIR, ".."))
BIN_DIR = abspath(join(PROJECT_ROOT, "bin"))
SQL_DIR = abspath(join(PROJECT_ROOT, "database"))
TEST_DATA = abspath(join(PROJECT_ROOT, "testdata"))
RESOURCES_DIR = abspath(join(PROJECT_ROOT, "resources"))
