# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
"""
Add or update copyright notices in python files under `bin` or `solar_model`, or SQL
files under `database`.
"""
import os
import textwrap
from glob import glob
from solar_pv import paths
from datetime import datetime

year = datetime.now().year
py_copyright = textwrap.dedent(f"""\
# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-{year}
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
""")
sql_copyright = textwrap.dedent(f"""\
-- This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-{year}
-- Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
""")


def _copyright_linenum(copyright: str):
    return len(copyright.split("\n")) - 1


def _has_copyright(f, copyright: str):
    first_lines = ''.join([f.readline() for _ in range(_copyright_linenum(copyright))])
    f.seek(0, 0)
    return "copyright © Centre for Sustainable Energy" in first_lines


def _set_copyright(filepath: str, copyright: str):
    with open(filepath, 'r+') as f:
        if _has_copyright(f, copyright):
            # read past the current copyright:
            [f.readline() for _ in range(_copyright_linenum(copyright))]
        content = f.read()
        f.seek(0, 0)
        f.write(copyright + content)


if __name__ == '__main__':

    for _dir, _, _ in os.walk(paths.SRC_DIR):
        for filepath in glob(os.path.join(_dir, '*.py')):
            if "__init__" not in filepath:
                _set_copyright(filepath, py_copyright)

    for _dir, _, _ in os.walk(paths.BIN_DIR):
        for filepath in glob(os.path.join(_dir, '*.py')):
            if "__init__" not in filepath:
                _set_copyright(filepath, py_copyright)

    for _dir, _, _ in os.walk(paths.SQL_DIR):
        for filepath in glob(os.path.join(_dir, '*.sql')):
            _set_copyright(filepath, sql_copyright)
