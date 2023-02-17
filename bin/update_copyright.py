#  This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
#  Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.

import os
import textwrap
from glob import glob
from solar_model import paths
from datetime import datetime

if __name__ == '__main__':
    year = datetime.now().year
    py_copyright = textwrap.dedent(f"""
    # This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-{year}
    # Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
    """)
    for _dir, _, _ in os.walk(paths.SRC_DIR):
        for pp in glob(os.path.join(_dir, '*.py')):
            with open(pp, 'r+') as f:
                content = f.read()
                f.seek(0, 0)
                f.write(py_copyright + content)

    for _dir, _, _ in os.walk(paths.SQL_DIR):
        for pp in glob(os.path.join(_dir, '*.sql')):
            print(pp)
