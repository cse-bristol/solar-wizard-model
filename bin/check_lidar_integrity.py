import os
import subprocess
from os.path import join
from typing import List


def check(dirname: str) -> List[str]:
    """
    Script for checking integrity of LIDAR files in a directory.

    Attempts a no-op transformation of each file to an in-memory file.

    See https://lists.osgeo.org/pipermail/gdal-dev/2013-November/037520.html
    """
    errs = []
    for f in os.listdir(dirname):
        res = subprocess.run(
            f"gdal_translate {join(dirname, f)} /vsimem/tmp.tif",
            capture_output=True, text=True, shell=True)
        if res.returncode != 0:
            errs.append(f)
            print(f)

    f"Found {len(errs)} corrupt files: {', '.join(errs)}"
    return errs


# if __name__ == '__main__':
#     check("/home/neil/data/albion-data/denmark/lidar/aalborg/lidar-dsm")
