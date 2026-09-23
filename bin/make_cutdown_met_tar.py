# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
"""
Regenerate the cut-down pvgis_data_uk.tar committed in testsdata/e2e. 
It crops every met layer the model reads out of the full UK-wide tar
to a small window covering the e2e fixture extent, so the e2e test can run without
the ~640MB original.

    python3 bin/make_cutdown_met_tar.py /path/to/full/pvgis_data_uk.tar

The window is the fixture's lidar extent (see LIDAR below) buffered by
BUFFER_M; met cells are ~1.6km x 2.5km so a few km of buffer is plenty. Only the
layers MetData.for_month reads are kept (both panels' spectral correction, so
pv_tech may be crystSi or CdTe); the full tar's unused PVMAPS inputs (t_gradient*,
t_offset_f_era*, relstd_sG_merge*, *_oper) are dropped.
"""
import os
import sys
import tarfile
import tempfile
from os.path import dirname, join

from osgeo import gdal

gdal.UseExceptions()

OUT_TAR = join(dirname(__file__), "..", "testdata/e2e", "pvgis_data_uk.tar")
SUFFIX = ".27700.tif"

# Fixture lidar extent (EPSG:27700), buffered on every side.
LIDAR = (356580.0, 172580.0, 357520.0, 173520.0)  # minx, miny, maxx, maxy
BUFFER_M = 6000.0


def wanted_members():
    for mm in range(1, 13):
        m = f"{mm:02d}"
        for hh in range(0, 24, 3):
            yield f"t2m_avg_{m}_{hh:02d}{SUFFIX}"
        yield f"tl_0m_{m}{SUFFIX}"
        yield f"kcb_{m}{SUFFIX}"
        yield f"kcd_{m}{SUFFIX}"
        yield f"windeffect_{m}{SUFFIX}"
        yield f"spectraleffect_cSi_{m}{SUFFIX}"
        yield f"spectraleffect_CdTe_{m}{SUFFIX}"


def main(src_tar: str):
    minx, miny, maxx, maxy = LIDAR
    projwin = [minx - BUFFER_M, maxy + BUFFER_M, maxx + BUFFER_M, miny - BUFFER_M]

    keep = set(wanted_members())
    present = set(tarfile.open(src_tar).getnames())
    missing = keep - present
    if missing:
        raise SystemExit(f"expected members absent from {src_tar}: {sorted(missing)}")
    names = sorted(keep)
    print(f"keeping {len(names)} of {len(present)} members")

    with tempfile.TemporaryDirectory() as work:
        for name in names:
            gdal.Translate(join(work, name), f"/vsitar/{src_tar}/{name}",
                           projWin=projwin, projWinSRS="EPSG:27700",
                           creationOptions=["COMPRESS=DEFLATE", "PREDICTOR=2"])
        with tarfile.open(OUT_TAR, "w") as tar:  # uncompressed: the model reads via /vsitar/
            for name in names:
                tar.add(join(work, name), arcname=name)

    print(f"wrote {OUT_TAR}: {os.path.getsize(OUT_TAR) / 1024:.0f} KiB")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit(f"usage: {sys.argv[0]} /path/to/full/pvgis_data_uk.tar")
    main(sys.argv[1])
