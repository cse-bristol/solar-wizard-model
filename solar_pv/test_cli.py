# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import os
import shutil
import tempfile
import unittest
from os.path import join

from osgeo import ogr, osr

from solar_pv.cli import discover_lidar, read_buildings


def _write_buildings_gpkg(path: str, epsg: int) -> None:
    """A one-feature GeoPackage in the given CRS, with id and height fields."""
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(epsg)
    srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)

    driver = ogr.GetDriverByName("GPKG")
    ds = driver.CreateDataSource(path)
    layer = ds.CreateLayer("buildings", srs, ogr.wkbPolygon)
    layer.CreateField(ogr.FieldDefn("toid", ogr.OFTString))
    layer.CreateField(ogr.FieldDefn("h", ogr.OFTReal))

    feature = ogr.Feature(layer.GetLayerDefn())
    feature.SetField("toid", "abc123")
    feature.SetField("h", 12.5)
    # A small polygon in central Bristol (~lon -2.6, lat 51.45):
    feature.SetGeometry(ogr.CreateGeometryFromWkt(
        "POLYGON((-2.60 51.45, -2.60 51.4501, -2.5999 51.4501, -2.5999 51.45, -2.60 51.45))"))
    layer.CreateFeature(feature)
    ds = None  # flush


class ReadBuildingsTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.dir, ignore_errors=True)

    def test_reprojects_to_27700(self):
        gpkg = join(self.dir, "buildings.gpkg")
        _write_buildings_gpkg(gpkg, epsg=4326)

        buildings = list(read_buildings(gpkg, id_field="toid", height_field="h"))

        self.assertEqual(1, len(buildings))
        b = buildings[0]
        self.assertEqual("abc123", b.building_id)
        self.assertAlmostEqual(12.5, b.height, places=3)
        self.assertTrue(b.geom_27700.startswith("POLYGON"))

        # Central Bristol is roughly easting ~358000, northing ~173000 in BNG;
        # a plain assertion that the 4326 coords became BNG-scale numbers:
        geom = ogr.CreateGeometryFromWkt(b.geom_27700)
        xmin, xmax, ymin, ymax = geom.GetEnvelope()
        self.assertTrue(0 < xmin < 700000, f"easting out of BNG range: {xmin}")
        self.assertTrue(0 < ymin < 1300000, f"northing out of BNG range: {ymin}")
        self.assertTrue(350000 < xmin < 365000, f"unexpected easting: {xmin}")
        self.assertTrue(165000 < ymin < 180000, f"unexpected northing: {ymin}")

    def test_id_field_defaults_to_fid(self):
        gpkg = join(self.dir, "buildings.gpkg")
        _write_buildings_gpkg(gpkg, epsg=4326)

        buildings = list(read_buildings(gpkg))

        self.assertEqual(1, len(buildings))
        # GPKG FIDs start at 1:
        self.assertEqual("1", buildings[0].building_id)
        self.assertIsNone(buildings[0].height)

    def test_already_27700_passes_through(self):
        gpkg = join(self.dir, "buildings.gpkg")
        _write_buildings_gpkg(gpkg, epsg=27700)

        # The polygon WKT above is lon/lat numbers, but tagged as 27700 they are
        # taken as eastings/northings - they should stay the same:
        buildings = list(read_buildings(gpkg, id_field="toid"))
        geom = ogr.CreateGeometryFromWkt(buildings[0].geom_27700)
        xmin, _, ymin, _ = geom.GetEnvelope()
        self.assertAlmostEqual(-2.60, xmin, places=4)
        self.assertAlmostEqual(51.45, ymin, places=4)


class DiscoverLidarTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.dir, ignore_errors=True)

    def _touch(self, name: str) -> str:
        path = join(self.dir, name)
        open(path, "a").close()
        return path

    def test_directory_finds_rasters_only(self):
        self._touch("a.tif")
        self._touch("b.tiff")
        self._touch("c.vrt")
        self._touch("notes.txt")
        self._touch("d.zip")

        tiles = discover_lidar([self.dir])

        names = sorted(os.path.basename(t.filename) for t in tiles)
        self.assertEqual(["a.tif", "b.tiff", "c.vrt"], names)

    def test_explicit_files_and_dirs_combine(self):
        f1 = self._touch("one.tif")
        subdir = join(self.dir, "sub")
        os.makedirs(subdir)
        open(join(subdir, "two.tiff"), "a").close()

        tiles = discover_lidar([f1, subdir])

        names = sorted(os.path.basename(t.filename) for t in tiles)
        self.assertEqual(["one.tif", "two.tiff"], names)

    def test_explicit_file_taken_regardless_of_extension(self):
        # An explicitly-named path is trusted (e.g. a .vrt or an oddly-named tile),
        # unlike directory scanning which filters by extension:
        odd = self._touch("elevation.dat")
        tiles = discover_lidar([odd])
        self.assertEqual([odd], [t.filename for t in tiles])
