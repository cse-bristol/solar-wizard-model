import os
import subprocess
import unittest
from os.path import join

from albion_models.solar_pv.pvgis.pvmaps import PVMaps, CSI, ELEVATION_OVERRIDE, ELEVATION, ELEVATION_PATCHED

RESOURCES_DIR: str = os.path.realpath("../../../../resources")
TEST_DATA_DIR: str = os.path.realpath("../../../../testdata/pvmaps")
GRASS_DBASE: str = f"{TEST_DATA_DIR}/grass_dbase"


class PVMapsFunctionTests(unittest.TestCase):
    """Test at lower level than other test_pvmaps_xxx modules
    """

    def test_create_patched_elevation(self):
        """
        Test using a test elevation override of 3 buildings where 2 buildings are higher than the lidar elevation and
        one is lower.
        """

        BUILDING_HEIGHTS = (
            (-1.7106636985081045, 55.401643887859656, 108.4, 111.9),
            (-1.7107027371707948, 55.40152744935752, 100.0, 100.0),   # This one is lower than the orig elevation
            (-1.7105647862297484, 55.40177153009174, 110.6, 113.5)
        )

        INPUT_DIR: str = f"{TEST_DATA_DIR}/test_pvmaps_functions/_create_patched_elevation/in"
        OUTPUT_DIR: str = f"{TEST_DATA_DIR}/test_pvmaps_functions/_create_patched_elevation/out"
        instance: PVMaps = PVMaps(
            grass_dbase_dir=os.path.realpath(GRASS_DBASE),
            input_dir=INPUT_DIR,
            output_dir=OUTPUT_DIR,
            pvgis_data_tar_file=os.path.realpath(f"{TEST_DATA_DIR}/pvgis_data_tar/pvgis_data.tar"),
            pv_model_coeff_file_dir=RESOURCES_DIR,
            keep_temp_mapset=True,
            num_processes=os.cpu_count(),
            output_direct_diffuse=False,
            horizon_step_degrees=45,
            horizon_search_distance=1000,
            flat_roof_degrees=10.0,
            flat_roof_degrees_threshold=10.0,
            panel_type=CSI,
            num_pv_calcs_per_year=None
        )

        instance._create_temp_mapset()
        instance._import_raster("elevation_4326_part.tif", ELEVATION)
        instance._import_raster("elevation_override.tif", ELEVATION_OVERRIDE)
        elevation_fname = join(INPUT_DIR, "elevation_4326_part.tif")
        override_raster_fname = join(INPUT_DIR, "elevation_override.tif")

        instance._set_region_to_and_zoom(ELEVATION)

        # Run fn being tested
        instance.elevation = instance._create_patched_elevation()

        patched_raster_fname = join(OUTPUT_DIR, "elevation_patched.tif")
        instance._export_raster(ELEVATION_PATCHED, patched_raster_fname)

        for (x, y, abs_h2, abs_max) in BUILDING_HEIGHTS:
            original_elevation = self._gdal_get_value(elevation_fname, x, y)
            override_elevation = self._gdal_get_value(override_raster_fname, x, y)
            patched_elevation = self._gdal_get_value(patched_raster_fname, x, y)

            exp_height = (abs_h2 + abs_max) / 2.0
            if override_elevation < original_elevation:
                print(f"{x}, {y} - Using original elevation")
                exp_height = original_elevation
            else:
                print(f"{x}, {y} - Using building elevation")

            self.assertAlmostEqual(exp_height, patched_elevation, 3, f"exp: {exp_height}, act: {patched_elevation}")

    def _gdal_get_value(self, raster_filename, x, y):
        res = subprocess.run(f"""
            gdallocationinfo
            -valonly
            -geoloc
            {raster_filename} {x} {y}
            """.replace("\n", " "), capture_output=True, text=True, shell=True)
        self.assertIs(len(res.stderr), 0, f"Error running gdallocationinfo {res.stderr}")
        height = float(res.stdout)
        return height
