import os
import subprocess
from os.path import join
from typing import List

from crop import crop_to_mask
from generate_slope_polygons import generate_aspect_polygons


def model_solar_pv(pg_uri: str, job_id: int, lidar_paths: List[str]):
    solar_dir = join(os.getenv("SOLAR_DIR"), f"job_{job_id}")
    os.makedirs(solar_dir, exist_ok=True)

    # Create vrt:
    vrt_file = join(solar_dir, 'tiles.vrt')
    # _run(f"gdalbuildvrt {vrt_file} {' '.join(lidar_paths)}")

    # create raster mask from mastermap buildings
    mask_file = _create_mask(job_id, solar_dir, pg_uri)
    # cropped_lidar = join(solar_dir, 'cropped_lidar.tif')
    # crop_to_mask(vrt_file, mask_file, cropped_lidar)

    # create aspect tif from lidar
    aspect_file = join(solar_dir, 'aspect.tif')
    # _run(f"gdaldem aspect {cropped_lidar} {aspect_file} -of GTiff -b 1")

    # slope_file = join(solar_dir, 'slope.tif')
    # _run(f"gdaldem slope {cropped_lidar} {slope_file} -of GTiff -b 1")

    # crop/bucket/mask aspect/slope tif, and polygonise
    generate_aspect_polygons(mask_file, aspect_file, pg_uri, job_id, solar_dir)

    # choose suitable polygons (not north-facing, area >= 10 for now) (postgis)
    # find horizons/slope/aspect for pixels in buildings
    # pv-gis


def _run(command: str):
    res = subprocess.run(command, capture_output=True, text=True, shell=True)
    print(res.stdout)
    print(res.stderr)
    if res.returncode != 0:
        raise ValueError(res.stderr)


def _create_mask(job_id: int, solar_dir: str, pg_uri: str) -> str:
    job_id = int(job_id)
    mask_sql = f"""
        SELECT ST_Transform(geom_4326, 27700) 
        FROM mastermap.building 
        WHERE ST_Intersects(geom_4326, ST_Transform((
            SELECT bounds FROM models.job_queue WHERE job_id={job_id} LIMIT 1
        ), 4326))
    """
    mask_file = join(solar_dir, 'mask.tif')
    _run(f"""
        gdal_rasterize 
        -sql '{mask_sql}' 
        -burn 1 -tr 1 1 
        -init 0 -ot Int16 
        -of GTiff -a_srs EPSG:27700 
        "PG:{pg_uri}" 
        {mask_file}
        """.replace("\n", " "))
    return mask_file


if __name__ == '__main__':
    model_solar_pv(os.getenv("PG_URI"), 24, [
        "/home/neil/git/320-albion-webapp/lidar/2017_st7060_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7061_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7062_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7063_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7064_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7065_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7066_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7067_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7068_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7069_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7160_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7161_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7162_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7163_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7164_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7165_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7166_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7167_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7168_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7169_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7260_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7261_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7262_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7263_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7264_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7265_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7266_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7267_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7360_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7361_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7362_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7363_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7364_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7365_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7366_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7460_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7461_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7462_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7463_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7464_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7465_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7466_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7467_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7468_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7469_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7560_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7561_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7562_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7563_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7564_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7565_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7566_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7567_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7568_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7569_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7660_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7661_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7662_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7663_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7665_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7666_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7667_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7668_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7669_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7760_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7761_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7762_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7763_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7764_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7765_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7766_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7767_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7768_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7769_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7860_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7861_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7862_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7863_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7864_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7865_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7866_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7867_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7868_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7869_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7960_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7961_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7962_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7963_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7964_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7965_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7966_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7967_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7968_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st7969_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st8060_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st8061_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st8062_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st8065_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st8066_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st8067_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st8068_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st8069_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st8160_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st8161_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st8162_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st8166_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st8167_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st8168_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st8169_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st8260_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st8261_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st8267_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st8268_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st8269_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st8360_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st8361_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st8368_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st8369_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st8460_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st8461_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st8462_DSM_1M.tiff",
        "/home/neil/git/320-albion-webapp/lidar/2017_st8463_DSM_1M.tiff",
    ])
