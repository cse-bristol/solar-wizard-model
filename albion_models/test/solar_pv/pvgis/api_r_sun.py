"""
Dev code to create per month beam and diffused raster maps for range of
aspects and slopes using the PVGIS API - for comparison with r.sun outputs
"""

import numpy as np
import requests as requests
from matplotlib import pyplot as plt
from osgeo import gdal

GT = (-1.9985231083140385, 1.4477662187810962e-05, 0.0, 51.722081099708056, 0.0, -8.994297250343682e-06)
PR = 'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AXIS["Latitude",NORTH],AXIS["Longitude",EAST],AUTHORITY["EPSG","4326"]]'
def _write_image(driver, values, name):
    ds: gdal.Dataset = driver.Create(name, xsize=values.shape[1], ysize=values.shape[0], bands=1,
                                     eType=gdal.GDT_Float32)
    ds.SetGeoTransform(GT)
    ds.SetProjection(PR)
    ds_band: gdal.Band = ds.GetRasterBand(1)
    ds_band.WriteArray(values)
    ds_band.SetNoDataValue(np.nan)
    ds_band.FlushCache()


XSIZE = 8
YSIZE = 4
test_locns = [(-1.9985158694829446, 51.72207660255943), (-1.9985158694829446, 51.72206760826218),
              (-1.9985158694829446, 51.72205861396493), (-1.9985158694829446, 51.72204961966768),
              (-1.9985013918207568, 51.72207660255943), (-1.9985013918207568, 51.72206760826218),
              (-1.9985013918207568, 51.72205861396493), (-1.9985013918207568, 51.72204961966768),
              (-1.9984869141585688, 51.72207660255943), (-1.9984869141585688, 51.72206760826218),
              (-1.9984869141585688, 51.72205861396493), (-1.9984869141585688, 51.72204961966768),
              (-1.998472436496381, 51.72207660255943), (-1.998472436496381, 51.72206760826218),
              (-1.998472436496381, 51.72205861396493), (-1.998472436496381, 51.72204961966768),
              (-1.9984579588341933, 51.72207660255943), (-1.9984579588341933, 51.72206760826218),
              (-1.9984579588341933, 51.72205861396493), (-1.9984579588341933, 51.72204961966768),
              (-1.9984434811720055, 51.72207660255943), (-1.9984434811720055, 51.72206760826218),
              (-1.9984434811720055, 51.72205861396493), (-1.9984434811720055, 51.72204961966768),
              (-1.9984290035098178, 51.72207660255943), (-1.9984290035098178, 51.72206760826218),
              (-1.9984290035098178, 51.72205861396493), (-1.9984290035098178, 51.72204961966768),
              (-1.9984145258476298, 51.72207660255943), (-1.9984145258476298, 51.72206760826218),
              (-1.9984145258476298, 51.72205861396493), (-1.9984145258476298, 51.72204961966768)]
userhorizon = "0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0"

# Create empty arrays for o/p images
api_result = []
for mon_ix in range(12):
    api_ = []
    for val_ix in range(2):
        api_.append(np.zeros((YSIZE, XSIZE)))
    api_result.append(api_)

# Iterate over range of aspects and slopes
ix = 0
for aspect_ix in range(XSIZE):
    aspect_grass = 45 * aspect_ix
    if aspect_grass == 0:
        aspect_grass = 360
    aspect_api = ((450 - aspect_grass) % 360) - 180

    for slope_ix in range(YSIZE):
        slope = 30 * slope_ix

        print(f"Aspect = {aspect_api} Slope = {slope}")

        # Query API - all months
        lat_north = test_locns[ix][1]
        lon_east = test_locns[ix][0]
        query_get = \
            f"https://re.jrc.ec.europa.eu/api/v5_2/DRcalc?lat={lat_north}&lon={lon_east}&userhorizon={userhorizon}" \
            f"&angle={slope}&aspect={aspect_api}&global=1&month=0" \
            f"&outputformat=json"
        query_get += "&raddatabase=PVGIS-SARAH2"

        print(query_get)
        response = requests.get(query_get)
        j_response = response.json()
        print(j_response)

        # Get totals for day returned in each month & store in o/p arrays
        for mon_ix in range(12):
            g_i = 0.0
            gb_i = 0.0
            gd_i = 0.0
            for hour_ix in range(24):
                out_ix = 24 * mon_ix + hour_ix
                gb_i += float(j_response["outputs"]["daily_profile"][out_ix]["Gb(i)"])
                gd_i += float(j_response["outputs"]["daily_profile"][out_ix]["Gd(i)"])
            api_result[mon_ix][0][slope_ix][aspect_ix] = gb_i
            api_result[mon_ix][1][slope_ix][aspect_ix] = gd_i

        ix += 1

# Write image files for use in GRASS
driver = gdal.GetDriverByName("GTiff")
driver.Register()
for mon_ix in range(12):
    for val_ix in range(2):
        values = api_result[mon_ix][val_ix]
        name = f"beam_{mon_ix}.tif" if val_ix == 0 else f"diffuse_{mon_ix}.tif"
        _write_image(driver, values, name)

# Display images using matplotlib
for mon_ix in range(12):
    fig, axs = plt.subplots(nrows=2)
    for val_ix in range(2):
        tpe = f" ({mon_ix})(beam)" if val_ix == 0 else f" ({mon_ix})(diffuse)"
        axs[val_ix].set_title(f'API {tpe}')
        axs[val_ix].imshow(api_result[mon_ix][val_ix])
plt.show()



