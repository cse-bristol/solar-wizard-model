from os.path import join

from albion_models import paths

# PROJ4 string for using OSTN15 to convert from easting/northing to lat/long.
#
# When this is used as the source SRS for an EPSG:27700 (easting/northing) GIS dataset,
# the transformation accuracy into EPSG:4326 (long/lat) is on the order of mm, rather than
# the default GDAL one which can be out by up to 20m.
OSTN15_TO_4326 = f'+proj=tmerc +lat_0=49 +lon_0=-2 +k=0.9996012717 ' \
                 f'+x_0=400000 +y_0=-100000 ' \
                 f'+ellps=airy +units=m +no_defs ' \
                 f'+nadgrids={join(paths.RESOURCES_DIR, "OSTN15_NTv2_OSGBtoETRS.gsb")}'

# PROJ4 string for using OSTN15 to convert from lat/long to easting/northing.
OSTN15_TO_27700 = f'+proj=tmerc +lat_0=49 +lon_0=-2 +k=0.9996012717 ' \
                  f'+x_0=400000 +y_0=-100000 ' \
                  f'+ellps=airy +units=m +no_defs ' \
                  f'+nadgrids={join(paths.RESOURCES_DIR, "OSTN15_NTv2_ETRStoOSGB.gsb")}'

OSTN02_PROJ4 = f'+proj=tmerc +lat_0=49 +lon_0=-2 +k=0.999601 ' \
               f'+x_0=400000 +y_0=-100000 ' \
               f'+ellps=airy +units=m + no_defs ' \
               f'+nadgrids={join(paths.RESOURCES_DIR, "OSTN02_NTv2.gsb")}'

_7_PARAM_SHIFT = "+proj=tmerc +lat_0=49 +lon_0=-2 " \
                 "+k=0.999601 +x_0=400000 +y_0=-100000 +ellps=airy +units=m +no_defs " \
                 "+towgs84=446.448,-125.157,542.060,0.1502,0.2470,0.8421,-20.4894"
