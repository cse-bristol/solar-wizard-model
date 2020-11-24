from distutils.core import setup

setup(name='albion_solar_pv',
      version='0.1',
      packages=['solar_pv', 'solar_pv.pv_gis', 'solar_pv.saga_gis'],
      scripts=['bin/solar_pv.py'])
