MODULE_TOPDIR = ../..

PGM = r.horizonmask

LIBES = $(GPROJLIB) $(RASTERLIB) $(GISLIB) $(MATHLIB) $(PROJLIB)
DEPENDENCIES = $(GPROJDEP) $(RASTERDEP) $(GISDEP)
EXTRA_INC = $(PROJINC) $(GDALCFLAGS)

include $(MODULE_TOPDIR)/include/Make/Module.make

default: cmd
