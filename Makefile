#-------------------------------------------------------------------------
#
# Makefile for cms_mms
#
#-------------------------------------------------------------------------

MODULE_big = cms_mms
OBJS =		\
			cms_mms.o \
			MurmurHash3.o \
			$(NULL)

EXTENSION = cms_mms
DATA =		\
			cms_mms--1.0.0.sql \
			$(NULL)


REGRESS = create add add_agg union union_agg results copy

EXTRA_CLEAN += -r $(RPM_BUILD_ROOT)

PG_CPPFLAGS += -fPIC
cms_mms.o: override CFLAGS += -std=c99
MurmurHash3.o: override CFLAGS += -std=c99

ifdef DEBUG
COPT		+= -O0
CXXFLAGS	+= -g -O0
endif

SHLIB_LINK	+= -lstdc++

ifndef PG_CONFIG
PG_CONFIG = pg_config
endif

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
