MODULES = row_to_column
EXTENSION = row_to_column
DATA = row_to_column--1.0.sql

PG_CONFIG = ~/installs/pg-debug/bin/pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
