MODULES = row_to_column
EXTENSION = pg_rowcol_sync
DATA = pg_rowcol_sync--1.0.sql

PG_CONFIG = ~/installs/pg-debug/bin/pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

