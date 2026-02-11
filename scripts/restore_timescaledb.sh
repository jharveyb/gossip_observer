#!/bin/bash

set -euo pipefail

# https://www.tigerdata.com/docs/self-hosted/latest/backup-and-restore/logical-backup

# TODO: use '-c' flag to script these
# psql -h "$HOST" -p 5432 -U postgres -W;
# CREATE DATABASE "$DB_NAME";
# \c "$DB";

# Package versions need to match the exporting machine
# CREATE EXTENSION IF NOT EXISTS timescaledb;
# CREATE EXTENSION IF NOT EXISTS timescaledb_toolkit;
# SELECT timescaledb_pre_restore();

# pg_restore -F c -d "$DB_NAME" -W --verbose "$INFILE"

# Reuse psql session from before
# SELECT timescaledb_post_restore();

