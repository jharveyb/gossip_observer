#!/bin/bash

set -euo pipefail

HOST="$1"
DB="$2"
OUTFILE="$3"

if [ -z "$HOST" ] || [ -z "$DB" ] || [ -z "$OUTFILE" ]; then
        echo "Usage: $0 <host> <db> <outfile>"
        exit 1
fi

DATESUF=$(date +%Y%m%d)

pg_dump -h "$HOST" -p 5432 -U postgres -F c -d "$DB" -W --verbose --file db_dumps/"$OUTFILE""$DATESUF".dump
