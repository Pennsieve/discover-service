#!/usr/bin/env bash

if [ "$#" -ne 1 ]; then
    >&2 echo "usage: generate-migration-file.sh [filename]"
    exit 1
fi

DESTINATION='server/src/main/resources/db/migration'
FILENAME=$1

DATE=$(date "+%Y%m%d%H%M%S")

FULL_FILENAME="$DESTINATION/V${DATE}__$FILENAME.sql"

touch $FULL_FILENAME
