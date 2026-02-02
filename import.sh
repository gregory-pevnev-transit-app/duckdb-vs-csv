#!/bin/bash

# FEED_CODE="WMATA_P" # 20MB
# FEED_CODE="VICMBAU" # 42MB
# FEED_CODE="CARRISMPT" # 58MB

FEED_CODES=( "WMATA_P" "VICMBAU" "CARRISMPT" )

for FEED_CODE in "${FEED_CODES[@]}"
do
  IMPORT_CSV_PATH="./workspace/$FEED_CODE/trips.txt"

  DUCKDB_FILE="./workspace/$FEED_CODE.duckdb"

  rm -f $DUCKDB_FILE

  IMPORT_SQL="SELECT * FROM read_csv('$IMPORT_CSV_PATH', all_varchar = true, header = true)"
  duckdb $DUCKDB_FILE -c "CREATE TABLE trips AS ($IMPORT_SQL)"

  du -sh $DUCKDB_FILE
done
