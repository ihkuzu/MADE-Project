#!/bin/bash

set -e

DATA_DIR="./data"
DB_FILE="${DATA_DIR}/nyc_climate_traffic.db"
SCRIPT_FILE="pipeline.py"

python3 $SCRIPT_FILE

if [ -f "$DB_FILE" ]; then
    echo "Test passed: $DB_FILE exists."
else
    echo "Test failed: $DB_FILE does not exist."
    exit 1
fi
