#!/bin/bash
# Run the Pig ETL across one or more countries in a single Pig job.
#
# Usage:
#   ./run_pig_etl.sh --countries NL

set -e

COUNTRIES=""
while [[ $# -gt 0 ]]; do
    case $1 in
        --countries) COUNTRIES="$2"; shift 2 ;;
        *) echo "Unknown argument: $1"; exit 1 ;;
    esac
done

if [[ -z "$COUNTRIES" ]]; then
    echo "Usage: $0 --countries NL,BE,FR"
    exit 1
fi

IFS=',' read -ra COUNTRY_ARR <<< "$COUNTRIES"
PATHS=""
for c in "${COUNTRY_ARR[@]}"; do
    FILE="/big_data/distributed_energy/raw/entsoe_weather_${c}.csv"
    if [[ -z "$PATHS" ]]; then
        PATHS="$FILE"
    else
        PATHS="$PATHS,$FILE"
    fi
done

echo "Countries: $COUNTRIES"
echo "Input paths: $PATHS"

pig -x mapreduce -param RAW_PATHS="$PATHS" \
    "$(dirname "$0")/../pig/clean_integrate.pig"
