#!/bin/bash

# Get the first parameter (client_id)
CLIENT_ID=$1

# Get the second parameter (full or nothing)
FULL_PARAM=$2

# Call the Python script with the appropriate parameters
if [ "$FULL_PARAM" = "full" ]; then
    python3 ./scripts/compare_results.py $CLIENT_ID "expected_results_full"
else
    python3 ./scripts/compare_results.py $CLIENT_ID
fi
