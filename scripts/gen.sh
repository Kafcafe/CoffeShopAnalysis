#!/bin/bash

# Docker Compose file generator wrapper script
#
# This script provides a user-friendly interface to the Python Docker Compose
# generator. It calls the Python script with provided arguments and interprets
# the exit codes to provide clear feedback to the user.
#
# Usage: ./gen.sh <output_file> <num_clients> <num_filters_by_year> <num_filters_by_hour> <num_filters_by_amount> <num_group_by_year_month>
#                 <num_group_by_semester>
#
# Exit codes from Python script:
#   0 - Success
#   1 - Invalid arguments or ValueError
#   2 - Unexpected error

# Call the Python script with all provided arguments
python3 ./scripts/generate-compose.py $1 $2 $3 $4 $5 $6 $7 $8 $9

# Capture the exit code from the Python script
exit_code=$?

# Provide user-friendly messages based on exit code
if [ $exit_code -eq 0 ]; then
    echo " ✅ docker compose file generated successfully"
elif [ $exit_code -eq 1 ]; then
    echo " ❌ Error: Please provide valid arguments"
    echo "Usage: ./gen.sh <output_file> <num_clients> <num_filters_by_year> <num_filters_by_hour> <num_filters_by_amount> <num_group_by_year_month> <num_group_by_semester> <num_join_items> <num_join_stores>"
else
    echo " ❌ Unexpected error occurred with exit code $exit_code"
    echo "Usage: ./gen.sh <output_file> <num_clients> <num_filters_by_year> <num_filters_by_hour> <num_filters_by_amount> <num_group_by_year_month> <num_group_by_semester> <num_join_items> <num_join_stores>"
fi
