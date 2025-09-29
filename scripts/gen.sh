#!/bin/bash

# Docker Compose file generator wrapper script
#
# This script provides a user-friendly interface to the Python Docker Compose
# generator. It calls the Python script with provided arguments and interprets
# the exit codes to provide clear feedback to the user.
#
# Usage: ./gen.sh <output_file> <num_clients> <num_filters>
#
# Exit codes from Python script:
#   0 - Success
#   1 - Invalid arguments or ValueError
#   2 - Unexpected error

# Call the Python script with all provided arguments
python3 ./scripts/generate-compose.py $1 $2 $3

# Capture the exit code from the Python script
exit_code=$?

# Provide user-friendly messages based on exit code
if [ $exit_code -eq 0 ]; then
    echo "✅ docker compose file generated successfully"
elif [ $exit_code -eq 1 ]; then
    echo "❌ Error: Please provide valid arguments"
    echo "Usage: ./gen.sh <output_file> <num_clients> <num_filters>"
else
    echo "❌ Unexpected error occurred with exit code $exit_code"
    echo "Usage: ./gen.sh <output_file> <num_clients> <num_filters>"
fi
