#!/bin/bash

# Boot a single client for the coffee shop analysis system.
#
# This script generates a Docker Compose file for a standalone client using the
# generate-single-client-dockerfile.py script, then builds and starts the client
# container. It provides user-friendly error handling and feedback.
#
# Usage:
#   ./bootc.sh <client_id> <data_folder>
#
# Arguments:
#   client_id:   Unique identifier for the client (used in container naming)
#   data_folder: Path to the data folder to mount as /data in the container
#
# Dependencies:
#   - python3: To run the Python script
#   - docker: To build and run containers
#   - docker compose: To manage multi-container applications
#   - scripts/generate-single-client-dockerfile.py: Script to generate compose file
#
# Exit Codes:
#   0: Success
#   1: Invalid arguments (from Python script)
#   Other: Unexpected error (from Python script or Docker commands)

client_id=$1
data_folder=$2

# Generate the Docker Compose file using the Python script
python3 ./scripts/generate-single-client-dockerfile.py $client_id $data_folder

# Capture the exit code from the Python script
exit_code=$?

# Provide user-friendly messages based on exit code
if [ $exit_code -eq 0 ]; then
    echo " ✅ docker compose file generated successfully"
elif [ $exit_code -eq 1 ]; then
    echo " ❌ Error: Please provide valid arguments"
    echo "Usage: ./bootc.sh <client_id> <data_folder>"
    exit $exit_code
else
    echo " ❌ Unexpected error occurred with exit code $exit_code"
    echo "Usage: ./bootc.sh <client_id> <data_folder>"
    exit $exit_code
fi

echo "Booting client with ID $client_id"

# Build the Docker image for the client
docker compose -f ./docker-compose.client$client_id.yaml build

# Start the client container
docker compose -f ./docker-compose.client$client_id.yaml up
