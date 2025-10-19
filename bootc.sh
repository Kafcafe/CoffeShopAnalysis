#!/bin/bash

client_id=$1
data_folder=$2

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
docker compose -f ./docker-compose.client$client_id.yaml build
docker compose -f ./docker-compose.client$client_id.yaml up
