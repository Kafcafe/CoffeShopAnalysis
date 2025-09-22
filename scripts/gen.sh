#!/bin/bash
python3 ./scripts/generate-compose.py $1 $2

exit_code=$?

if [ $exit_code -eq 0 ]; then
    echo "✅ docker compose file generated successfully"
elif [ $exit_code -eq 1 ]; then
    echo "❌ Error: Please provide valid arguments."
else
    echo "❌ Unexpected error occurred with exit code $exit_code"
fi