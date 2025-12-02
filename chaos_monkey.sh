#!/bin/bash

# Chaos Monkey Script
# Usage: ./caos_monkey.sh <docker_compose_file> <number_of_rounds> [sleep_seconds]

if [ $# -lt 2 ] || [ $# -gt 3 ]; then
    echo "Usage: $0 <docker_compose_file> <number_of_rounds> [sleep_seconds]"
    exit 1
fi

COMPOSE_FILE="$1"
NUMBER_OF_ROUNDS="$2"
SLEEP_SECONDS="${3:-15}"  # Default to 15 if not provided

filters=("filter-amount" "filter-hour" "filter-year")
groups=("group-semester" "group-topk" "group-yearmonth")
joins=("join-items" "join-store" "join-store_q3" "join-users")

echo "Docker Compose File: $COMPOSE_FILE"
echo "Number of Rounds: $NUMBER_OF_ROUNDS"
echo "Sleep Seconds: $SLEEP_SECONDS"

# Validate that containers from docker compose are running
echo "Validating that containers are running..."

# Get services from docker compose file
services=$(docker compose -f "$COMPOSE_FILE" config --services 2>/dev/null)

if [ $? -ne 0 ]; then
    echo "❌ Error reading docker compose file: $COMPOSE_FILE"
    exit 1
fi

# Check if containers are running
not_running=()
for service in $services; do
    container_status=$(docker ps --filter "name=$service" --format "{{.Names}}" 2>/dev/null)
    if [ -z "$container_status" ]; then
        not_running+=("$service")
    fi
done

# Report validation results
if [ ${#not_running[@]} -eq 0 ]; then
    echo "✅ All containers are running"
else
    echo "❌ The following containers are not running:"
    for container in "${not_running[@]}"; do
        echo "  - $container"
    done
    exit 1
fi

# Loop through all lists for the specified number of rounds
echo "Starting chaos attacks..."

for round in $(seq 1 $NUMBER_OF_ROUNDS); do
    echo "=== Round $round/$NUMBER_OF_ROUNDS ==="
    
    echo "Attacking filters:"
    for filter_type in "${filters[@]}"; do
        echo "  - $filter_type"
        ./scripts/boom.sh --mode group -t "$filter_type"
        sleep $SLEEP_SECONDS
    done

    echo "Attacking groups:"
    for group_type in "${groups[@]}"; do
        echo "  - $group_type"
        ./scripts/boom.sh --mode group -t "$group_type"
        sleep $SLEEP_SECONDS
    done

    echo "Attacking joins:"
    for join_type in "${joins[@]}"; do
        echo "  - $join_type"
        ./scripts/boom.sh --mode group -t "$join_type"
        sleep $SLEEP_SECONDS
    done
    
    if [ $round -lt $NUMBER_OF_ROUNDS ]; then
        echo "Round $round completed. Preparing for next round..."
    else
        echo "All rounds completed!"
    fi
done

