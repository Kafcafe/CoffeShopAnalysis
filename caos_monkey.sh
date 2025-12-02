#!/bin/bash

set -e

COMPOSE_FILE="docker-compose-dev.yaml"
NUM_CLIENTS=5
NUM_NODES_PER_TYPE=5
ATTACK_INTERVAL=15  # seconds
LOG_FILE="chaos_monkey.log"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log() {
    local level=$1
    shift
    local message="$@"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo -e "[$timestamp] ${level}: $message" | tee -a $LOG_FILE
}

log_info() {
    log "${BLUE}INFO${NC}" "$@"
}

log_warn() {
    log "${YELLOW}WARN${NC}" "$@"
}

log_error() {
    log "${RED}ERROR${NC}" "$@"
}

log_success() {
    log "${GREEN}SUCCESS${NC}" "$@"
}

cleanup() {
    log_info "🧹 Cleaning up..."
    make down > /dev/null 2>&1 || true
    log_info "Cleanup completed"
}

handle_signal() {
    log_warn "🚨 Signal received, initiating cleanup..."
    cleanup
    exit 130
}

trap handle_signal SIGINT SIGTERM

check_clients_finished() {
    local running_clients=$(docker ps --filter "name=client" --filter "status=running" -q | wc -l)
    if [ "$running_clients" -eq 0 ]; then
        return 0  
    else
        return 1
    fi
}

get_random_container_from_group() {
    local group_prefix=$1
    local containers=$(docker ps --filter "name=${group_prefix}" --filter "status=running" --format "{{.Names}}" | grep -E "${group_prefix}[0-9]+" || true)
    
    if [ -z "$containers" ]; then
        log_warn "No running containers found for group: $group_prefix"
        return 1
    fi
    
    local containers_array=($containers)
    local random_index=$((RANDOM % ${#containers_array[@]}))
    echo "${containers_array[$random_index]}"
}

attack_random_node() {
    local all_layer_types=(
        "filter-year" "filter-hour" "filter-amount"
        "group-yearmonth" "group-semester" "group-store" "group-topk"
        "join-items" "join-store" "join-store_q3" "join-users"
    )
    
    local layers_attacked=0
    local total_layers=${#all_layer_types[@]}
    
    log_info "🎯 Starting complete attack on all $total_layers layer types..."
    
    for layer_type in "${all_layer_types[@]}"; do
        local target_container=$(get_random_container_from_group "$layer_type")
        
        if [ $? -eq 0 ] && [ -n "$target_container" ]; then
            local category=""
            if [[ $layer_type == filter-* ]]; then
                category="FILTER"
            elif [[ $layer_type == group-* ]]; then
                category="GROUP"
            elif [[ $layer_type == join-* ]]; then
                category="JOIN"
            fi
            
            log_warn "💥 Attacking $category LAYER ($layer_type): $target_container"
            
            if ./scripts/boom.sh --mode target -t "$target_container" -f "$COMPOSE_FILE" 2>/dev/null; then
                log_success "Successfully attacked $category: $target_container"
                layers_attacked=$((layers_attacked + 1))
            else
                log_error "Failed to attack $category: $target_container"
            fi
        else
            log_warn "No suitable target found for layer type: $layer_type"
        fi
        
        if [ "$layer_type" != "${all_layer_types[-1]}" ]; then
            log_info "⏳ Waiting 15 seconds before attacking next layer..."
            sleep 15
        fi
    done
    
    log_info "📊 Attack round completed. Layers attacked: $layers_attacked/$total_layers"
}

chaos_loop() {
    log_info "🐒 Starting chaos monkey attacks..."
    log_info "Will attack ALL 11 layer types (3 filters + 4 groups + 4 joins) with 15-second intervals between each"
    log_info "Complete attack rounds will be spaced by $ATTACK_INTERVAL seconds"
    
    local attack_count=0
    
    while ! check_clients_finished; do
        attack_count=$((attack_count + 1))
        log_info "🎯 Attack round $attack_count - Starting complete layer attack sequence"
        
        attack_random_node
        
            log_info "😴 Waiting $ATTACK_INTERVAL seconds before next complete attack round..."
            sleep $ATTACK_INTERVAL
        fi
    done
    
    log_success "🎉 All clients have finished! Stopping chaos attacks."
    log_info "Total attack rounds executed: $attack_count"
}

verify_results() {
    log_info "🔍 Verifying results for all clients..."
    local verification_failed=false
    
    for client_id in $(seq 1 $NUM_CLIENTS); do
        log_info "Verifying results for client $client_id..."
        
        if python3 ./scripts/compare_results.py "$client_id" > "verification_client_${client_id}.log" 2>&1; then
            log_success "✅ Client $client_id results verified successfully"
            grep -E "✅|❌" "verification_client_${client_id}.log" | while read line; do
                if [[ $line == *"✅"* ]]; then
                    log_success "$line"
                else
                    log_error "$line"
                    verification_failed=true
                fi
            done
        else
            log_error "❌ Client $client_id verification failed"
            verification_failed=true
        fi
    done
    
    if [ "$verification_failed" = true ]; then
        log_error "🚨 Some verifications failed. Check individual verification logs."
        return 1
    else
        log_success "🎉 All client results verified successfully!"
        return 0
    fi
}

wait_for_services() {
    log_info "⏳ Waiting for all services to be built and running..."
    local max_attempts=120  # Increased for build time
    local attempt=1
    
    while [ $attempt -le $max_attempts ]; do
        local building_containers=$(docker ps -a --filter "status=created" --format "{{.Names}}" | wc -l)
        local exited_containers=$(docker ps -a --filter "status=exited" --format "{{.Names}}" | wc -l)
        local unhealthy_services=$(docker ps --filter "health=unhealthy" --format "{{.Names}}" | wc -l)
        local starting_services=$(docker ps --filter "health=starting" --format "{{.Names}}" | wc -l)
        local running_containers=$(docker ps --filter "status=running" --format "{{.Names}}" | wc -l)
        local total_expected=$(grep -E '^\s+[a-zA-Z].*:$' "$COMPOSE_FILE" | wc -l)  # Count services in compose file
        
        if [ "$exited_containers" -gt 0 ]; then
            local failed_containers=$(docker ps -a --filter "status=exited" --format "{{.Names}}")
            log_error "❌ Some containers failed to start: $failed_containers"
            return 1
        fi
        
        if [ "$building_containers" -eq 0 ] && [ "$unhealthy_services" -eq 0 ] && [ "$starting_services" -eq 0 ] && [ "$running_containers" -ge 10 ]; then
            log_success "✅ All services are built, running and healthy! ($running_containers containers running)"
            log_info "⏳ Waiting additional 30 seconds for internal service initialization..."
            sleep 30
            return 0
        fi
        
        log_info "Attempt $attempt/$max_attempts: Building/Starting services (building: $building_containers, running: $running_containers/$total_expected, unhealthy: $unhealthy_services, starting: $starting_services)"
        sleep 10  
        attempt=$((attempt + 1))
    done
    
    log_error "❌ Timeout waiting for services to be built and running"
    return 1
}

main() {
    log_info "🚀 Starting Chaos Monkey for Coffee Shop Analysis System"
    log_info "Configuration:"
    log_info "  - Compose file: $COMPOSE_FILE"
    log_info "  - Clients: $NUM_CLIENTS"
    log_info "  - Nodes per type: $NUM_NODES_PER_TYPE"
    log_info "  - Attack interval: ${ATTACK_INTERVAL}s"
    
    log_info "📝 Generating docker-compose file with $NUM_NODES_PER_TYPE nodes of each type..."
    if ! ./gen.sh "$COMPOSE_FILE" "$NUM_CLIENTS" \
                  "$NUM_NODES_PER_TYPE" "$NUM_NODES_PER_TYPE" "$NUM_NODES_PER_TYPE" \
                  "$NUM_NODES_PER_TYPE" "$NUM_NODES_PER_TYPE" \
                  "$NUM_NODES_PER_TYPE" "$NUM_NODES_PER_TYPE" "$NUM_NODES_PER_TYPE" \
                  "$NUM_NODES_PER_TYPE"; then
        log_error "❌ Failed to generate docker-compose file"
        exit 1
    fi
    
    log_success "✅ Docker-compose file generated: $COMPOSE_FILE"
    
    log_info "🐳 Starting all Docker services..."
    if ! make up; then
        log_error "❌ Failed to start services"
        exit 1
    fi
    
    if ! wait_for_services; then
        log_error "❌ Services failed to become healthy"
        cleanup
        exit 1
    fi
    
    chaos_loop
    
    log_info "⏳ Waiting additional 30 seconds for final processing..."
    sleep 30
    
    if verify_results; then
        log_success "🎉 Chaos test completed successfully! All results verified."
        exit_code=0
    else
        log_error "🚨 Chaos test completed but some verifications failed."
        exit_code=1
    fi
    
    cleanup
    
    log_info "🏁 Chaos monkey test completed!"
    exit $exit_code
}

if [ ! -f "./gen.sh" ]; then
    log_error "gen.sh not found in current directory"
    exit 1
fi

if [ ! -f "./scripts/boom.sh" ]; then
    log_error "boom.sh not found in scripts directory"
    exit 1
fi

if [ ! -f "./scripts/compare_results.py" ]; then
    log_error "compare_results.py not found in scripts directory"
    exit 1
fi

if ! command -v docker &> /dev/null; then
    log_error "Docker is not installed or not in PATH"
    exit 1
fi

if ! command -v make &> /dev/null; then
    log_error "Make is not installed or not in PATH"
    exit 1
fi

main "$@"
