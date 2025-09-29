"""
Docker Compose file generator for distributed systems project.

This script generates Docker Compose YAML files with a configurable number of client
services for a distributed coffee shop analysis system. It creates a complete
infrastructure including RabbitMQ message broker, client handler, and multiple clients.

Usage:
    python3 generate-compose.py <output_file> <num_clients>

Example:
    python3 generate-compose.py docker-compose.yaml 5
"""

import sys
import constants


def generate_compose(file_destination: str, client_nums: int, filter_nums: int):
    """
    Generate a Docker Compose file with the specified number of clients.

    Args:
        file_destination (str): Path where the generated compose file will be saved
        client_nums (int): Number of client services to create

    The generated compose file includes:
    - Network configuration for service communication
    - RabbitMQ message broker service
    - Client handler service
    - Multiple client services based on client_nums parameter
    """
    compose: str = ""

    # Build the compose file by concatenating templates
    compose += constants.NETWORK_TEMPLATE      # Network configuration
    compose += constants.RABBITMQ_SERVICE_TEMPLATE  # Message broker service
    compose += constants.CLIENT_HANDLER_TEMPLATE    # Client handler service

    # Add client services based on the specified number
    for i in range(client_nums):
        # Format the client template with unique ID (starting from 1)
        compose += constants.CLIENTS_TEMPLATE.format(id=i+1)

    for i in range(filter_nums):
        compose += constants.FILTER_TEMPLATE.format(id=i+1)

    # Write the complete compose file to disk
    with open(file_destination, 'w') as f:
        f.write(compose)


SUCCESS_EXIT_CODE: int = 0
INVALID_ARGS_EXIT_CODE: int = 1
UNEXPECTED_ERROR_EXIT_CODE: int = 2

def main():
    """
    Main entry point for the Docker Compose file generator.

    Parses command line arguments, validates input, and orchestrates the
    compose file generation process with comprehensive error handling.

    Exit codes:
        0: Success
        1: Invalid arguments or ValueError
        2: Unexpected error
    """
    try:
        # Validate command line arguments
        if len(sys.argv) != 4:
            print("Usage: ./generar-compose.py <output_file> <num_clients>")
            sys.exit(1)

        # Debug: show received arguments
        print(sys.argv)

        # Parse arguments
        file_destination: str = sys.argv[1]
        client_nums: int = int(sys.argv[2])
        filter_bums: int = int(sys.argv[3])

        # Generate the compose file
        generate_compose(file_destination, client_nums, filter_bums)
        print(f"Compose file '{file_destination}' generated with {client_nums} clients and {filter_bums} filters.")
        sys.exit(SUCCESS_EXIT_CODE)

    except ValueError as err:
        # Handle invalid number format
        print("You should provide a valid integer for the number of clients.", err)
        sys.exit(INVALID_ARGS_EXIT_CODE)
    except Exception as e:
        # Handle any other unexpected errors
        print("An unexpected error occurred:", e)
        sys.exit(UNEXPECTED_ERROR_EXIT_CODE)



if __name__ == "__main__":
    main()