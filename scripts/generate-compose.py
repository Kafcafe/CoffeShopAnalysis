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

FILTER_BY_YEAR_TYPE: str = "year"
FILTER_BY_HOUR_TYPE: str = "hour"
FILTER_BY_AMOUNT_TYPE: str = "amount"

GROUP_BY_YEAR_MONTH: str = "yearmonth"
GROUP_BY_SEMESTER: str = "semester"

JOIN_ITEMS_TYPE: str = "items"

def generate_compose(file_destination: str,
                     client_nums: int,
                     filter_by_year_nums: int,
                     filter_by_hour_nums: int,
                     filter_by_amount_nums: int,
                     group_by_year_month_nums: int,
                     group_by_semester_nums: int,
                     join_items_nums: int):
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


    for i in range(filter_by_year_nums):
        filter_type = FILTER_BY_YEAR_TYPE
        compose += constants.FILTER_TEMPLATE.format(id=f"-{filter_type}{i+1}", filter_type=filter_type, filter_count=filter_by_year_nums)

    for i in range(filter_by_hour_nums):
        filter_type = FILTER_BY_HOUR_TYPE
        compose += constants.FILTER_TEMPLATE.format(id=f"-{filter_type}{i+1}", filter_type=filter_type, filter_count=filter_by_hour_nums)

    for i in range(filter_by_amount_nums):
        filter_type = FILTER_BY_AMOUNT_TYPE
        compose += constants.FILTER_TEMPLATE.format(id=f"-{filter_type}{i+1}", filter_type=filter_type, filter_count=filter_by_amount_nums)


    for i in range(group_by_year_month_nums):
        group_type = GROUP_BY_YEAR_MONTH
        compose += constants.GROUP_TEMPLATE.format(id=f"-{group_type}{i+1}", group_type=group_type, group_count=group_by_year_month_nums)

    for i in range(group_by_semester_nums):
        group_type = GROUP_BY_SEMESTER
        compose += constants.GROUP_TEMPLATE.format(id=f"-{group_type}{i+1}", group_type=group_type, group_count=group_by_semester_nums)

    for i in range(join_items_nums):
        compose += constants.JOIN_TEMPLATE.format(id=f"-{JOIN_ITEMS_TYPE}{i+1}", join_type=JOIN_ITEMS_TYPE, join_count=join_items_nums)

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
        if len(sys.argv) != 9:
            print("Usage: ./generar-compose.py <output_file> <num_clients> <num_filters_by_year> <num_filters_by_hour> <num_filters_by_amount> <num_group_by_year_month> <num_group_by_semester> <num_join_items>")
            sys.exit(1)

        # Debug: show received arguments
        print(sys.argv)

        # Parse arguments
        file_destination: str = sys.argv[1]
        client_nums: int = int(sys.argv[2])
        filter_by_year_nums: int = int(sys.argv[3])
        filter_by_hour_nums: int = int(sys.argv[4])
        filter_by_amount_nums: int = int(sys.argv[5])
        group_by_year_month_nums: int = int(sys.argv[6])
        group_by_semester_nums: int = int(sys.argv[7])
        join_items_nums: int = int(sys.argv[8])

        # Generate the compose file
        generate_compose(file_destination,
                         client_nums,
                         filter_by_year_nums,
                         filter_by_hour_nums,
                         filter_by_amount_nums,
                         group_by_year_month_nums,
                         group_by_semester_nums,
                         join_items_nums)
        
        print(f"""
 Compose file '{file_destination}' generated with:
 - Clients: {client_nums}
 - Filters by Year: {filter_by_year_nums}
 - Filters by Hour: {filter_by_hour_nums}
 - Filters by Amount: {filter_by_amount_nums}
 - Group by Year: {group_by_year_month_nums}
 - Group by Semester: {group_by_semester_nums}    
 - Join Items: {join_items_nums}
        """)

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