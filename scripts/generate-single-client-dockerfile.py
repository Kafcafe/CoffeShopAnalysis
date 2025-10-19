"""
Generate Docker Compose file for a single client in the coffee shop analysis system.

This script creates a docker-compose YAML file for running a standalone client
service that processes coffee shop data. The generated file includes network
configuration and a single client service that mounts the specified data folder.

Usage:
    python generate-single-client-dockerfile.py <client_id> <data_folder>

Arguments:
    client_id: Unique identifier for the client (used in container naming)
    data_folder: Path to the data folder to mount as /data in the container

Output:
    Creates docker-compose.client{client_id}.yaml in the current directory

Exit Codes:
    0: Success
    1: Invalid arguments
    2: Unexpected error

Dependencies:
    - constants.py: Contains Docker Compose templates
"""

import sys
import constants

SUCCESS_EXIT_CODE: int = 0
INVALID_ARGS_EXIT_CODE: int = 1
UNEXPECTED_ERROR_EXIT_CODE: int = 2


def main():
    """
    Main function that generates a Docker Compose file for a single client.

    Validates command line arguments, constructs the Docker Compose YAML content
    using predefined templates, and writes the file to disk.

    Raises:
        SystemExit: With appropriate exit code on validation failure or errors
    """
    try:
        # Validate command line arguments
        if len(sys.argv) != 3:
            print(
                "Usage: ./generate-single-client-dockerfile.py <client_id> <data_folder>"
            )
            print(f"\nYour input of length { len(sys.argv)}: {sys.argv}")
            sys.exit(INVALID_ARGS_EXIT_CODE)

        # Debug: show received arguments
        print(sys.argv)

        # Parse arguments
        client_id: str = sys.argv[1]
        data_folder: str = sys.argv[2]

        # Determine output file path
        file_destination: str = f"./docker-compose.client{client_id}.yaml"

        # Build Docker Compose content
        # Start with network configuration
        compose: str = constants.NETWORK_TEMPLATE
        # Add services section
        compose += "services:"
        # Add client service using standalone template
        compose += constants.CLIENT_STANDALONE_TEMPLATE.format(
            id=client_id, data_folder=data_folder
        )

        # Write the complete compose file to disk
        with open(file_destination, "w") as f:
            f.write(compose)

        print(
            f"Client docker-compose file '{file_destination}' generated with ID {client_id} with data source '{data_folder}'"
        )

        sys.exit(SUCCESS_EXIT_CODE)

    except ValueError as err:
        # Handle invalid number format (though client_id can be string)
        print("You should provide valid arguments.", err)
        sys.exit(INVALID_ARGS_EXIT_CODE)
    except Exception as e:
        # Handle any other unexpected errors
        print("An unexpected error occurred:", e)
        sys.exit(UNEXPECTED_ERROR_EXIT_CODE)


if __name__ == "__main__":
    """
    Script entry point.

    Calls the main function and handles any uncaught exceptions
    that might occur during execution.
    """
    try:
        main()
    except Exception as e:
        print(f"Error found: [{e.__class__.__name__}] {e}")
