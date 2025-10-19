import sys
import constants
import os

SUCCESS_EXIT_CODE: int = 0
INVALID_ARGS_EXIT_CODE: int = 1
UNEXPECTED_ERROR_EXIT_CODE: int = 2


def main():
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

        # Determine script directory
        file_destination: str = f"./docker-compose.client{client_id}.yaml"

        compose: str = constants.NETWORK_TEMPLATE
        compose += "services:"
        compose += constants.CLIENT_STANDALONE_TEMPLATE.format(
            id=client_id, data_folder=data_folder
        )
        # Write the complete compose file to disk
        with open(file_destination, "w") as f:
            f.write(compose)

        print(
            f"Client docker-compose file '{file_destination}' generated with ID {client_id} with data sourcr '{data_folder}'"
        )

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
    try:
        main()
    except Exception as e:
        print(f"Error found: [{e.__class__.__name__}] {e}")
