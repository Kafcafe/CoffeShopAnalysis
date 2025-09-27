import sys
import constants


def generate_compose(file_destination, client_nums):
    compose = ""
    compose += constants.NETWORK_TEMPLATE
    compose += constants.RABBITMQ_SERVICE_TEMPLATE
    compose += constants.CLIENT_HANDLER_TEMPLATE
    for i in range(client_nums):
        compose += constants.CLIENTS_TEMPLATE.format(id=i+1)
    with open(file_destination, 'w') as f:
        f.write(compose)
        

def main():
    try:
        if len(sys.argv) != 3:
            print("You should call this script as: ./generar-compose.py <output_file> <num_clients>")
            sys.exit(1)
        print(sys.argv)
        file_destination = sys.argv[1]
        client_nums = int(sys.argv[2])
        generate_compose(file_destination, client_nums)
        print(f"Compose file '{file_destination}' generated with {client_nums} clients.")
        sys.exit(0)
    except ValueError as err: 
        print("You should provide a valid integer for the number of clients.", err)
        sys.exit(1)
    except Exception as e:
        print("An unexpected error occurred:", e)
        sys.exit(2)



if __name__ == "__main__":
    main()