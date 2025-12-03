from . import shell_cmd

DOCKER_COMPOSE_FILE = "docker-compose-dev.yaml"


def run():
    command = f"./chaos_monkey.sh {DOCKER_COMPOSE_FILE} 5 12"
    return shell_cmd.stdout(command)