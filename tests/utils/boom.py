from . import shell_cmd

DOCKER_COMPOSE_FILE = "docker-compose-dev.yaml"


def run():
    command = f"sh ./chaos_monkey.sh {DOCKER_COMPOSE_FILE} 5 >> logs.txt"
    shell_cmd.background(command)