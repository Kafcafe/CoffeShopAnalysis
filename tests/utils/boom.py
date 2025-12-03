import os
from . import shell_cmd

DOCKER_COMPOSE_FILE = "docker-compose-dev.yaml"


def run():
    command = f"sh ./chaos_monkey.sh {DOCKER_COMPOSE_FILE} 5 12 >> logs.txt"
    return shell_cmd.background(command)

def stop(pid):
    try: 
        os.kill(pid, 9)
    except Exception as _:
        print("Process could not be stopped, already finished")