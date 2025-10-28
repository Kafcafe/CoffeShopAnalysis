import subprocess as sp
from random import choice
import commands

# TO DO: make this configurable from cli args
DOCKER_COMPOSE_FILE = "docker-compose-dev.yaml"


class Boom:

    def __init__(self, target: dict[str, str] = {}) -> None:
        self.target = target

    def __get_containers(self) -> list[str]:
        result = sp.run(
            commands.DOCKER_SERVICES.format(compose=DOCKER_COMPOSE_FILE),
            shell=True,
            check=True,
            stdout=sp.PIPE,
        )
        return result.stdout.decode().strip().split("\n")

    def __is_a_posible_target(self, target: str) -> bool:
        ## For now, just avoid stopping this containers, in the future we wil handle this cases
        if "rabbitmq" in target:
            return False

        if "client" in target and "handler" not in target:
            return False

        if "client" in target and "handler" in target:
            return False

        return True

    def __choose_target(self) -> str:
        containers = self.__get_containers()
        candidates = [c for c in containers if self.__is_a_posible_target(c)]
        if not candidates:
            raise RuntimeError("No suitable containers found to stop")
        return choice(candidates)

    def run(self) -> None:
        dead_man = self.__choose_target()
        print(f"Boombing container: {dead_man}")
        sp.run(
            commands.DOCKER_KILL.format(compose=DOCKER_COMPOSE_FILE, dead_man=dead_man),
            shell=True,
            check=True,
        )
