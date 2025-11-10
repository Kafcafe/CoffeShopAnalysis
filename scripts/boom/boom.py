import subprocess as sp
from random import choice
import commands
import re
DOCKER_COMPOSE_FILE = "docker-compose-dev.yaml"


class Boom:

    def __init__(self, configs: dict[str, str] = {}) -> None:
        self.configs = configs

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
        is_a_valid_canditate = False
        candidate = None
        while not is_a_valid_canditate:
            choose_candidate = choice(candidates)
            filtered = [c for c in candidates if re.sub(r"\d", "", c) == re.sub(r"\d", "", choose_candidate)]
            if len(filtered) > 1:
                candidate = choose_candidate
                break
        return candidate

    def __choose_random(self):
        dead_man = self.__choose_target()
        self.__exec_command(commands.DOCKER_KILL, dead_man)

    def __with_target(self):
        target = self.configs.get("t", None)
        if target is None:
            raise ValueError("No target specified for 'with_target' method")
        self.__exec_command(commands.DOCKER_KILL, target)

    def __exec_command(self, command: str, target: str):
        file = self.configs.get("f", DOCKER_COMPOSE_FILE)
        if not file: 
            file = DOCKER_COMPOSE_FILE
        sp.run(
            command.format(compose=file, dead_man=target),
            shell=True,
            check=True,
        )

    def __for_group(self):
        if not self.configs.get("t", None):
            print("No group specified")
            return 
        containers = self.__get_containers()
        filtered = [c for c in containers if re.sub(r"\d", "", c) == self.configs.get("t")]
        if len(filtered) <= 1:
            print("Not enough containers in the group to choose from")
            return
        dead_man = choice(filtered)
        self.__exec_command(commands.DOCKER_KILL, dead_man)

    def run(self) -> None:
        cli = {
            "random": self.__choose_random,
            "target": self.__with_target,
            "group": self.__for_group,
        }
        mode = self.configs.get("mode", "random")
        cli[mode]()
