from . import shell_cmd

DOCKER_COMPOSE_FILE = "docker-compose-dev.yaml"


def kill_target(target: str):
    command = f"sh ./scripts/boom.sh -f {DOCKER_COMPOSE_FILE} -t {target} --mode group"
    code = shell_cmd.silent(command)
    if code != 0:
        raise RuntimeError(f"Boom script failed with code {code}")
