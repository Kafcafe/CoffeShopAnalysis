DOCKER_SERVICES = """docker compose -f {compose} config --services"""
DOCKER_KILL = """docker compose -f {compose} stop {dead_man} --timeout 0"""
