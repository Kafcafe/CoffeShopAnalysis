DOCKER_SERVICES = """docker compose -f {compose} config --services"""
DOCKER_KILL = """docker compose -f {compose} kill -s SIGKILL {dead_man}"""
