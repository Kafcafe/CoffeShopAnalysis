import os
import pytest
from utils import docker


@pytest.fixture(scope="module", autouse=True)
def setup():
    os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    docker.stop_all()
    docker.prune()
    docker.up()

@pytest.fixture(autouse=True)
def beforeEach():
    docker.down(grace_period=10)

def test():
    docker.wait_for_clients(1)
    logs = docker.logs(follow=False)
    print("Logs:")
    for line in logs: 
        print(line, end='')

    print("\n")
    print("Finish reading logs")