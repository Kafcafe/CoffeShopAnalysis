import os
import pytest
from utils import docker


@pytest.fixture(scope="module", autouse=True)
def setup():
    docker.stop_all()
    docker.prune()
    docker.build()

@pytest.fixture(autouse=True)
def beforeEach():
    docker.down(t=10)

def test():
    logs = docker.logs(follow=False)

    for line in logs: 
        print(line, end='')

