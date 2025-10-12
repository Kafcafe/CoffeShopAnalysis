import os
import pytest
from utils import docker, parser, compare_results

@pytest.fixture(scope="module", autouse=True)
def setup():
    os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    docker.stop_all()
    docker.prune()
    docker.up()

@pytest.fixture(autouse=True)
def beforeEach():
    pass

def read_logs(container_name):
    logs = docker.logs(follow=False, target=container_name)
    for line in logs: 
        parsed = parser.parse_log_line(line)
        
        if 'results' in parsed and parsed['results'] == 'error': 
            pytest.fail("Client reported error")


        if 'action' in parsed and parsed['action'] == 'finish' and 'result' in parsed and parsed['result'] == 'success':
            return
    
    pytest.fail("Client did not finish successfully")

def build_results_path(clients_id):
    root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    paths = []
    for i in range(4): 
        for client_id in clients_id:
            paths.append(os.path.join(root_path, f"results/results_q{i+1}_{client_id}.txt"))
    return paths

def test():
    docker.wait_for_clients(1)
    read_logs('client1')
    results_paths = build_results_path([1])
    assert compare_results.compare_all_results(*results_paths)
    