import os
import pytest
from tests.utils import docker, parser, compare_results, config, gen_compose

@pytest.fixture(scope="module", autouse=True)
def setup():
    os.chdir(os.environ['REPO_PATH'])
    docker.stop_all()
    docker.prune()
    docker.up()

@pytest.fixture(autouse=True)
def beforeEach():
    config.set_server_config(limit=1)

def read_logs(container_name):
    logs = docker.logs(follow=True, target=container_name)
    for line in logs: 
        parsed = parser.parse_log_line(line)
        if 'results' in parsed and parsed['results'] == 'error': 
            pytest.fail("Client reported error")

        if 'action' in parsed and parsed['action'] == 'finish' and 'result' in parsed and parsed['result'] == 'success':
            return
    
    pytest.fail("Client did not finish successfully")

def build_results_path(clients_id):
    root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    paths = {}
    for i in range(4): 
        for client_id in clients_id:
            path = os.path.join(root_path, f"results/results_q{i+1}_{client_id}.txt")
            if client_id not in paths:
                paths[client_id] = []
            paths[client_id].append(path)
    return paths

def test_server_with_one_node_each():
    docker.down()
    gen_compose.gen_docker_compose()
    docker.up()
    docker.wait_for_clients(1)
    read_logs('client1')
    results_paths = build_results_path([1])
    assert compare_results.compare_all_results(results_paths)

def test_server_with_two_nodes_each_full():
    docker.down()
    gen_compose.gen_docker_compose(1, 2, 2, 2, 2, 2, 2, 2, 2, 2)
    docker.up()
    docker.wait_for_clients(1)
    read_logs('client1')
    results_paths = build_results_path([1])
    assert compare_results.compare_all_results(results_paths) 