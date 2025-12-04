

import os
import threading
import pytest
from tests.utils import boom, docker, parser, compare_results, config, gen_compose
import time

@pytest.fixture(scope="module", autouse=True)
def setup():
    os.chdir(os.environ['REPO_PATH'])
    docker.stop_all()
    docker.prune()
    docker.up()

@pytest.fixture(autouse=True)
def beforeEach():
    config.set_server_config(limit=5)

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


def test_three():
    docker.down()
    gen_compose.gen_docker_compose(5, 5, 5, 5, 5, 5, 5, 5, 5, 5)
    docker.up()
    time.sleep(20)   
    
    stop_event = threading.Event()
    node_killer = threading.Thread(target=boom.run, args=(stop_event,))
    node_killer.daemon = True
    node_killer.start()
    
    try:
        docker.wait_for_clients(5)
        read_logs('client1')
        read_logs('client2')
        read_logs('client3')
        read_logs('client4')
        read_logs('client5')
        results_paths = build_results_path([1, 2, 3, 4, 5])
        
        stop_event.set()
        node_killer.join(timeout=10)  
        
        if node_killer.is_alive():
            try:
                os.system("pkill -f boom.sh")
                os.system("pkill -f chaos_monkey.sh")
            except Exception:
                pass
        
        print()
        assert compare_results.compare_all_results(results_paths)
        
    finally:
        stop_event.set()
        if node_killer.is_alive():
            try:
                os.system("pkill -f boom.sh")
                os.system("pkill -f chaos_monkey.sh")
            except Exception:
                pass