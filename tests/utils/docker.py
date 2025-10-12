from . import shell_cmd

class DockerException(Exception):
	def __init__(self, message):
		message = 'Docker: ' + message
		self.message = message
		super().__init__(self.message)

def stop(container, grace_period=5):
	shell_cmd.silent(f'docker stop {container} -t {grace_period}')

def stop_all():
	container_list = ''.join(list(shell_cmd.pipe('docker ps -q'))).replace('\n', ' ')
	if len(container_list) > 0:
		shell_cmd.silent('docker stop ' + container_list)

def prune():
	shell_cmd.silent('docker container prune -f')
	shell_cmd.silent('docker network prune -f')
	shell_cmd.silent('docker image prune -f')

def build():
	shell_cmd.silent('docker build -f ./server/Dockerfile -t server:latest .')
	shell_cmd.silent('docker build -f ./client/Dockerfile -t client:latest .')

def up():
	return_code = shell_cmd.silent('docker compose -f docker-compose-dev.yaml up -d')
	if (return_code != 0):
		raise DockerException('Can\'t start system')

def down(grace_period=0):
	shell_cmd.silent('docker compose -f docker-compose-dev.yaml stop' + (f' -t {grace_period}' if grace_period > 0 else ''))
	shell_cmd.silent('docker compose -f docker-compose-dev.yaml down')

def logs(follow=True, target=''):
	return shell_cmd.pipe('docker compose -f docker-compose-dev.yaml logs' + (f' {target}' if target else '') + (' -f' if follow else ''))

def wait_for_clients(n_clients):
	if n_clients <= 0:
		return
	shell_cmd.silent('docker wait ' + ' '.join([f'client{i}' for i in range(1, n_clients+1)]))