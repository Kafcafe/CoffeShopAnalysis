import subprocess

def _cmd_string_to_arr(cmd_string):
	return cmd_string.split(' ')

def stdout(cmd_string):
	cmd_arr = _cmd_string_to_arr(cmd_string)
	sp = subprocess.run(cmd_arr)
	
	return sp.returncode

def silent(cmd_string):
	cmd_arr = _cmd_string_to_arr(cmd_string)
	sp = subprocess.run(
		cmd_arr,
		stdout=subprocess.DEVNULL,
		stderr=subprocess.DEVNULL,
	)
	return sp.returncode

def pipe(cmd_string):
	cmd_arr = _cmd_string_to_arr(cmd_string)
	sp = subprocess.Popen(
		cmd_arr,
		stdout=subprocess.PIPE,
		stderr=subprocess.STDOUT,
		encoding='utf-8',
	)
	return iter(sp.stdout.readline, '')


def background(cmd_string):
	return subprocess.Popen(cmd_string, shell=True, start_new_session=True)