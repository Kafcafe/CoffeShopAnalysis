from . import shell_cmd
import os
import signal
import subprocess

DOCKER_COMPOSE_FILE = "docker-compose-dev.yaml"


def run(stop_event=None):
    """
    Run chaos monkey with optional stop event control
    """
    if stop_event is None:
        command = f"./chaos_monkey.sh {DOCKER_COMPOSE_FILE} 5 12"
        return shell_cmd.stdout(command)
    
    command = f"./chaos_monkey.sh {DOCKER_COMPOSE_FILE} 5 12"
    print(f"[CHAOS MONKEY] Starting: {command}")
    
    process = subprocess.Popen(
        command.split(),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,  
        universal_newlines=True,
        preexec_fn=os.setsid
    )
    
    try:
        while process.poll() is None:
            if stop_event and stop_event.is_set():
                print("[CHAOS MONKEY] Stop signal received, terminating...")
                os.killpg(os.getpgid(process.pid), signal.SIGTERM)
                try:
                    process.wait(timeout=5)
                    print("[CHAOS MONKEY] Terminated gracefully")
                except subprocess.TimeoutExpired:
                    print("[CHAOS MONKEY] Force killing...")
                    os.killpg(os.getpgid(process.pid), signal.SIGKILL)
                break
            
            # Read and display output in real time
            line = process.stdout.readline()
            if line:
                print(f"[CHAOS MONKEY] {line.rstrip()}")
            else:
                # Check every 0.1 seconds for more responsive output
                import time
                time.sleep(0.1)
        
        # Read any remaining output
        remaining_output = process.stdout.read()
        if remaining_output:
            for line in remaining_output.strip().split('\n'):
                if line.strip():
                    print(f"[CHAOS MONKEY] {line.rstrip()}")
        
        print(f"[CHAOS MONKEY] Process completed with exit code: {process.returncode}")
        return process.returncode
        
    except Exception as _:
        try:
            os.killpg(os.getpgid(process.pid), signal.SIGKILL)
        except Exception:
            pass
        return -1