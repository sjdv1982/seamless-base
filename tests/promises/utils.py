import contextlib
import socket
import subprocess
import time
import shutil


@contextlib.contextmanager
def start_server(command, *, startup_timeout=10):
    if not shutil.which(command[0]):
        import pytest

        pytest.skip(f"Command not available: {command[0]}")
    proc = subprocess.Popen(
        command,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        yield proc
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=startup_timeout)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=2)


def wait_for_server(port, host="127.0.0.1", timeout=10, interval=0.1):
    deadline = time.time() + timeout
    while time.time() < deadline:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(interval)
            if sock.connect_ex((host, port)) == 0:
                return
        time.sleep(interval)
    raise TimeoutError(f"Server did not start on {host}:{port} within {timeout}s")
