import pytest


@pytest.fixture
def available_port(free_tcp_port):
    return free_tcp_port
