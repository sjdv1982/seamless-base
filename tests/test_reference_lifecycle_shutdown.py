from __future__ import annotations

import subprocess
import sys


def _run(script: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-c", script],
        check=False,
        capture_output=True,
        text=True,
    )


def test_balanced_explicit_close_is_quiet_on_reference_logger():
    result = _run(
        """
import logging
from seamless import Cell, Checksum, close
logging.basicConfig(level=logging.WARNING)
cell = Cell(Checksum('00' * 32))
close()
"""
    )
    assert result.returncode == 0, result.stderr
    assert "seamless.references" not in result.stderr


def test_explicit_close_reports_manual_leak_once_and_is_idempotent():
    result = _run(
        """
import logging
from seamless import Checksum, close
logging.basicConfig(level=logging.WARNING)
checksum = Checksum('11' * 32)
checksum.incref()
close()
close()
"""
    )
    assert result.returncode == 0, result.stderr
    assert result.stderr.count("unmatched manual references") == 1
