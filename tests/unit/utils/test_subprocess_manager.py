"""
Unit tests for subprocess management methods in SubprocessManager class
"""
import sys
import tempfile
import time
from pathlib import Path

import pytest
from pytest import mark

from jade.exceptions import ExecutionError
from jade.utils.run_command import check_run_command, run_command
from jade.utils.subprocess_manager import SubprocessManager


@mark.parametrize(
    "command, timeout", [("echo 'Hello'", None), ("ls --invalidoption", None), ("sleep 2", 0.1)]
)
def test_subprocess_manager__run(command, timeout):
    """Should run command one at a time with"""
    mgr = SubprocessManager()
    mgr.run(command)
    ret = mgr.wait_for_completion()

    if command == "echo 'Hello'":
        assert ret == 0
        assert mgr.return_code == 0

    if command == "ls --invalidoption":
        assert ret != 0
        assert mgr.return_code != 0

    if command == "sleep 2":
        assert ret == 0
        assert mgr.return_code == 0


def test_subprocess_manager__run__no_wait():
    """Should run command without blocking"""
    mgr = SubprocessManager()
    command = "sleep 2"
    mgr.run(command)
    assert mgr.in_progress() is True
    # Exit without waiting. No exceptions or assertions should occur.


def test_subprocess_manager__run__timeout():
    """Should terminate run when timeout"""
    mgr = SubprocessManager()
    start = time.time()
    mgr.run("sleep 10", timeout=1)
    mgr.wait_for_completion()
    duration = time.time() - start
    assert duration < 5
    assert mgr.return_code is None


def test_subprocess_manager__in_progress():
    """Should return true if commands are still running"""
    mgr = SubprocessManager()
    command = "sleep 1"
    mgr.run(command)
    assert mgr.in_progress() is True


def test_subprocess_manager__terminate():
    """Should terminate subprocess on call terminate() method"""
    mgr = SubprocessManager()
    command = "sleep 10"
    mgr.run(command)
    assert mgr.in_progress()

    start = time.time()
    mgr.terminate()
    assert not mgr.in_progress()
    duration = time.time() - start
    assert duration < 5
    assert mgr.return_code is None


def test_run_command__on_output():
    """Should run a command as a subprocess"""
    command = "echo 'Hello World'"
    ret = run_command(command)
    assert ret == 0


def test_run_command__stdout():
    """Should run a command as a subprocess"""
    command = "echo 'Hello Disco'"
    output = {}
    ret = run_command(command, output)
    assert ret == 0
    assert "stdout" in output
    assert "Hello Disco" in output["stdout"]


def test_run_command():
    """Should run a command as a subprocess"""
    command = "ls -l /dirnotexist"
    output = {}
    ret = run_command(command, output)
    assert ret != 0
    assert "stderr" in output
    assert "No such file or directory" in output["stderr"]


def test_run_command_with_retries():
    """Test that a retry works."""
    with tempfile.TemporaryDirectory() as tmpdir:
        script = Path(tmpdir) / "read_input.py"
        input_file = Path(tmpdir) / "inputs.txt"
        input_file.write_text("2")
        content = f"""import sys
from pathlib import Path
input_file = Path("{input_file}")
cur_val = int(input_file.read_text())
input_file.write_text(str(cur_val - 1))
sys.exit(cur_val)
"""
        script.write_text(content)
        command = f"python {script}"
        ret = run_command(command, num_retries=2, retry_delay_s=0.1)
        assert ret == 0


def test_run_command_retries_exhausted():
    """Test retries that never work."""
    command = "ls invalid_test_file"
    ret = run_command(command, num_retries=3, retry_delay_s=0.1)
    assert ret != 0


def test_run_command_skip_retries():
    """Should run a command as a subprocess"""
    command = "jade bad-command"
    output = {}
    errors = ["No such command"]
    # Make sure that we get the expected return.
    ret = run_command(command, output)
    assert ret != 0
    assert "stderr" in output
    assert errors[0] in output["stderr"]

    # Now make it hang if it doesn't skip retries.
    ret = run_command(
        command, output, error_strings=errors, num_retries=sys.maxsize, retry_delay_s=100000
    )
    assert ret != 0
    assert "stderr" in output
    assert "No such command" in output["stderr"]


def test_check_run_command():
    """Test that check_run_command raises an exception."""
    with pytest.raises(ExecutionError):
        check_run_command("ls invalid_test_file")
