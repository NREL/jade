import logging
import shlex
import subprocess
import sys
import time

from jade.exceptions import ExecutionError
from jade.utils.timing_utils import timed_debug


logger = logging.getLogger(__name__)


@timed_debug
def run_command(cmd, output=None, cwd=None, num_retries=0, retry_delay_s=2.0):
    """Runs a command as a subprocess.

    Parameters
    ----------
    cmd : str
        command to run
    output : dict, default=None
        If a dict is passed then return stdout and stderr as keys.
    cwd : str, default=None
        Change the working directory to cwd before executing the process.
    num_retries : int, default=0
        Retry the command on failure this number of times.
        Return code and output are from the last command execution.
    retry_delay_s : float, default=2.0
        Number of seconds to delay in between retries.

    Returns
    -------
    int
        return code from system; usually zero is good, non-zero is error

    Caution: Capturing stdout and stderr in memory can be hazardous with
    long-running processes that output lots of text. In those cases consider
    running subprocess.Popen with stdout and/or stderr set to a pre-configured
    file descriptor.

    """
    logger.debug(cmd)
    # Disable posix if on Windows.
    command = shlex.split(cmd, posix="win" not in sys.platform)
    max_tries = num_retries + 1
    assert max_tries >= 1
    ret = None
    for i in range(max_tries):
        _output = {} if isinstance(output, dict) else None
        ret = _run_command(command, _output, cwd)
        if ret != 0 and num_retries > 0:
            logger.warning("Command [%s] failed on iteration %s: %s", cmd, i + 1, ret)
            if _output:
                logger.debug(_output["stderr"])
        if ret == 0 or i == max_tries - 1:
            if isinstance(output, dict):
                output.update(_output)
            break
        time.sleep(retry_delay_s)

    if ret != 0:
        logger.debug("Command [%s] failed: %s", cmd, ret)
        if output:
            logger.debug(output["stderr"])

    return ret


def _run_command(command, output, cwd):
    if output is not None:
        pipe = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=cwd)
        out, err = pipe.communicate()
        output["stdout"] = out.decode("utf-8")
        output["stderr"] = err.decode("utf-8")
        ret = pipe.returncode
    else:
        ret = subprocess.call(command, cwd=cwd)

    return ret


def check_run_command(*args, **kwargs):
    """Same as run_command except that it raises an exception on failure.

    Raises
    ------
    ExecutionError
        Raised if the command returns a non-zero return code.

    """
    ret = run_command(*args, **kwargs)
    if ret != 0:
        raise ExecutionError(f"command returned error code: {ret}")
