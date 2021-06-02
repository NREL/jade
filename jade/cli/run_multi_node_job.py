"""Manages multi-node job coordination"""

import logging
import os
import socket
import time
from pathlib import Path

import click

from jade.loggers import setup_logging
from jade.utils.run_command import check_run_command, run_command
from jade.utils.utils import get_cli_string


SHUTDOWN_FILENAME = "shutdown"

logger = logging.getLogger(__name__)


@click.command(
    context_settings=dict(
        allow_extra_args=True,
        ignore_unknown_options=True,
    )
)
@click.argument("job_name")
@click.option(
    "-o",
    "--jade-runtime-output",
    required=True,
    type=click.Path(exists=True),
    help="output directory passed to 'jade submit-jobs'",
)
@click.option(
    "--verbose", is_flag=True, default=False, show_default=True, help="Enable verbose log output."
)
@click.argument("manager_script_and_args", nargs=-1, type=click.UNPROCESSED)
def run_multi_node_job(job_name, jade_runtime_output, verbose, manager_script_and_args):
    """Run a job across multiple nodes. The manager node will invoke manager_script_and_args."""
    output = {}
    check_run_command(f"jade cluster am-i-manager {jade_runtime_output}", output)
    result = output["stdout"].strip()
    if result == "true":
        ret = run_manager(job_name, jade_runtime_output, verbose, manager_script_and_args)
    else:
        assert result == "false", result
        # The only purpose of this worker function is to keep the node allocation
        # alive. There are more efficient ways of doing this with HPC commands.
        # However, this procedure allows us to run the JADE JobRunner in the
        # background on each node and collect resource utilization statistics.
        ret = run_worker(job_name, jade_runtime_output, verbose)

    return ret


def run_manager(job_name, output_dir, verbose, manager_script_and_args):
    """Run the manager instance."""
    shutdown_file = _get_shutdown_file(job_name, output_dir)
    if shutdown_file.exists():
        os.remove(shutdown_file)
    try:
        return _run_manager(job_name, output_dir, verbose, manager_script_and_args)
    finally:
        # Notify the workers to shutdown.
        shutdown_file.touch()


def _run_manager(job_name, output_dir, verbose, manager_script_and_args):
    filename = os.path.join(output_dir, f"run_multi_node_job_manager__{job_name}.log")
    level = logging.DEBUG if verbose else logging.INFO
    setup_logging(__name__, filename, file_level=level, console_level=level, mode="w")
    logger.info("Run manager on %s: %s", socket.gethostname(), get_cli_string())

    # Note that the manager receives its own hostname.
    output = {}
    check_run_command(f"jade cluster hostnames {output_dir}", output)
    hostnames = [x for x in output["stdout"].split() if x != ""]
    logger.info("Manager found %s hostnames: %s", len(hostnames), hostnames)
    cmd = " ".join(manager_script_and_args)
    logger.info("Run manager script [%s]", cmd)

    os.environ["JADE_OUTPUT_DIR"] = output_dir
    os.environ["JADE_COMPUTE_NODE_NAMES"] = " ".join(hostnames)
    start = time.time()
    ret = run_command(cmd)
    logger.info("Finished job. duration = %s seconds", time.time() - start)
    return ret


def run_worker(job_name, output_dir, verbose, poll_interval=60):
    """Run a worker instance."""
    hostname = socket.gethostname()
    filename = os.path.join(output_dir, f"run_multi_node_job_worker__{job_name}__{hostname}.log")
    level = logging.DEBUG if verbose else logging.INFO
    setup_logging(__name__, filename, file_level=level, console_level=level, mode="w")
    logger.info("Run worker: %s", get_cli_string())

    shutdown_file = _get_shutdown_file(job_name, output_dir)
    while not shutdown_file.exists():
        logger.debug("sleep for %s seconds", poll_interval)
        time.sleep(poll_interval)

    logger.info("Detected shutdown.")
    return 0


def _get_shutdown_file(job_name, output_dir):
    return Path(output_dir) / (SHUTDOWN_FILENAME + "__" + job_name)
