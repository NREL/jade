"""CLI to start a SLURM cluster."""

import logging
import os
import re
import sys

import click

from jade.common import OUTPUT_DIR
from jade.enums import Status
from jade.hpc.common import HpcType
from jade.jobs.cluster import Cluster
from jade.jobs.job_runner import JobRunner
from jade.loggers import setup_logging
from jade.utils.subprocess_manager import run_command
from jade.utils.utils import get_cli_string


logger = logging.getLogger(__name__)


@click.argument(
    "config-file",
    type=str,
)
@click.option(
    "-o", "--output",
    default=OUTPUT_DIR,
    show_default=True,
    help="Output directory."
)
@click.option(
    "-q", "--num-processes",
    default=None,
    show_default=False,
    type=int,
    help="Number of processes to run in parallel; defaults to num CPUs."
)
@click.option(
    "--verbose",
    is_flag=True,
    default=False,
    show_default=True,
    help="Enable verbose log output."
)
@click.command()
def run_jobs(config_file, output, num_processes, verbose):
    """Starts jobs on HPC."""
    match = re.search(r"batch_(\d+)\.json", config_file)
    assert match
    batch_id = match.group(1)
    os.makedirs(output, exist_ok=True)
    filename = os.path.join(output, f"run_jobs_batch_{batch_id}.log")
    level = logging.DEBUG if verbose else logging.INFO
    setup_logging(__name__, filename, file_level=level,
                  console_level=logging.ERROR)
    logger.info(get_cli_string())

    # When running on compute nodes try to submit more jobs before and after
    # running this batch's jobs.
    cluster, _ = Cluster.deserialize(output)
    hpc = cluster.config.submitter_options.hpc_config.hpc_type
    if hpc != HpcType.LOCAL:
        _try_submit_jobs(output)

    mgr = JobRunner(config_file, output=output, batch_id=batch_id)
    status = mgr.run_jobs(verbose=verbose, num_processes=num_processes)
    ret = status.value

    if status == Status.GOOD and hpc != HpcType.LOCAL:
        _try_submit_jobs(output)

    sys.exit(ret)


def _try_submit_jobs(output):
    try_submit_cmd = f"jade try-submit-jobs {output}"
    ret = run_command(try_submit_cmd)
    if ret != 0:
        logger.error("Failed to run '%s' ret=%s", try_submit_cmd, ret)
