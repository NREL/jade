"""Manages multi-node job coordination"""

import logging
import os
import shutil
import socket
import time
from pathlib import Path

import click

from jade.common import CONFIG_FILE
from jade.exceptions import ExecutionError
from jade.jobs.job_configuration_factory import create_config_from_file
from jade.loggers import setup_logging
from jade.spark.metrics import SparkMetrics
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
def run_spark_cluster(job_name, jade_runtime_output, verbose, manager_script_and_args):
    """Create a Spark cluster across multiple nodes. The manager node will invoke the script."""
    config = create_config_from_file(Path(jade_runtime_output) / CONFIG_FILE)
    job = config.get_job(job_name)
    _set_hostnames(jade_runtime_output)
    output = {}
    check_run_command(f"jade cluster am-i-manager {jade_runtime_output}", output)
    result = output["stdout"].strip()
    if result == "true":
        ret = run_cluster_master(job, jade_runtime_output, verbose, manager_script_and_args)
    else:
        assert result == "false", result
        ret = run_worker(job, jade_runtime_output, verbose)

    return ret


def run_cluster_master(job, output_dir, verbose, manager_script_and_args):
    """Run the cluster master instance."""
    shutdown_file = _get_shutdown_file(job.name, output_dir)
    if shutdown_file.exists():
        os.remove(shutdown_file)
    try:
        return _run_cluster_master(job, output_dir, verbose, manager_script_and_args)
    finally:
        # Notify the workers to shutdown.
        shutdown_file.touch()


def _run_cluster_master(job, output_dir, verbose, manager_script_and_args):
    filename = os.path.join(output_dir, f"run_spark_cluster__{job.name}.log")
    level = logging.DEBUG if verbose else logging.INFO
    setup_logging(__name__, filename, file_level=level, console_level=level, mode="w")
    logger.info(
        "Run cluster master on %s job=%s: %s", socket.gethostname(), job.name, get_cli_string()
    )

    # It would be better to start all workers from the master. Doing so would require that
    # Spark processes on the master node be able to ssh into the worker nodes.
    # I haven't spent the time to figure out to do that inside Singularity containers.
    master_cmd = job.model.spark_config.get_start_master()
    logger.info("Run spark master: [%s]", master_cmd)
    check_run_command(master_cmd)
    manager_node = _get_manager_node_name(output_dir)
    worker_cmd = _get_worker_command(job, manager_node)
    logger.info("Run spark worker: [%s]", worker_cmd)
    check_run_command(worker_cmd)

    # Wait for workers.
    # TODO: find a way to check programmatically with the rest api
    # or parse the logs
    time.sleep(15)
    user_output = Path(output_dir) / "user"
    args = list(manager_script_and_args) + [_get_cluster(manager_node), str(user_output)]
    user_cmd = str(job.model.spark_config.get_run_user_script()) + " " + " ".join(args)
    logger.info("Run user script [%s]", user_cmd)

    start = time.time()
    ret = run_command(user_cmd)
    logger.info("Finished job. duration = %s seconds", time.time() - start)

    # TODO: This is failing for an unknown reason.
    # metrics = SparkMetrics("localhost")
    # try:
    #    metrics.generate_metrics(user_output / "spark_metrics")
    # except Exception:
    #    o = {}
    #    logger.exception("Failed to generate metrics")
    #    retcode = run_command("curl -X GET http://localhost:4040/api/v1/applications", output=o)
    #    logger.info("result of curl command: retcode=%s output=%s", retcode, o)
    spark_logs = Path(output_dir) / "spark_logs"
    if spark_logs.exists():
        shutil.rmtree(spark_logs)
    shutil.copytree("/tmp/scratch/spark/logs", spark_logs)

    check_run_command(job.model.spark_config.get_stop_worker())
    check_run_command(job.model.spark_config.get_stop_master())
    return ret


def run_worker(job, output_dir, verbose, poll_interval=60):
    """Run a worker instance."""
    manager_node = _get_manager_node_name(output_dir)
    logger.error("in worker manager_node=%s job=%s", manager_node, job.name)
    hostname = socket.gethostname()
    filename = os.path.join(output_dir, f"run_spark_job_worker__{hostname}__{job.name}.log")
    level = logging.DEBUG if verbose else logging.INFO
    setup_logging(__name__, filename, file_level=level, console_level=level, mode="w")
    logger.info("Run worker: %s", get_cli_string())

    # Give the master a head start.
    time.sleep(10)
    cmd = _get_worker_command(job, manager_node, memory="75g")
    ret = 1
    output = {}
    for _ in range(5):
        output.clear()
        logger.info("Run spark worker: [%s]", cmd)
        ret = run_command(cmd, output=output)
        if ret == 0:
            break
    if ret != 0:
        logger.error("Failed to start spark worker: %s: %s", ret, output)

    shutdown_file = _get_shutdown_file(job.name, output_dir)
    while not shutdown_file.exists():
        logger.debug("sleep for %s seconds", poll_interval)
        time.sleep(poll_interval)

    logger.info("Detected shutdown.")
    check_run_command(job.model.spark_config.get_stop_worker())
    return 0


def _get_cluster(manager_node):
    return f"spark://{manager_node}:7077"


def _get_worker_command(job, manager_node, memory="80g"):
    cluster = _get_cluster(manager_node)
    return job.spark_config.get_start_worker(memory, cluster)


def _get_manager_node_name(output_dir):
    output = {}
    job_id = os.environ["SLURM_JOB_ID"]  # TODO: needs to be agnostic to HPC type
    check_run_command(f"jade cluster manager-node {output_dir} {job_id}", output)
    return output["stdout"].strip()


def _set_hostnames(output_dir):
    output = {}
    job_id = os.environ["SLURM_JOB_ID"]  # TODO: needs to be agnostic to HPC type
    check_run_command(f"jade cluster hostnames -j {job_id} {output_dir}", output)
    hostnames = [x for x in output["stdout"].split() if x != ""]
    logger.info("Found %s hostnames: %s", len(hostnames), hostnames)
    os.environ["JADE_OUTPUT_DIR"] = output_dir
    os.environ["JADE_COMPUTE_NODE_NAMES"] = " ".join(hostnames)
    return hostnames


def _get_shutdown_file(job_name, output_dir):
    return Path(output_dir) / (SHUTDOWN_FILENAME + "__" + job_name)
