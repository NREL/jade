"""Manages multi-node job coordination"""

import logging
import os
import shutil
import socket
import time
from pathlib import Path

import click

from jade.common import CONFIG_FILE, JOBS_OUTPUT_DIR
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

    job_output = Path(output_dir) / JOBS_OUTPUT_DIR / job.name
    if job_output.exists():
        shutil.rmtree(job_output)
    job_output.mkdir(parents=True)
    events_dir = job_output / "spark" / "events"
    events_dir.mkdir(parents=True)
    logs_dir = job_output / "spark" / "logs"
    logs_dir.mkdir()

    # Make a job-specific conf directory because the log and event files need to be per-job.
    job_conf_dir = job_output / "spark" / "conf"
    shutil.copytree(Path(job.model.spark_config.conf_dir) / "conf", job_conf_dir)
    _fix_spark_conf_file(job_conf_dir, events_dir)
    _set_env_variables(job_conf_dir, logs_dir)

    # It would be better to start all workers from the master. Doing so would require that
    # Spark processes on the master node be able to ssh into the worker nodes.
    # I haven't spent the time to figure out to do that inside Singularity containers.
    master_cmd = job.model.spark_config.get_start_master()
    logger.info("Run spark master: [%s]", master_cmd)
    check_run_command(master_cmd)
    history_cmd = job.model.spark_config.get_start_history_server()
    logger.info("Run spark history server: [%s]", history_cmd)
    check_run_command(history_cmd)
    manager_node = _get_manager_node_name(output_dir)
    # Let the master keep some extra memory for its services and the history server.
    # This also means that the master node can start 7 executors with default settings.
    # 80 - 3 = 77. 77g / 11g per executor = 7
    worker_memory = str(job.model.spark_config.worker_memory_gb - 3) + "g"
    worker_cmd = _get_worker_command(job, manager_node, memory=worker_memory)
    logger.info("Run spark worker: [%s]", worker_cmd)
    check_run_command(worker_cmd)

    # Wait for workers.
    # TODO: find a way to check programmatically with the rest api
    # or parse the logs
    time.sleep(15)
    args = list(manager_script_and_args) + [_get_cluster(manager_node), str(job_output)]
    user_cmd = str(job.model.spark_config.get_run_user_script()) + " " + " ".join(args)
    logger.info("Run user script [%s]", user_cmd)

    start = time.time()
    ret = run_command(user_cmd)
    logger.info("Finished job. duration = %s seconds", time.time() - start)

    # Delay to ensure the history is saved.
    time.sleep(10)
    metrics = SparkMetrics("localhost", history=True)
    try:
        metrics.generate_metrics(job_output / "spark_metrics")
    except Exception:
        logger.exception("Failed to generate metrics")

    check_run_command(job.model.spark_config.get_stop_worker())
    check_run_command(job.model.spark_config.get_stop_history_server())
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
    job_output = Path(output_dir) / JOBS_OUTPUT_DIR / job.name
    logs_dir = job_output / "spark" / "logs"
    job_conf_dir = job_output / "spark" / "conf"
    _set_env_variables(job_conf_dir, logs_dir)
    worker_memory = str(job.model.spark_config.worker_memory_gb) + "g"
    cmd = _get_worker_command(job, manager_node, worker_memory)
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


def _set_env_variables(conf_dir, logs_dir):
    os.environ["SPARK_CONF_DIR"] = str(conf_dir.absolute())
    os.environ["SPARK_LOG_DIR"] = str(logs_dir.absolute())


def _get_cluster(manager_node):
    return f"spark://{manager_node}:7077"


def _get_worker_command(job, manager_node, memory):
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


def _fix_spark_conf_file(job_conf_dir, events_dir):
    events_dir = events_dir.absolute()
    conf_file = job_conf_dir / "spark-defaults.conf"
    with open(conf_file, "a") as f_out:
        f_out.write(f"spark.eventLog.dir file://{events_dir}\n")
        f_out.write(f"spark.history.fs.logDirectory file://{events_dir}\n")
