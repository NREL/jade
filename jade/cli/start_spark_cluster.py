import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

import click

from jade.common import OUTPUT_DIR
from jade.utils.run_command import check_run_command, run_command


@click.command()
@click.argument("compute-nodes", nargs=-1)
@click.option(
    "-C",
    "--container",
    required=True,
    help="Path to Spark container",
    type=click.Path(exists=True),
    callback=lambda _, __, x: Path(x),
)
@click.option(
    "-c",
    "--spark-conf",
    required=True,
    help="Spark conf directory",
    type=click.Path(exists=True),
    callback=lambda _, __, x: Path(x),
)
@click.option(
    "-s",
    "--script",
    help="Script to run on cluster. If None, sleep indefinitely.",
)
@click.option(
    "-o",
    "--output",
    default=OUTPUT_DIR,
    help="Output directory",
    callback=lambda _, __, x: Path(x),
)
@click.option(
    "-f",
    "--force",
    is_flag=True,
    default=False,
    show_default=True,
    help="Overwrite output directory if it exists.",
)
def start_spark_cluster(compute_nodes, container, spark_conf, script, output, force):
    conda_env = os.environ.get("CONDA_DEFAULT_ENV")
    if conda_env is None:
        print(f"Did not detect conda environment. Exiting.", file=sys.stderr)
        sys.exit(1)
    if not compute_nodes:
        print("compute-nodes cannot be empty", file=sys.stderr)
        sys.exit(1)
    if output.exists():
        if force:
            shutil.rmtree(output)
        else:
            print(f"{output} already exists. Choose a different path or set --force to overwrite.")
            sys.exit(1)
    output.mkdir()
    master_node = compute_nodes[0]
    worker_nodes = []
    if len(compute_nodes) > 1:
        worker_nodes = compute_nodes[1:]

    _start_spark_master(master_node, container, spark_conf, script, output, conda_env)
    for worker in worker_nodes:
        _start_spark_worker(worker, master_node, container, spark_conf, output, conda_env)


def _start_spark_master(node_name, container, spark_conf, script, output_dir, conda_env):
    container = container.absolute()
    spark_conf = spark_conf.absolute()
    output_dir = output_dir.absolute()
    cwd = os.getcwd()

    script_option = "" if script is None else f"-s {os.path.abspath(script)}"
    text = f"""#!/bin/bash
ssh {node_name} "conda activate {conda_env}
cd {cwd}
jade-internal start-spark-cluster-master {container} {spark_conf} {output_dir} {script_option}"
"""
    spark_script = Path("/tmp/create_spark_master.sh")
    spark_script.write_text(text)
    try:
        subprocess.Popen(["bash", spark_script])
        time.sleep(2)
    finally:
        spark_script.unlink()


def _start_spark_worker(
    node_name, cluster_master_node_name, container, spark_conf, output_dir, conda_env
):
    container = container.absolute()
    spark_conf = spark_conf.absolute()
    output_dir = output_dir.absolute()
    cwd = os.getcwd()
    text = f"""#!/bin/bash
ssh {node_name} "conda activate {conda_env}
cd {cwd}
jade-internal start-spark-cluster-worker {cluster_master_node_name} {container} {spark_conf} {output_dir}"
"""
    script = Path("/tmp/create_spark_cluster_worker.sh")
    script.write_text(text)
    try:
        subprocess.Popen(["bash", script])
        time.sleep(2)
    finally:
        script.unlink()
