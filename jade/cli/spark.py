import logging
import os
import shutil
import stat
import subprocess
import sys
import time
from pathlib import Path

import click

from jade.common import HPC_CONFIG_FILE, OUTPUT_DIR
from jade.loggers import setup_logging
from jade.models import HpcConfig, get_model_defaults
from jade.models.spark import SparkConfigModel, SparkContainerModel
from jade.utils.utils import dump_data, load_data


SPARK_MODEL_DEFAULTS = get_model_defaults(SparkConfigModel)
DYNAMIC_ALLOCATION_SETTINGS = """spark.dynamicAllocation.enabled true
spark.dynamicAllocation.shuffleTracking.enabled true
spark.shuffle.service.enabled true"""
GPU_DISCOVERY_SCRIPT = "/opt/sparkRapidsPlugin/getGpusResources.sh"


def _replace_tag(values, filename):
    text = filename.read_text()
    for tag, value in values:
        text = text.replace(f"<{tag}>", value)
    filename.write_text(text)


def _gpu_cb(_, __, val):
    if val is None:
        return val
    val = val.lower()
    if val == "true":
        return True
    if val == "false":
        return False
    raise click.BadParameter(f"Supported values for 'gpu': true or false")


@click.command()
@click.option(
    "-C",
    "--collect-worker-logs",
    default=False,
    is_flag=True,
    show_default=True,
    help="Collect logs from worker processes.",
)
@click.option(
    "-c",
    "--container-path",
    type=str,
    required=True,
    help="Path to container that can run Spark",
)
@click.option(
    "--dynamic-allocation/--no-dynamic-allocation",
    is_flag=True,
    default=False,
    show_default=True,
    help="Enable Spark dynamic resource allocation.",
)
@click.option(
    "-g",
    "--gpu",
    default=None,
    callback=_gpu_cb,
    help="Possible values: not set (default), 'true', 'false'. If not set, create GPU-ready "
    "config if hpc_config.toml specifies GPUs.",
)
@click.option(
    "-h",
    "--hpc-config",
    type=click.Path(exists=True),
    default=HPC_CONFIG_FILE,
    show_default=True,
    help="HPC config file to be used. Defines number of nodes.",
)
@click.option(
    "-m",
    "--master-node-memory-overhead-gb",
    default=SPARK_MODEL_DEFAULTS["master_node_memory_overhead_gb"],
    show_default=True,
    help="Memory overhead for Spark master processes",
)
@click.option(
    "-n",
    "--node-memory-overhead-gb",
    default=SPARK_MODEL_DEFAULTS["node_memory_overhead_gb"],
    show_default=True,
    help="Memory overhead for node operating system and existing applications",
)
@click.option(
    "-r",
    "--run-user-script-inside-container",
    type=bool,
    is_flag=True,
    default=False,
    show_default=True,
    help="Run the user script inside the container.",
)
@click.option(
    "-S",
    "--spark-dir",
    type=str,
    default="spark",
    show_default=True,
    help="Spark configuration directory to create.",
)
@click.option(
    "-s",
    "--shuffle-partition-multiplier",
    type=int,
    default=1,
    show_default=True,
    help="Set spark.sql.shuffle.partitions to total_cores multiplied by this value.",
)
@click.option(
    "-U",
    "--use-tmpfs-for-scratch",
    is_flag=True,
    default=False,
    show_default=True,
    help="Use tmpfs on node for scratch space.",
)
@click.option(
    "-a",
    "--alt-scratch",
    default=None,
    show_default=True,
    help="Use this alternative directory for scratch space.",
)
@click.option(
    "-u",
    "--update-config-file",
    type=str,
    default=None,
    help="Update all jobs in this config file with this Spark configuration.",
)
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    default=False,
    show_default=True,
    help="Enable verbose log output.",
)
@click.option(
    "-W",
    "--worker-memory-gb",
    default=SPARK_MODEL_DEFAULTS["worker_memory_gb"],
    show_default=True,
    help="If 0, give all node memory minus overhead to worker.",
)
@click.option(
    "-f",
    "--force",
    default=False,
    is_flag=True,
    show_default=True,
    help="Overwrite the spark configuration directory if it already exists.",
)
def config(
    collect_worker_logs,
    container_path,
    dynamic_allocation,
    gpu,
    hpc_config,
    master_node_memory_overhead_gb,
    node_memory_overhead_gb,
    run_user_script_inside_container,
    spark_dir,
    shuffle_partition_multiplier,
    update_config_file,
    use_tmpfs_for_scratch,
    alt_scratch,
    verbose,
    worker_memory_gb,
    force,
):
    """Create a Spark configuration to use for running a job on a Spark cluster."""
    level = logging.DEBUG if verbose else logging.WARNING
    setup_logging("config_spark", None, console_level=level)
    spark_dir = Path(spark_dir)
    if spark_dir.exists():
        if force:
            shutil.rmtree(spark_dir)
        else:
            print(
                f"The directory '{spark_dir}' already exists. Use a different name or pass --force to overwrite.",
                file=sys.stderr,
            )
            sys.exit(1)
    spark_dir.mkdir(parents=True)

    if use_tmpfs_for_scratch and alt is not None:
        print("use_tmpfs_for_scratch and alt_scratch cannot both be set", file=sys.stderr)
        sys.exit(1)

    hpc_config_data = HpcConfig.load(hpc_config)
    nodes = getattr(hpc_config_data.hpc, "nodes", None)
    if nodes is None:
        print(f"hpc_type={hpc_config_data.hpc_type} doesn't have a nodes field", file=sys.stderr)
        sys.exit(1)
    mem = getattr(hpc_config_data.hpc, "mem", None)
    if mem is None:
        executor_mem_gb = 11
        print(f"Use default per-executor memory of {executor_mem_gb}G")
    else:
        num_executors = 7
        if not mem.endswith("G"):
            raise Exception(f"This feature only supports HPC memory requirements ending with 'G'")
        per_node_mem_gb = int(mem[:-1])
        if use_tmpfs_for_scratch:
            per_node_mem_gb //= 2
        overhead = master_node_memory_overhead_gb - node_memory_overhead_gb
        executor_mem_gb = (per_node_mem_gb - overhead) // num_executors
        print(f"Use custom per-executor memory of {executor_mem_gb}G based on per-node {mem}")

    for dirname in ("bin", "conf"):
        src_path = Path(os.path.dirname(__file__)).parent / "spark" / dirname
        dst_path = spark_dir / dirname
        if not dst_path.exists():
            dst_path.mkdir()
        for filename in src_path.iterdir():
            shutil.copyfile(filename, dst_path / filename.name)

    use_gpus = _should_use_gpus(hpc_config_data, gpu)

    with open(spark_dir / "conf" / "spark-defaults.conf", "a") as f_out:
        f_out.write("\n")
        f_out.write(f"spark.executor.memory {executor_mem_gb}G\n")
        # Online documentation says this value should correlate with the number of cores in the
        # cluster. Some sources say 1 per core, others say 2 or 4 per core. Depends on use case.
        # This should be a reasonable default for users, who can customize dynamically.
        params = ["spark.sql.shuffle.partitions"]
        # Some sources say that we should set spark.default.parallelism to the same value,
        # others say it doesn't work. Experiments showed harmful effects if dynamic allocation
        # was enabled with a custom value.
        for param in params:
            f_out.write(param)
            f_out.write(" ")
            f_out.write(str(nodes * 35 * shuffle_partition_multiplier))
            f_out.write("\n")

        if dynamic_allocation:
            f_out.write("\n")
            f_out.write(DYNAMIC_ALLOCATION_SETTINGS)
            f_out.write("\n")

        if use_gpus:
            src_path = (
                Path(os.path.dirname(__file__)).parent / "spark" / "conf" / "resourcesFile.json"
            )
            resources_file = spark_dir / "conf" / "resourcesFile.json"
            shutil.copyfile(src_path, resources_file)
            f_out.write(
                "spark.worker.resource.gpu.discoveryScript /opt/sparkRapidsPlugin/getGpusResources.sh\n"
            )
            f_out.write(f"spark.worker.resourcesFile {resources_file}\n")

    if use_gpus:
        filename = spark_dir / "conf" / "spark-env.sh"
        with open(filename, "a") as f_out:
            num_gpus = hpc_config_data.get_num_gpus() or 2
            f_out.write(
                f'SPARK_WORKER_OPTS="-Dspark.worker.resource.gpu.amount={num_gpus} '
                f'-Dspark.worker.resource.gpu.discoveryScript={GPU_DISCOVERY_SCRIPT}"\n'
            )

    replacement_values = [
        ("SPARK_DIR", str(spark_dir)),
        ("CONTAINER_PATH", container_path),
    ]
    for name in ("run_spark_script_wrapper.sh", "run_user_script_wrapper.sh"):
        filename = spark_dir / "bin" / name
        _replace_tag(replacement_values, filename)
        st = os.stat(filename)
        os.chmod(filename, st.st_mode | stat.S_IEXEC)
        print(f"Assigned paths in {filename}")

    scripts = [spark_dir / "conf" / "spark-env.sh"] + list((spark_dir / "bin").glob("*.sh"))
    for script in scripts:
        st = os.stat(script)
        os.chmod(script, st.st_mode | stat.S_IEXEC)

    print(
        f"Created Spark configuration in {spark_dir.absolute()} for a {nodes}-node cluster. "
        f"GPUs={use_gpus}"
    )

    spark_config = SparkConfigModel(
        collect_worker_logs=collect_worker_logs,
        conf_dir=str(spark_dir),
        container=SparkContainerModel(path=container_path),
        enabled=True,
        master_node_memory_overhead_gb=master_node_memory_overhead_gb,
        node_memory_overhead_gb=node_memory_overhead_gb,
        run_user_script_inside_container=run_user_script_inside_container,
        use_tmpfs_for_scratch=use_tmpfs_for_scratch,
        alt_scratch=alt_scratch,
        worker_memory_gb=worker_memory_gb,
    )

    if update_config_file is not None:
        if not Path(update_config_file).exists():
            print(f"'update_config_file={update_config_file} does not exist", file=sys.stderr)
            sys.exit(1)
        config = load_data(update_config_file)
        for job in config["jobs"]:
            job["spark_config"] = spark_config.dict()
        dump_data(config, update_config_file, indent=2)
        print(f"Updated jobs in {update_config_file} with this Spark configuration.")
    else:
        print(
            "\nAdd and customize this JSON object to the 'spark_config' field for each Spark "
            "job in your config.json file:\n"
        )
        print(spark_config.json(indent=2))


def _should_use_gpus(hpc_config, gpu):
    hc_gpus = getattr(hpc_config.hpc, "gres", None)
    if gpu is None:
        use_gpus = hc_gpus is not None
    elif gpu:
        use_gpus = True
    else:
        use_gpus = False
    return use_gpus


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
def start_cluster(compute_nodes, container, spark_conf, script, output, force):
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


@click.group()
def spark():
    """Spark commands"""


spark.add_command(config)
spark.add_command(start_cluster)
