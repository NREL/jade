"""CLI to display and manage config files."""

from jade.models.submission_group import SubmissionGroup
import json
import logging
import os
import re
import shutil
import stat
import sys
import tempfile
from pathlib import Path

import click
from prettytable import PrettyTable

from jade.cli.common import COMMON_SUBMITTER_OPTIONS, add_options, make_submitter_params
from jade.jobs.cluster import Cluster
from jade.common import CONFIG_FILE, HPC_CONFIG_FILE
from jade.extensions.generic_command import GenericCommandConfiguration, GenericCommandParameters
from jade.jobs.job_configuration_factory import create_config_from_file
from jade.loggers import setup_logging
from jade.utils.run_command import check_run_command
from jade.utils.utils import dump_data, load_data
from jade.models import HpcConfig, SlurmConfig, FakeHpcConfig, LocalHpcConfig, get_model_defaults
from jade.models.spark import SparkConfigModel, SparkContainerModel


logger = logging.getLogger(__name__)


def _handle_indexes_list(_, __, indexes):
    if indexes is None:
        return indexes
    return [int(x) for x in indexes]


def _handle_indexes_set(_, __, indexes):
    if indexes is None:
        return indexes
    return {int(x) for x in indexes}


@click.group()
def config():
    """Manage a JADE configuration."""
    setup_logging("config", None)


@click.command()
@click.argument("filename", type=click.Path(exists=True))
@click.option(
    "-A",
    "--append-job-name",
    default=False,
    is_flag=True,
    show_default=True,
    help="Set append_job_name for every job.",
)
@click.option(
    "-a",
    "--append-output-dir",
    default=False,
    is_flag=True,
    show_default=True,
    help="Set append_output_dir for every job.",
)
@click.option(
    "-c",
    "--config-file",
    default=CONFIG_FILE,
    show_default=True,
    help="config file to generate.",
)
@click.option(
    "-C",
    "--cancel-on-blocking-job-failure",
    is_flag=True,
    default=False,
    show_default=True,
    help="cancel any job if one of its blocking jobs fails.",
)
@click.option(
    "-m",
    "--minutes-per-job",
    type=int,
    help="estimated minutes per job.",
)
@click.option(
    "--shuffle/--no-shuffle",
    is_flag=True,
    default=False,
    show_default=True,
    help="Shuffle order of jobs.",
)
@click.option(
    "--strip-whitespace/--no-strip-whitespace",
    is_flag=True,
    default=False,
    show_default=True,
    help="Strip whitespace in file.",
)
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    default=False,
    show_default=True,
    help="Enable verbose log output.",
)
def create(
    filename,
    append_job_name,
    append_output_dir,
    config_file,
    cancel_on_blocking_job_failure,
    minutes_per_job,
    shuffle,
    strip_whitespace,
    verbose,
):
    """Create a config file from a filename with a list of executable commands."""
    level = logging.DEBUG if verbose else logging.WARNING
    setup_logging("auto_config", None, console_level=level)

    config = GenericCommandConfiguration.auto_config(
        filename,
        cancel_on_blocking_job_failure=cancel_on_blocking_job_failure,
        minutes_per_job=minutes_per_job,
        append_job_name=append_job_name,
        append_output_dir=append_output_dir,
    )
    if shuffle:
        config.shuffle_jobs()
    print(f"Created configuration with {config.get_num_jobs()} jobs.")
    indent = None if strip_whitespace else 2
    config.dump(config_file, indent=indent)
    print(f"Dumped configuration to {config_file}.\n")


@click.command()
@click.argument("config_file", type=click.Path(exists=True))
@click.argument("job_index", type=int)
@click.argument("blocking_job_indexes", nargs=-1, callback=_handle_indexes_set)
@click.option(
    "-o",
    "--output-file",
    default="updated_config.json",
    show_default=True,
    help="Create new config file with updated jobs.",
)
def assign_blocked_by(config_file, job_index, blocking_job_indexes, output_file):
    """Assign the blocked_by attribute for job_name with jobs specified by block_job_indexes
    (0-based). If no blocking jobs are specified then make all other jobs blocking.

    \b
    Examples:
    1. Make the last job blocked by all other jobs.
       jade config assign-blocked-by config.json post-process-job -o new-config.json
    2. Select first 10 indexes through shell expansion.
       jade config assign-blocked-by config.json post-process-job {0..9} -o new-config.json
    3. Specify blocking indexes.
       jade config assign-blocked-by config.json post-process-job 0 1 2 3 -o new-config.json
    """
    if blocking_job_indexes and job_index in blocking_job_indexes:
        print(f"job_index={job_index} is included in blocking_job_indexes", file=sys.stderr)
        sys.exit(1)

    config = create_config_from_file(config_file)
    if job_index > config.get_num_jobs() - 1:
        print(f"Invalid job_index={job_index}. Max={config.get_num_jobs() - 1}", file=sys.stderr)
        sys.exit(1)

    blocking_jobs = set()
    main_job = None
    for i, job in enumerate(config.iter_jobs()):
        if i == job_index:
            main_job = job
            continue
        if i != job_index and (not blocking_job_indexes or i in blocking_job_indexes):
            blocking_jobs.add(job.name)

    assert main_job
    main_job.set_blocking_jobs(blocking_jobs)
    config.dump(output_file, indent=2)
    print(f"Added {len(blocking_jobs)} blocking jobs to {main_job.name} in {output_file}")


@click.command()
@click.option(
    "-a",
    "--account",
    default="",
    help="HPC account/allocation",
)
@click.option(
    "-c",
    "--config-file",
    default="hpc_config.toml",
    show_default=True,
    help="config file to create",
)
@click.option(
    "-m",
    "--mem",
    default=None,
    help="Amount of memory required by a single node.",
)
@click.option(
    "-p",
    "--partition",
    default=None,
    help="HPC partition",
)
@click.option(
    "-q",
    "--qos",
    default=None,
    type=str,
    help="QoS value",
)
@click.option(
    "-t",
    "--hpc-type",
    type=click.Choice(["slurm", "fake", "local"]),
    default="slurm",
    show_default=True,
    help="HPC queueing system",
)
@click.option(
    "--tmp",
    default=None,
    help="Amount of local storage space required by a single node.",
)
@click.option(
    "-w",
    "--walltime",
    default="4:00:00",
    help="HPC walltime",
)
def hpc(account, config_file, mem, partition, qos, hpc_type, tmp, walltime):
    """Create an HPC config file."""
    if hpc_type == "slurm":
        hpc = SlurmConfig(
            account=account,
            mem=mem,
            partition=partition,
            qos=qos,
            tmp=tmp,
            walltime=walltime,
        )
    elif hpc_type == "fake":
        hpc = FakeHpcConfig(walltime=walltime)
    else:
        assert hpc_type == "local"
        hpc = LocalHpcConfig()

    # This converts enums to values.
    data = json.loads(HpcConfig(hpc_type=hpc_type, hpc=hpc).json())
    dump_data(data, config_file)
    print(f"Created HPC config file {config_file}")


@click.command()
@click.argument("config_file", type=click.Path(exists=True))
@click.option(
    "-f",
    "--fields",
    multiple=True,
    help="include in output table; can specify mulitple times",
)
@click.option(
    "-n",
    "--no-blocked-by",
    is_flag=True,
    default=False,
    show_default=True,
    help="exclude blocking jobs in table",
)
def show(config_file, fields, no_blocked_by):
    """Show the jobs in the configuration."""
    _show(config_file, fields, not no_blocked_by)


# This is a standalone function so that it can be called from _filter.
def _show(config_file, fields=None, blocked_by=True):
    config = create_config_from_file(config_file)
    num_jobs = config.get_num_jobs()
    print(f"Num jobs: {num_jobs}")
    if num_jobs == 0:
        return

    # generic_command jobs have a command field which is very useful.
    # Other extensions do not.
    has_command = False
    for job in config.iter_jobs():
        if isinstance(job, GenericCommandParameters):
            has_command = True
            break

    field_names = ["index", "name"]
    if has_command:
        field_names.append("command")
    if blocked_by:
        field_names.append("blocked_by (job names)")
    if fields is not None:
        field_names += fields

    table = PrettyTable()
    table.field_names = field_names
    for i, job in enumerate(config.iter_jobs()):
        job_dict = job.serialize()
        row = [i, job.name]
        if has_command:
            row.append(job_dict.get("command", ""))
        if blocked_by:
            blocking_jobs = sorted(list(job.get_blocking_jobs()))
            text = ", ".join(blocking_jobs)
            if len(text) > 50:
                text = f"truncated...blocked by {len(blocking_jobs)} jobs"
            row.append(text)
        if fields is not None:
            for field in fields:
                row.append(job_dict.get(field, ""))
        table.add_row(row)
    print(table)


@click.command("filter")
@click.argument("config_file", type=click.Path(exists=True))
@click.argument("indexes", nargs=-1, callback=_handle_indexes_list)
@click.option(
    "-o",
    "--output-file",
    help="Create new config file with filtered jobs.",
)
@click.option(
    "-f",
    "--fields",
    type=str,
    multiple=True,
    nargs=2,
    help="Filter on field value. Multiple accepted.",
)
@click.option(
    "-s",
    "--show-config",
    is_flag=True,
    show_default=True,
    default=False,
    help="Show the new config (only applicable if output-file is provided).",
)
# Named _filter to avoid collisions with the built-in function.
def _filter(config_file, output_file, indexes, fields, show_config=False):
    """Filters jobs in CONFIG_FILE. Prints the new jobs to the console or
    optionally creates a new file.

    Note: This does not detect duplicate ranges.

    \b
    Examples:
    1. Select the first two jobs by index. Output only.
       jade config filter c1.json 0 1
    2. Select indexes through shell expansion, create new file.
       jade config filter c1.json {0..4} {10..14} 20 25 -o c2.json
    3. Select jobs with parameters param1=green and param2=3.
       jade config filter c1.json -o c2.json -f param1 green -f param2 3

    """
    cfg = load_data(config_file)
    jobs = cfg["jobs"]
    if not jobs:
        print("The configuration has no jobs", file=sys.stderr)
        sys.exit(1)

    if output_file is None:
        handle, new_config_file = tempfile.mkstemp(suffix=".json")
        os.close(handle)
        show_config = True
    else:
        new_config_file = output_file

    try:
        if not new_config_file.endswith(".json"):
            print("new_config_file must have extension .json", file=sys.stderr)
            sys.exit(1)

        orig_len = len(jobs)

        # Note: when looking at just the JSON, there is no way to get the job name,
        # and so we can't check for duplicates.

        if indexes:
            new_jobs = [jobs[i] for i in indexes]
        else:
            new_jobs = jobs

        if fields:
            final_jobs = []
            for job in new_jobs:
                matched = True
                for field in fields:
                    if str(job[field[0]]) != field[1]:
                        matched = False
                        break
                if matched:
                    final_jobs.append(job)

            new_jobs = final_jobs

        cfg["jobs"] = new_jobs
        new_len = len(cfg["jobs"])
        dump_data(cfg, new_config_file, indent=4)
        print(f"Filtered {config_file} ({orig_len} jobs) into ({new_len} jobs)\n")
        if output_file is not None:
            print(f"Wrote new config to {output_file}")

        if show_config:
            _show(new_config_file)
    finally:
        if output_file is None:
            os.remove(new_config_file)


SPARK_MODEL_DEFAULTS = get_model_defaults(SparkConfigModel)


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
    "--run-user-script-outside-container",
    type=bool,
    is_flag=True,
    default=False,
    show_default=True,
    help="Run the user script outside the container.",
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
def spark(
    collect_worker_logs,
    container_path,
    hpc_config,
    master_node_memory_overhead_gb,
    node_memory_overhead_gb,
    run_user_script_outside_container,
    spark_dir,
    shuffle_partition_multiplier,
    update_config_file,
    use_tmpfs_for_scratch,
    verbose,
    worker_memory_gb,
):
    """Create a Spark configuration to use for running a job on a Spark cluster."""
    level = logging.DEBUG if verbose else logging.WARNING
    setup_logging("config_spark", None, console_level=level)
    spark_dir = Path(spark_dir)

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
            raise Exception(f"This feature only support HPC memory requirements ending with 'G'")
        per_node_mem_gb = int(mem[:-1])
        if use_tmpfs_for_scratch:
            per_node_mem_gb //= 2
        overhead = master_node_memory_overhead_gb - node_memory_overhead_gb
        executor_mem_gb = (per_node_mem_gb - overhead) // num_executors
        print(f"Use custom per-executor memory of {executor_mem_gb}G based on per-node {mem}")

    if not spark_dir.exists():
        spark_dir.mkdir(parents=True)
    for dirname in ("bin", "conf"):
        src_path = Path(os.path.dirname(__file__)).parent / "spark" / dirname
        dst_path = spark_dir / dirname
        if not dst_path.exists():
            dst_path.mkdir()
        for filename in src_path.iterdir():
            shutil.copyfile(filename, dst_path / filename.name)

    with open(spark_dir / "conf" / "spark-defaults.conf", "a") as f_out:
        f_out.write("\n")
        f_out.write(f"spark.executor.memory {executor_mem_gb}G\n")
        # Online documentation says this value should correlate with the number of cores in the
        # cluster. Some sources say 1 per core, others say 2 or 4 per core. Depends on use case.
        # This should be a reasonable default for users, who can customize dynamically.
        for param in ("spark.sql.shuffle.partitions", "spark.default.parallelism"):
            f_out.write(param)
            f_out.write(" ")
            f_out.write(str(nodes * 35 * shuffle_partition_multiplier))
            f_out.write("\n")
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

    print(f"Created Spark configuration in {spark_dir.absolute()} for a {nodes}-node cluster.")

    spark_config = SparkConfigModel(
        collect_worker_logs=collect_worker_logs,
        conf_dir=str(spark_dir),
        container=SparkContainerModel(path=container_path),
        enabled=True,
        master_node_memory_overhead_gb=master_node_memory_overhead_gb,
        node_memory_overhead_gb=node_memory_overhead_gb,
        run_user_script_outside_container=run_user_script_outside_container,
        use_tmpfs_for_scratch=use_tmpfs_for_scratch,
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


def _replace_tag(values, filename):
    text = filename.read_text()
    for tag, value in values:
        text = text.replace(f"<{tag}>", value)
    filename.write_text(text)


@click.command()
@click.option(
    "-c",
    "--config-file",
    default="submitter_params.json",
    show_default=True,
    type=Path,
    help="config file to create; can be .toml or .json",
)
@add_options(COMMON_SUBMITTER_OPTIONS)
def submitter_params(
    config_file=None,
    dry_run=None,
    per_node_batch_size=None,
    hpc_config=None,
    local=None,
    max_nodes=None,
    poll_interval=None,
    resource_monitor_interval=None,
    resource_monitor_type=None,
    num_processes=None,
    verbose=None,
    reports=None,
    enable_singularity=None,
    container=None,
    try_add_blocked_jobs=None,
    time_based_batching=None,
    node_setup_script=None,
    node_shutdown_script=None,
    no_distributed_submitter=None,
):
    """Create parameters for use in 'jade submit-jobs'."""
    params = make_submitter_params(
        per_node_batch_size=per_node_batch_size,
        dry_run=dry_run,
        hpc_config=hpc_config,
        local=local,
        max_nodes=max_nodes,
        poll_interval=poll_interval,
        resource_monitor_interval=resource_monitor_interval,
        resource_monitor_type=resource_monitor_type,
        num_processes=num_processes,
        verbose=verbose,
        reports=reports,
        enable_singularity=enable_singularity,
        container=container,
        try_add_blocked_jobs=try_add_blocked_jobs,
        time_based_batching=time_based_batching,
        node_setup_script=node_setup_script,
        node_shutdown_script=node_shutdown_script,
        no_distributed_submitter=no_distributed_submitter,
    )
    # This converts enums to values.
    data = json.loads(params.json())
    if config_file.suffix == ".json":
        dump_data(data, config_file, indent=2)
    else:
        dump_data(data, config_file)
    print(f"Created submitter parameter file {config_file}")


@click.command()
@click.argument("output_dir", type=click.Path(exists=True), callback=lambda _, __, x: Path(x))
@click.option(
    "-c",
    "--config-file",
    default="submission_groups.json",
    show_default=True,
    type=Path,
    help="config file to create; can be .toml or .json",
)
@click.option(
    "-f",
    "--force",
    default=False,
    is_flag=True,
    show_default=True,
    help="Overwrite config_file if it already exists.",
)
def save_submission_groups(output_dir, config_file, force):
    if config_file.exists() and not force:
        print(
            f"{config_file} exists. Use a different name or pass --force to overwrite.",
            file=sys.stderr,
        )
        sys.exit(1)

    existing_groups_file = output_dir / Cluster.SUBMITTER_GROUP_FILE
    if not existing_groups_file.exists():
        print(f"{output_dir} is not a valid JADE output directory", file=sys.stderr)
        sys.exit(1)

    shutil.copyfile(existing_groups_file, config_file)
    print(f"Copied submission groups to {config_file}")


@click.command()
@click.argument("params_file", type=click.Path(exists=True))
@click.argument("name")
@click.argument("config_file", type=click.Path(exists=True))
def add_submission_group(params_file, name, config_file):
    """Add a submission group with parameters defined in params_file to config_file."""
    config = load_data(config_file)
    for group in config["submission_groups"]:
        if name == group["name"]:
            print(f"Error: {name} is already stored in {config_file}", file=sys.stderr)
            sys.exit(1)

    params = load_data(params_file)
    group = {
        "name": name,
        "submitter_params": params,
    }
    # Make sure it parses.
    SubmissionGroup(**group)

    config["submission_groups"].append(group)
    dump_data(config, config_file, indent=2)
    print(f"Updated {config_file} with submission group {name}.")


config.add_command(create)
config.add_command(assign_blocked_by)
config.add_command(hpc)
config.add_command(show)
config.add_command(_filter)
config.add_command(spark)
config.add_command(submitter_params)
config.add_command(save_submission_groups)
config.add_command(add_submission_group)
