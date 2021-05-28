"""CLI to display and manage config files."""

import json
import logging
import os
import re
import sys
import tempfile
from pathlib import Path

import click
from prettytable import PrettyTable

from jade.cli.common import COMMON_SUBMITTER_OPTIONS, add_options, make_submitter_params
from jade.common import CONFIG_FILE, HPC_CONFIG_FILE
from jade.extensions.generic_command import GenericCommandConfiguration
from jade.jobs.job_configuration_factory import create_config_from_file
from jade.loggers import setup_logging
from jade.utils.utils import dump_data, load_data
from jade.models import HpcConfig, SlurmConfig, FakeHpcConfig, LocalHpcConfig
from jade.models.submitter_params import SubmitterParams


logger = logging.getLogger(__name__)


@click.group()
def config():
    """Manage a JADE configuration."""
    setup_logging("config", None)


@click.command()
@click.argument("filename", type=click.Path(exists=True))
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
    "-v",
    "--verbose",
    is_flag=True,
    default=False,
    show_default=True,
    help="Enable verbose log output.",
)
def create(filename, config_file, cancel_on_blocking_job_failure, minutes_per_job, verbose):
    """Create a config file from a filename with a list of executable commands."""
    level = logging.DEBUG if verbose else logging.WARNING
    setup_logging("auto_config", None, console_level=level)

    config = GenericCommandConfiguration.auto_config(
        filename,
        cancel_on_blocking_job_failure=cancel_on_blocking_job_failure,
        minutes_per_job=minutes_per_job,
    )
    print(f"Created configuration with {config.get_num_jobs()} jobs.")
    config.dump(config_file)
    print(f"Dumped configuration to {config_file}.\n")


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

    field_names = ["index", "name"]
    if blocked_by:
        field_names.append("blocked_by (job names)")
    if fields is not None:
        field_names += fields

    table = PrettyTable()
    table.field_names = field_names
    for i, job in enumerate(config.iter_jobs()):
        row = [i, job.name]
        if blocked_by:
            blocking_jobs = sorted(list(job.get_blocking_jobs()))
            text = ", ".join(blocking_jobs)
            if len(text) > 50:
                text = f"truncated...blocked by {len(blocking_jobs)} jobs"
            row.append(text)
        if fields is not None:
            job_dict = job.serialize()
            for field in fields:
                row.append(job_dict.get(field, ""))
        table.add_row(row)
    print(table)


@click.command("filter")
@click.argument("config_file", type=click.Path(exists=True))
@click.argument("indices", nargs=-1)
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
def _filter(config_file, output_file, indices, fields, show_config=False):
    """Filters jobs in CONFIG_FILE. Prints the new jobs to the console or
    optionally creates a new file.

    Note: This does not detect duplicate ranges.

    \b
    Examples:
    1. Select the first job. Output only.
       jade config filter c1.json 0
    2. Select indices 0-4, 10-14, 20, 25, create new file.
       jade config filter c1.json :5 10:15 20 25 -o c2.json
    3. Select the last 5 jobs. Note the use of '--' to prevent '-5' from being
       treated as an option.
       jade config filter c1.json -o c2.json -- -5:
    4. Select indices 5 through the end.
       jade config filter c1.json -o c2.json 5:
    5. Select jobs with parameters param1=green and param2=3.
       jade config filter c1.json -o c2.json -f param1 green -f param2 3

    """
    cfg = load_data(config_file)
    jobs = cfg["jobs"]
    if not jobs:
        print("The configuration has no jobs")
        sys.exit(1)

    if output_file is None:
        handle, new_config_file = tempfile.mkstemp(suffix=".json")
        os.close(handle)
        show_config = True
    else:
        new_config_file = output_file

    try:
        if not new_config_file.endswith(".json"):
            print("new_config_file must have extension .json")
            sys.exit(1)

        orig_len = len(jobs)
        new_jobs = []
        regex_int = re.compile(r"^(?P<index>\d+)$")
        regex_range = re.compile(r"^(?P<start>[\d-]*):(?P<end>[\d-]*)$")
        for index in indices:
            match = regex_int.search(index)
            if match:
                i = int(match.groupdict()["index"])
                new_jobs.append(jobs[i])
                continue
            match = regex_range.search(index)
            if match:
                start = match.groupdict()["start"]
                if start == "":
                    start = None
                else:
                    start = int(start)
                end = match.groupdict()["end"]
                if end == "":
                    end = None
                else:
                    end = int(end)
                new_jobs += jobs[start:end]

        # Note: when looking at just the JSON, there is no way to get the job name,
        # and so we can't check for duplicates.

        if not new_jobs:
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
    num_processes=None,
    verbose=None,
    reports=None,
    try_add_blocked_jobs=None,
    time_based_batching=None,
    node_setup_script=None,
    node_shutdown_script=None,
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
        num_processes=num_processes,
        verbose=verbose,
        reports=reports,
        try_add_blocked_jobs=try_add_blocked_jobs,
        time_based_batching=time_based_batching,
        node_setup_script=node_setup_script,
        node_shutdown_script=node_shutdown_script,
    )
    # This converts enums to values.
    data = json.loads(params.json())
    if config_file.suffix == ".json":
        dump_data(data, config_file, indent=2)
    else:
        dump_data(data, config_file)
    print(f"Created submitter parameter file {config_file}")


config.add_command(create)
config.add_command(hpc)
config.add_command(show)
config.add_command(_filter)
config.add_command(submitter_params)
