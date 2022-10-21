"""CLI to show extensions."""

import logging
import os
import shutil
import sys
from pathlib import Path

import click

from jade.common import HPC_CONFIG_FILE, OUTPUT_DIR
from jade.hpc.common import HpcType
from jade.loggers import setup_logging
from jade.jobs.pipeline_manager import PipelineManager
from jade.models import (
    HpcConfig,
    LocalHpcConfig,
    SingularityParams,
    SubmitterParams,
    get_model_defaults,
)
from jade.utils.utils import get_cli_string, load_data


logger = logging.getLogger(__name__)

SUBMITTER_PARAMS_DEFAULTS = get_model_defaults(SubmitterParams)


@click.group()
def pipeline():
    """Manage JADE execution pipeline."""
    setup_logging("pipeline", None)


@click.command()
@click.option(
    "-a",
    "--auto-config-commands",
    multiple=True,
    help="Commands or scripts to invoke at runtime to create configs for each stage.",
)
@click.option(
    "-f",
    "--config-files",
    multiple=True,
    type=click.Path(exists=True),
    help="Config files, one per stage. Mutually exclusive with --auto-config-cmds.",
)
@click.option(
    "-b",
    "--per-node-batch-size",
    default=SUBMITTER_PARAMS_DEFAULTS["per_node_batch_size"],
    show_default=True,
    help="Number of jobs to run on one node in one batch.",
)
@click.option(
    "-c",
    "--config-file",
    type=click.Path(),
    default=PipelineManager.CONFIG_FILENAME,
    show_default=True,
    help="pipeline config file.",
)
@click.option(
    "-h",
    "--hpc-config",
    type=click.Path(),
    default=HPC_CONFIG_FILE,
    show_default=True,
    help="HPC config file.",
)
@click.option(
    "-l",
    "--local",
    is_flag=True,
    default=False,
    show_default=True,
    help="Run locally even if on HPC.",
)
@click.option(
    "-n",
    "--max-nodes",
    default=None,
    type=int,
    show_default=True,
    help="Max number of node submission requests to make in parallel. Default is unbounded.",
)
@click.option(
    "-p",
    "--poll-interval",
    default=SUBMITTER_PARAMS_DEFAULTS["poll_interval"],
    type=float,
    show_default=True,
    help="Interval in seconds on which to poll jobs for status.",
)
@click.option(
    "-q",
    "--num-parallel-processes-per-node",
    "--num-processes",
    default=None,
    show_default=False,
    type=int,
    help="Number of processes to run in parallel; defaults to num CPUs.",
)
@click.option(
    "--reports/--no-reports",
    is_flag=True,
    default=True,
    show_default=True,
    help="Generate reports after execution.",
)
@click.option(
    "-S",
    "--enable-singularity",
    is_flag=True,
    default=False,
    show_default=True,
    help="Add Singularity parameters and set the config to run in a container.",
)
@click.option(
    "-C",
    "--container",
    type=click.Path(exists=True),
    help="Path to container",
)
@click.option(
    "--verbose", is_flag=True, default=False, show_default=True, help="Enable verbose log output."
)
def create(
    config_files,
    auto_config_commands,
    per_node_batch_size,
    config_file,
    hpc_config,
    local,
    max_nodes,
    poll_interval,
    num_parallel_processes_per_node,
    reports,
    enable_singularity,
    container,
    verbose,
):
    """Create a pipeline with multiple Jade configurations. The configs can be specified with
    individual config files or scripts that will be invoked at runtime to create configs based on
    dynamic inputs."""
    if local:
        hpc_config = HpcConfig(hpc_type=HpcType.LOCAL, hpc=LocalHpcConfig())
    else:
        if not os.path.exists(hpc_config):
            print(
                f"{hpc_config} does not exist. Generate it with 'jade config hpc' "
                "or run in local mode with '-l'",
                file=sys.stderr,
            )
            sys.exit(1)
        hpc_config = HpcConfig(**load_data(hpc_config))

    if not config_files and not auto_config_commands:
        print("Either --config-files or --auto-config-commands must be specified", file=sys.stderr)
        sys.exit(1)
    if config_files and auto_config_commands:
        print(
            "You cannot specify both --config-files and  --auto-config-commands", file=sys.stderr
        )
        sys.exit(1)

    if enable_singularity:
        singularity_params = SingularityParams(enabled=True, container=container)
    else:
        singularity_params = None
    submit_params = SubmitterParams(
        generate_reports=reports,
        hpc_config=hpc_config,
        max_nodes=max_nodes,
        num_parallel_processes_per_node=num_parallel_processes_per_node,
        per_node_batch_size=per_node_batch_size,
        poll_interval=poll_interval,
        singularity_params=singularity_params,
        verbose=verbose,
    )
    if config_files:
        PipelineManager.create_config_from_files(config_files, config_file, submit_params)
    else:
        PipelineManager.create_config_from_commands(
            auto_config_commands, config_file, submit_params
        )


@click.command()
@click.option("-o", "--output", default=OUTPUT_DIR, show_default=True, help="Output directory.")
@click.option(
    "--verbose", is_flag=True, default=False, show_default=True, help="Enable verbose log output."
)
def status(output, verbose):
    """Check status of the pipeline."""
    try:
        mgr = PipelineManager.load(output)
    except FileNotFoundError:
        print(f"{output} is not a valid pipeline output directory", file=sys.stderr)
        sys.exit(1)

    config = mgr.config
    completed_stages = []
    current_stage = None
    for stage in config.stages:
        if stage.stage_num < config.stage_num:
            completed_stages.append(stage)
        elif stage.stage_num == config.stage_num:
            current_stage = stage

    print(f"Is complete: {config.is_complete}")
    if current_stage is not None:
        print(f"Current stage number: {config.stage_num}")
        print("\nTo view the status of the current stage:")
        print(f"  jade show-status -o {current_stage.path}")
    if completed_stages:
        print(f"\nTo view results of the completed stages:")
        for stage in completed_stages:
            print(f"  jade show-results -o {stage.path}")


@click.command()
@click.argument("config-file")
@click.option(
    "-f",
    "--force",
    default=False,
    is_flag=True,
    show_default=True,
    help="Delete output directory if it exists.",
)
@click.option("-o", "--output", default=OUTPUT_DIR, show_default=True, help="Output directory.")
@click.option(
    "--verbose", is_flag=True, default=False, show_default=True, help="Enable verbose log output."
)
def submit(config_file, output, force, verbose=False):
    """Submit the pipeline for execution."""
    if os.path.exists(output):
        if force:
            shutil.rmtree(output)
        else:
            print(
                f"{output} already exists. Delete it or use '--force' to overwrite.",
                file=sys.stderr,
            )
            sys.exit(1)
    os.makedirs(output, exist_ok=True)

    filename = os.path.join(output, "pipeline_submit.log")
    level = logging.DEBUG if verbose else logging.INFO
    setup_logging(__name__, filename, file_level=level, console_level=level)
    logger.info(get_cli_string())

    mgr = PipelineManager.create(config_file, output)
    try:
        mgr.submit_next_stage(1)
    except Exception:
        logger.exception("Pipeline execution failed")
        raise

    logging.shutdown()
    sys.exit(0)


@click.command()
@click.argument("output")
@click.option(
    "--stage-num",
    required=True,
    type=int,
    help="stage number to submit",
)
@click.option(
    "--return-code",
    required=True,
    type=int,
    help="return code of stage index that just completed",
)
@click.option(
    "--verbose", is_flag=True, default=False, show_default=True, help="Enable verbose log output."
)
def submit_next_stage(output, stage_num, return_code, verbose=False):
    """Internal command to submit the next stage of the pipeline for execution."""
    filename = os.path.join(output, "pipeline_submit.log")
    level = logging.DEBUG if verbose else logging.INFO
    setup_logging(__name__, filename, file_level=level, console_level=level, mode="a")
    logger.info(get_cli_string())

    mgr = PipelineManager.load(output)
    try:
        mgr.submit_next_stage(stage_num, return_code=return_code)
    except Exception:
        logger.exception("Pipeline execution failed")
        raise

    logging.shutdown()
    sys.exit(0)


pipeline.add_command(create)
pipeline.add_command(status)
pipeline.add_command(submit)
pipeline.add_command(submit_next_stage)
