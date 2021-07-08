"""CLI to show extensions."""

import logging
import os
import shutil
import sys

import click

from jade.common import HPC_CONFIG_FILE, OUTPUT_DIR
from jade.hpc.common import HpcType
from jade.loggers import setup_logging
from jade.jobs.pipeline_manager import PipelineManager
from jade.models import HpcConfig, LocalHpcConfig, SubmitterParams, get_model_defaults
from jade.utils.utils import get_cli_string, load_data


logger = logging.getLogger(__name__)

SUBMITTER_PARAMS_DEFAULTS = get_model_defaults(SubmitterParams)


@click.group()
def pipeline():
    """Manage JADE execution pipeline."""
    setup_logging("pipeline", None)


@click.command()
@click.argument(
    "auto-config-cmds",
    nargs=-1,
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
    "--verbose", is_flag=True, default=False, show_default=True, help="Enable verbose log output."
)
def create(
    auto_config_cmds,
    per_node_batch_size,
    config_file,
    hpc_config,
    local,
    max_nodes,
    poll_interval,
    num_processes,
    reports,
    verbose,
):
    """Create a pipeline with multiple Jade configurations."""
    if local:
        hpc_config = HpcConfig(hpc_type=HpcType.LOCAL, hpc=LocalHpcConfig())
    else:
        if not os.path.exists(hpc_config):
            print(
                f"{hpc_config} does not exist. Generate it with 'jade config hpc' "
                "or run in local mode with '-l'"
            )
            sys.exit(1)
        hpc_config = HpcConfig(**load_data(hpc_config))

    submit_params = SubmitterParams(
        generate_reports=reports,
        hpc_config=hpc_config,
        max_nodes=max_nodes,
        num_processes=num_processes,
        per_node_batch_size=per_node_batch_size,
        poll_interval=poll_interval,
        verbose=verbose,
    )
    PipelineManager.create_config(auto_config_cmds, config_file, submit_params)


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
            print(f"{output} already exists. Delete it or use '--force' to overwrite.")
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
pipeline.add_command(submit)
pipeline.add_command(submit_next_stage)
