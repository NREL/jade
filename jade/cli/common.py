"""Common functions for CLI scripts"""

import logging
import logging.config
import os
import sys

import click

from jade.common import HPC_CONFIG_FILE
from jade.enums import Mode
from jade.exceptions import UserAbort
from jade.models import HpcConfig, LocalHpcConfig, SubmitterParams, get_model_defaults
from jade.utils.utils import load_data


logger = logging.getLogger(__name__)


_ENUM_MAPPING = {
    # class to type of value
    "mode": (Mode, str),
}


def handle_enum_input(_, param, value):
    """Converts inputs to enums."""
    try:
        tup = _ENUM_MAPPING.get(param.name)
        assert tup is not None, "Must add {} to _ENUM_MAPPING".format(param.name)
        # Force the correct type onto the value.
        return tup[0](tup[1](value))
    except ValueError as err:
        raise click.BadParameter(str(err))


def _handle_simulation_scripts(ctx, _, value):
    if not os.path.exists(value):
        logger.info("Creating %s...", value)
        try:
            os.makedirs(value, exist_ok=True)
        except Exception as err:
            logger.error("Could not create %s: %s", value, err)
            raise
    else:
        proceed_with_user_permission(ctx, "Overwrite {}".format(value))
        try:
            scripts = [f for f in os.listdir(value) if f.endswith(".sh")]
            for s in scripts:
                os.remove(os.path.join(value, s))
        except Exception as err:
            logger.error("Could not delete %s: %s", value, err)
            raise

    return value


def proceed_with_user_permission(ctx, message):
    """Pauses execution to prompt user for permission to proceed."""
    prompt = ctx.params.get("prompt")
    if prompt is None:
        prompt = ctx.parent.params["prompt"]
    if not prompt:
        logger.info("Proceed because user passed --no-prompt.")
        return

    answer = ""
    while answer not in ("y", "n"):
        answer = input("{} [Y/N]? ".format(message)).lower()

    if answer == "n":
        raise UserAbort


SUBMITTER_PARAMS_DEFAULTS = get_model_defaults(SubmitterParams)

COMMON_SUBMITTER_OPTIONS = (
    click.option(
        "-b",
        "--per-node-batch-size",
        default=SUBMITTER_PARAMS_DEFAULTS["per_node_batch_size"],
        show_default=True,
        help="Number of jobs to run on one node in one batch.",
    ),
    click.option(
        "--dry-run",
        default=False,
        is_flag=True,
        show_default=True,
        help="Dry run mode. Do not run any jobs. Not allowed in local mode.",
    ),
    click.option(
        "-h",
        "--hpc-config",
        type=click.Path(),
        default=HPC_CONFIG_FILE,
        show_default=True,
        help="HPC config file.",
    ),
    click.option(
        "-l",
        "--local",
        is_flag=True,
        default=False,
        show_default=True,
        envvar="LOCAL_SUBMITTER",
        help="Run on local system. Optionally, set the environment variable " "LOCAL_SUBMITTER=1.",
    ),
    click.option(
        "-n",
        "--max-nodes",
        default=SUBMITTER_PARAMS_DEFAULTS["max_nodes"],
        show_default=True,
        type=click.IntRange(
            2,
        ),
        help="Max number of node submission requests to make in parallel. Default is unbounded.",
    ),
    click.option(
        "-p",
        "--poll-interval",
        default=SUBMITTER_PARAMS_DEFAULTS["poll_interval"],
        type=float,
        show_default=True,
        help="Interval in seconds on which to poll jobs for status.",
    ),
    click.option(
        "-r",
        "--resource-monitor-interval",
        default=SUBMITTER_PARAMS_DEFAULTS["resource_monitor_interval"],
        type=int,
        show_default=True,
        help="Interval in seconds on which to collect resource stats. Default is None.",
    ),
    click.option(
        "-q",
        "--num-processes",
        default=SUBMITTER_PARAMS_DEFAULTS["num_processes"],
        show_default=False,
        type=int,
        is_eager=True,
        help="Number of processes to run in parallel; defaults to num CPUs.",
    ),
    click.option(
        "--reports/--no-reports",
        is_flag=True,
        default=SUBMITTER_PARAMS_DEFAULTS["generate_reports"],
        show_default=True,
        help="Generate reports after execution.",
    ),
    click.option(
        "-t",
        "--time-based-batching",
        is_flag=True,
        default=SUBMITTER_PARAMS_DEFAULTS["time_based_batching"],
        show_default=True,
        help="Use estimated runtimes to create batches. Each job must have its estimated runtime "
        "defined. Also requires --num-processes to be set. Overrides --per-node-batch-size.",
    ),
    click.option(
        "--try-add-blocked-jobs/--no-try-add-blocked-jobs",
        is_flag=True,
        default=True,
        show_default=SUBMITTER_PARAMS_DEFAULTS["try_add_blocked_jobs"],
        help="Add blocked jobs to a node's batch if they are blocked by jobs "
        "already in the batch.",
    ),
    click.option(
        "--verbose",
        is_flag=True,
        default=SUBMITTER_PARAMS_DEFAULTS["verbose"],
        show_default=True,
        help="Enable verbose log output.",
    ),
    click.option(
        "-x",
        "--node-setup-script",
        default=SUBMITTER_PARAMS_DEFAULTS["node_setup_script"],
        help="Script to run on each node before starting jobs (download input files).",
    ),
    click.option(
        "-y",
        "--node-shutdown-script",
        default=SUBMITTER_PARAMS_DEFAULTS["node_shutdown_script"],
        help="Script to run on each after completing jobs (upload output files).",
    ),
)


def add_options(options):
    def _add_options(func):
        for option in reversed(options):
            func = option(func)
        return func

    return _add_options


def make_submitter_params(
    per_node_batch_size=None,
    dry_run=None,
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
    """Returns an instance of SubmitterParams for use in a job submission."""
    if local:
        hpc_config = HpcConfig(hpc_type="local", hpc=LocalHpcConfig())
    else:
        # TODO: If the config file contains submission groups then this should not be required.
        if not os.path.exists(hpc_config):
            print(
                f"{hpc_config} does not exist. Generate it with 'jade config hpc' "
                "or run in local mode with '-l'"
            )
            sys.exit(1)
        hpc_config = HpcConfig(**load_data(hpc_config))

    if local and dry_run:
        print("Dry run is not allowed in local mode.")
        sys.exit(1)

    if (
        time_based_batching
        and per_node_batch_size != SUBMITTER_PARAMS_DEFAULTS["per_node_batch_size"]
    ):
        # This doesn't catch the case where the user passes --per-node-batch-size=default, but
        # I don't see that click provides a way to detect that condition.
        print("Error: --per-node-batch-size and --time-based-batching are mutually exclusive")
        sys.exit(1)

    if time_based_batching and num_processes is None:
        print("Error: num_processes must be set with time-based batching")
        sys.exit(1)

    return SubmitterParams(
        generate_reports=reports,
        hpc_config=hpc_config,
        max_nodes=max_nodes,
        num_processes=num_processes,
        per_node_batch_size=per_node_batch_size,
        dry_run=dry_run,
        node_setup_script=node_setup_script,
        node_shutdown_script=node_shutdown_script,
        poll_interval=poll_interval,
        resource_monitor_interval=resource_monitor_interval,
        time_based_batching=time_based_batching,
        try_add_blocked_jobs=try_add_blocked_jobs,
        verbose=verbose,
    )
