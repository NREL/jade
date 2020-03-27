"""CLI to show extensions."""

import logging
import os
import shutil
import sys

import click

from jade.loggers import setup_logging
from jade.jobs.job_submitter import DEFAULTS
from jade.utils.subprocess_manager import run_command
from jade.utils.utils import dump_data, get_cli_string, load_data


logger = logging.getLogger(__name__)


@click.group()
def pipeline():
    """Manage JADE execution pipeline."""
    setup_logging("pipeline", None)


@click.command()
@click.option(
    "-c", "--config-file",
    type=click.Path(),
    default="pipeline.toml",
    show_default=True,
    help="pipeline config file."
)
@click.argument(
    "auto-config-cmds",
    nargs=-1,
)
def create(auto_config_cmds, config_file):
    """Create a pipeline with multiple Jade configurations."""
    data = {"stages": []}
    for i, cmd in enumerate(auto_config_cmds):
        stage_num = i + 1
        data["stages"].append(
            {
                "auto_config_cmd": cmd,
                "config_file": _get_stage_config_file_name(stage_num),
                "submit-params": {
                    "--max-nodes": DEFAULTS["max_nodes"],
                    "--per-node-batch-size": DEFAULTS["per_node_batch_size"],
                    "--num-processes": None,
                }
            }
        )

    dump_data(data, config_file)
    print(f"Created pipeline config file {config_file}")


@click.command()
@click.argument("config-file")
@click.option(
    "-o", "--output",
    default=DEFAULTS["output"],
    show_default=True,
    help="Output directory."
)
@click.option(
    "--verbose",
    is_flag=True,
    default=False,
    show_default=True,
    help="Enable verbose log output."
)
def submit(config_file, output, verbose=False):
    """Submit the pipeline for execution."""
    global logger
    os.makedirs(output, exist_ok=True)
    filename = os.path.join(output, "pipeline_submit.log")
    level = logging.DEBUG if verbose else logging.INFO
    logger = setup_logging(__name__, filename, file_level=level,
                           console_level=level)

    logger.info(get_cli_string())
    config = load_data(config_file)

    logger.info("Start execution pipeline %s num_stages=%s", config_file,
                len(config["stages"]))
    for i, stage in enumerate(config["stages"]):
        stage_num = i + 1
        ret = _run_auto_config(output, stage, stage_num)
        if ret != 0:
            sys.exit(ret)
        ret = run_command(_make_submit_cmd(output, stage, stage_num, verbose))
        if ret != 0:
            sys.exit(ret)

    logger.info("Finished execution pipeline")
    sys.exit(0)


def _run_auto_config(output, stage, stage_num):
    config_file = stage["config_file"]
    if os.path.exists(config_file):
        os.remove(config_file)

    auto_config_cmd = stage["auto_config_cmd"]
    if stage_num > 1:
        # The output of the previous stage is the input for this stage.
        auto_config_cmd += " " + _get_stage_output_path(output, stage_num - 1)

    ret = run_command(auto_config_cmd)
    if ret != 0:
        logger.error("Failed to auto-config stage %s: %s", stage_num, ret)
        return ret

    if not os.path.exists(config_file):
        logger.error("auto-config stage %s did not produce %s", stage_num,
                     config_file)
        return 1

    final_file = _get_stage_config_file_path(output, stage_num)
    shutil.move(config_file, final_file)
    stage["config_file"] = final_file
    return ret


def _make_submit_cmd(output, stage, stage_num, verbose):
    stage_output = _get_stage_output_path(output, stage_num)
    cmd = f"jade submit-jobs {stage['config_file']} -o {stage_output}"
    for key, val in stage["submit-params"].items():
        cmd += f" {key}={val}"
    if verbose:
        cmd += " --verbose"

    return cmd


def _get_stage_config_file_name(stage_num):
    return f"config-stage{stage_num}.json"


def _get_stage_config_file_path(output, stage_num):
    return os.path.join(output, _get_stage_config_file_name(stage_num))


def _get_stage_output_name(stage_num):
    return f"output-stage{stage_num}"


def _get_stage_output_path(output, stage_num):
    return os.path.join(output, _get_stage_output_name(stage_num))



pipeline.add_command(create)
pipeline.add_command(submit)
