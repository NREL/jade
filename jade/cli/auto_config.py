"""CLI to automatically create a configuration."""

import logging
import click

from jade.common import CONFIG_FILE
from jade.loggers import setup_logging
from jade.exceptions import InvalidExtension
from jade.extensions.registry import Registry, ExtensionClassType
from jade.jobs.batch_post_process import BatchPostProcess
from jade.jobs.job_post_process import JobPostProcess
from jade.utils.utils import load_data


# TODO: need one group command for auto-config; this should be a subcommand.


@click.command()
@click.argument("extension")
@click.argument("inputs")
@click.option(
    "-p",
    "--job-post-process-config-file",
    type=click.Path(exists=True),
    is_eager=True,
    help="The path of job-based post-process config file.",
)
@click.option(
    "-b",
    "--batch-post-process-config-file",
    type=click.Path(exists=True),
    help="The config file which contains batch post-processing commands.",
)
@click.option(
    "-c",
    "--config-file",
    default=CONFIG_FILE,
    show_default=True,
    help="config file to generate.",
)
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    default=False,
    show_default=True,
    help="Enable verbose log output.",
)
def auto_config(
        extension,
        inputs,
        job_post_process_config_file,
        batch_post_process_config_file,
        config_file,
        verbose):
    """Automatically create a configuration."""
    level = logging.DEBUG if verbose else logging.WARNING
    setup_logging("auto_config", None, console_level=level)

    if job_post_process_config_file is not None:
        module, class_name, data = JobPostProcess.load_config_from_file(
            job_post_process_config_file
        )
        JobPostProcess(module, class_name, data)  # ensure everything ok
        job_post_process_config = {"module": module, "class_name": class_name, "data": data}
    else:
        job_post_process_config = None

    if batch_post_process_config_file is not None:
        bpp = BatchPostProcess(config_file=batch_post_process_config_file)
        batch_post_process_config = bpp.serialize()
    else:
        batch_post_process_config = None

    # User extension
    registry = Registry()
    if not registry.is_registered(extension):
        raise InvalidExtension(f"Extension '{extension}' is not registered.")

    cli = registry.get_extension_class(extension, ExtensionClassType.CLI)
    config = cli.auto_config(
        inputs,
        job_post_process_config=job_post_process_config,
        batch_post_process_config=batch_post_process_config,
    )

    print(f"Created configuration with {config.get_num_jobs()} jobs.")
    config.dump(config_file)
    print(f"Dumped configuration to {config_file}.\n")
