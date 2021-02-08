"""CLI to show job status."""

import logging
import os
import sys

import click

from jade.common import HPC_CONFIG_FILE
from jade.models import HpcConfig, SlurmConfig, FakeHpcConfig, LocalHpcConfig
from jade.utils.utils import dump_data


logger = logging.getLogger(__name__)


@click.command()
@click.option(
    "-a", "--account",
    default="",
    help="HPC account/allocation",
)
@click.option(
    "-c", "--config-file",
    default="hpc_config.toml",
    show_default=True,
    help="config file to create",
)
@click.option(
    "-p", "--partition",
    default="",
    help="HPC partition",
)
@click.option(
    "-q", "--qos",
    default=None,
    type=str,
    help="QoS value",
)
@click.option(
    "-t", "--hpc-type",
    type=click.Choice(["slurm", "fake", "local"]),
    default="slurm",
    show_default=True,
    help="HPC queueing system",
)
@click.option(
    "-w", "--walltime",
    default="",
    help="HPC walltime",
)
def create_hpc_config(account, config_file, partition, qos, hpc_type, walltime):
    """Create an HPC config file."""
    if hpc_type == "slurm":
        hpc = SlurmConfig(
            account=account,
            partition=partition,
            qos=qos,
            walltime=walltime,
        )
    elif hpc_type == "fake":
        hpc = FakeHpcConfig(walltime=walltime)
    else:
        assert hpc_type == "local"
        hpc = LocalHpcConfig()

    config = HpcConfig(hpc_type=hpc_type, hpc=hpc)
    data = config.dict()
    data["hpc_type"] = data["hpc_type"].value
    dump_data(data, config_file)
    print(f"Created HPC config file {config_file}")
