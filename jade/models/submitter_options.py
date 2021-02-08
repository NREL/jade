"""Models for submitter options"""

import enum
from typing import List, Optional, Set, Union

from pydantic import Field

from jade.models import JadeBaseModel, HpcConfig, SlurmConfig


DEFAULTS = {
    "max_nodes": 16,
    "per_node_batch_size": 500,
    "poll_interval": 60,
}


class SubmitterOptions(JadeBaseModel):
    """Defines the submitter options selected by the user."""

    generate_reports: Optional[bool] = Field(
        title="generate_reports",
        description="controls whether to generate reports after completion",
        default=True,
    )
    hpc_config: HpcConfig = Field(
        title="hpc_config",
        description="HPC config options",
    )
    max_nodes: Optional[int] = Field(
        title="max_nodes",
        description="max number of compute nodes to use simultaneously",
        default=DEFAULTS["max_nodes"],
    )
    num_processes: Optional[int] = Field(
        title="num_processes",
        description="number of processes to run in parallel on each node",
        default=None,
    )
    per_node_batch_size: Optional[int] = Field(
        title="per_node_batch_size",
        description="how many jobs to assign to each node",
        default=DEFAULTS["per_node_batch_size"],
    )
    poll_interval: Optional[int] = Field(
        title="poll_interval",
        description="interval in seconds on which to poll jobs for status",
        default=DEFAULTS["poll_interval"],
    )
    verbose: Optional[bool] = Field(
        title="verbose",
        description="enable debug logging",
        default=False,
    )
