"""Models for submitter options"""

from typing import List, Optional, Set, Union

from pydantic import Field

from jade.common import HPC_CONFIG_FILE
from jade.models import JadeBaseModel, HpcConfig, SlurmConfig


DEFAULTS = {
    "hpc_config_file": HPC_CONFIG_FILE,
    "per_node_batch_size": 500,
    "poll_interval": 60,
    "resource_monitor_interval": 30,
}


class SubmitterParams(JadeBaseModel):
    """Defines the submitter options selected by the user."""

    generate_reports: Optional[bool] = Field(
        title="generate_reports",
        description="Controls whether to generate reports after completion",
        default=True,
    )
    hpc_config: HpcConfig = Field(
        title="hpc_config",
        description="HPC config options",
    )
    max_nodes: Optional[int] = Field(
        title="max_nodes",
        description="Max number of compute nodes to use simultaneously, default is unbounded",
        default=None,
    )
    num_processes: Optional[int] = Field(
        title="num_processes",
        description="Number of processes to run in parallel on each node",
        default=None,
    )
    per_node_batch_size: Optional[int] = Field(
        title="per_node_batch_size",
        description="How many jobs to assign to each node",
        default=DEFAULTS["per_node_batch_size"],
    )
    node_setup_script: Optional[str] = Field(
        title="node_setup_script",
        description="Script to run on each node before starting jobs",
    )
    node_shutdown_script: Optional[str] = Field(
        title="node_shutdown_script",
        description="Script to run on each node after completing jobs",
    )
    poll_interval: Optional[int] = Field(
        title="poll_interval",
        description="Interval in seconds on which to poll jobs for status",
        default=DEFAULTS["poll_interval"],
    )
    resource_monitor_interval: Optional[int] = Field(
        title="resource_monitor_interval",
        description="Interval in seconds on which to collect resource stats",
        default=DEFAULTS["resource_monitor_interval"],
    )
    try_add_blocked_jobs: Optional[bool] = Field(
        title="try_add_blocked_jobs",
        description="Add blocked jobs to a batch if all blocking jobs are in the batch. "
        "Be aware of time constraints.",
        default=True,
    )
    time_based_batching: Optional[bool] = Field(
        title="time_based_batching",
        description="Use time-based batching instead of job-count-based batching",
        default=False,
    )
    dry_run: Optional[bool] = Field(
        title="dry_run",
        description="Dry run mode; don't start any jobs",
        default=False,
    )
    verbose: Optional[bool] = Field(
        title="verbose",
        description="Enable debug logging",
        default=False,
    )
