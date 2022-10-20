"""Models for submitter options"""

import re
from datetime import timedelta
from typing import Optional

from pydantic import Field

from jade.enums import ResourceMonitorType
from jade.models import JadeBaseModel, HpcConfig, SingularityParams


class SubmitterParams(JadeBaseModel):
    """Defines the submitter options selected by the user."""

    generate_reports: bool = Field(
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
    num_parallel_processes_per_node: Optional[int] = Field(
        title="num_parallel_processes_per_node",
        description="Number of processes to run in parallel on each node",
        default=None,
        alias="num_processes",
    )
    per_node_batch_size: int = Field(
        title="per_node_batch_size",
        description="How many jobs to assign to each node",
        default=500,
    )
    # The next two parameters are obsolete and will eventually be deleted.
    node_setup_script: Optional[str] = Field(
        title="node_setup_script",
        description="Script to run on each node before starting jobs",
        default=None,
    )
    node_shutdown_script: Optional[str] = Field(
        title="node_shutdown_script",
        description="Script to run on each node after completing jobs",
        default=None,
    )
    poll_interval: int = Field(
        title="poll_interval",
        description="Interval in seconds on which to poll jobs for status",
        default=10,
    )
    resource_monitor_interval: Optional[int] = Field(
        title="resource_monitor_interval",
        description="Interval in seconds on which to collect resource stats. Disable monitoring "
        "by setting this to None/null."
        "summaries of stats.",
        default=10,
    )
    resource_monitor_type: ResourceMonitorType = Field(
        title="resource_monitor_type",
        description=f"Type of resource monitoring to perform. Options: {[x.value for x in ResourceMonitorType]}",
        default=ResourceMonitorType.AGGREGATION,
    )
    try_add_blocked_jobs: bool = Field(
        title="try_add_blocked_jobs",
        description="Add blocked jobs to a batch if all blocking jobs are in the batch. "
        "Be aware of time constraints.",
        default=True,
    )
    time_based_batching: bool = Field(
        title="time_based_batching",
        description="Use time-based batching instead of job-count-based batching",
        default=False,
    )
    dry_run: bool = Field(
        title="dry_run",
        description="Dry run mode; don't start any jobs",
        default=False,
    )
    verbose: bool = Field(
        title="verbose",
        description="Enable debug logging",
        default=False,
    )
    singularity_params: Optional[SingularityParams] = Field(
        title="singularity_params",
        description="Singularity container parameters",
        default=None,
    )
    distributed_submitter: bool = Field(
        title="distributed_submitter",
        description="Submit new jobs and update status on compute nodes.",
        default=True,
    )

    def get_wall_time(self):
        """Return the wall time from the HPC parameters.

        Returns
        -------
        timedelta

        """
        wall_time = getattr(self.hpc_config.hpc, "walltime", None)
        if wall_time is None:
            return timedelta(seconds=0xFFFFFFFF)  # largest 8-byte integer
        return _to_timedelta(wall_time)

    def dict(self, *args, **kwargs):
        data = super().dict(*args, **kwargs)
        if data["node_setup_script"] is None:
            data.pop("node_setup_script")
        if data["node_shutdown_script"] is None:
            data.pop("node_shutdown_script")
        return data


_REGEX_WALL_TIME = re.compile(r"(\d+):(\d+):(\d+)")


def _to_timedelta(wall_time):
    match = _REGEX_WALL_TIME.search(wall_time)
    assert match
    hours = int(match.group(1))
    minutes = int(match.group(2))
    seconds = int(match.group(3))
    return timedelta(hours=hours, minutes=minutes, seconds=seconds)
