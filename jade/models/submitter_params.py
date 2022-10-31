"""Models for submitter options"""

import re
from datetime import timedelta
from typing import Optional

from pydantic import Field

from jade.enums import ResourceMonitorType
from jade.models import JadeBaseModel, HpcConfig, SingularityParams


class ResourceMonitorStats(JadeBaseModel):
    """Defines the stats to monitor."""

    cpu: bool = Field(
        description="Monitor CPU utilization",
        default=True,
    )
    disk: bool = Field(
        description="Monitor disk/storage utilization",
        default=False,
    )
    memory: bool = Field(
        description="Monitor memory utilization",
        default=True,
    )
    network: bool = Field(
        description="Monitor network utilization",
        default=False,
    )
    process: bool = Field(
        description="Monitor per-job process utilization",
        default=False,
    )
    include_child_processes: bool = Field(
        description="Include stats from direct child processes in utilization for each job.",
        default=True,
    )
    recurse_child_processes: bool = Field(
        description="Recurse child processes to find all descendants..",
        default=False,
    )


class SubmitterParams(JadeBaseModel):
    """Defines the submitter options selected by the user."""

    generate_reports: bool = Field(
        description="Controls whether to generate reports after completion",
        default=True,
    )
    hpc_config: HpcConfig = Field(
        description="HPC config options",
    )
    max_nodes: Optional[int] = Field(
        description="Max number of compute nodes to use simultaneously, default is unbounded",
        default=None,
    )
    num_parallel_processes_per_node: Optional[int] = Field(
        description="Number of processes to run in parallel on each node",
        default=None,
        alias="num_processes",
    )
    per_node_batch_size: int = Field(
        description="How many jobs to assign to each node",
        default=500,
    )
    # The next two parameters are obsolete and will eventually be deleted.
    node_setup_script: Optional[str] = Field(
        description="Script to run on each node before starting jobs",
        default=None,
    )
    node_shutdown_script: Optional[str] = Field(
        description="Script to run on each node after completing jobs",
        default=None,
    )
    poll_interval: int = Field(
        description="Interval in seconds on which to poll jobs for status",
        default=10,
    )
    resource_monitor_interval: Optional[int] = Field(
        description="Interval in seconds on which to collect resource stats. Disable monitoring "
        "by setting this to None/null."
        "summaries of stats.",
        default=10,
    )
    resource_monitor_type: ResourceMonitorType = Field(
        description=f"Type of resource monitoring to perform. Options: {[x.value for x in ResourceMonitorType]}",
        default=ResourceMonitorType.AGGREGATION,
    )
    resource_monitor_stats: ResourceMonitorStats = Field(
        description="Resource utilization stats to monitor",
        default=ResourceMonitorStats(),
    )
    try_add_blocked_jobs: bool = Field(
        description="Add blocked jobs to a batch if all blocking jobs are in the batch. "
        "Be aware of time constraints.",
        default=True,
    )
    time_based_batching: bool = Field(
        description="Use time-based batching instead of job-count-based batching",
        default=False,
    )
    dry_run: bool = Field(
        description="Dry run mode; don't start any jobs",
        default=False,
    )
    verbose: bool = Field(
        description="Enable debug logging",
        default=False,
    )
    singularity_params: Optional[SingularityParams] = Field(
        description="Singularity container parameters",
        default=None,
    )
    distributed_submitter: bool = Field(
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
